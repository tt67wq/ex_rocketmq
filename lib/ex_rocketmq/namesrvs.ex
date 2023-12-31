defmodule ExRocketmq.Namesrvs do
  @moduledoc """
  The namesrvs layer of the rocketmq: how client communicates with the namesrvs.
  Most time, you should start nameserver under the supervision tree of your application.

  EG:
  ```elixir

      Supervisor.start_link(
        [
          {Namesrvs,
           remotes: [
             [transport: Transport.Tcp.new(host: "test.rocket-mq.net", port: 31_120)]
           ],
           opts: [
             name: :namesrvs
           ]},
        ],
        strategy: :one_for_one
      )
  ```

  Your consumer or producer module can communicate with namesrvs by its name
  """

  use GenServer

  alias ExRocketmq.Models
  alias ExRocketmq.Protocol.Request
  alias ExRocketmq.Protocol.Response
  alias ExRocketmq.Remote
  alias ExRocketmq.Remote.Packet
  alias ExRocketmq.Typespecs

  require Logger
  require Packet
  require Request
  require Response

  # request constants
  @req_get_routeinfo_by_topic Request.req_get_routeinfo_by_topic()
  @req_get_broker_cluster_info Request.req_get_broker_cluster_info()

  # response constants
  @resp_success Response.resp_success()

  @namesrvs_opts_schema [
    remotes: [
      type: {:list, :keyword_list},
      required: true,
      doc: "The remote opts of the namesrvs"
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The other opts of the namesrvs"
    ]
  ]

  @type namesrvs_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@namesrvs_opts_schema))]

  @spec start_link(namesrvs_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    {opts, init} =
      opts
      |> NimbleOptions.validate!(@namesrvs_opts_schema)
      |> Keyword.pop(:opts)

    GenServer.start_link(__MODULE__, init, opts)
  end

  @doc """
  query the topic route info from the namesrvs

  ## Examples

      iex> ExRocketmq.Namesrvs.query_topic_route_info(namesrvs, "topic")
      {:ok,
       %ExRocketmq.Models.TopicRouteInfo{
         broker_datas: [
           %ExRocketmq.Models.BrokerData{
             broker_addrs: %{"0" => "10.88.4.78:20911", "2" => "10.88.4.144:20911"},
             broker_name: "sts-broker-d2-0",
             cluster: "d2"
           }
         ],
         queue_datas: [
           %ExRocketmq.Models.QueueData{
             broker_name: "sts-broker-d2-0",
             perm: 6,
             read_queue_nums: 2,
             topic_syn_flag: 0,
             write_queue_nums: 2
           }
         ]
       }}
  """
  @spec query_topic_route_info(namesrvs :: pid() | atom(), topic :: String.t()) ::
          Typespecs.ok_t(Models.TopicRouteInfo.t()) | Typespecs.error_t()
  def query_topic_route_info(namesrvs, topic) do
    with {:ok, msg} <-
           GenServer.call(
             namesrvs,
             {:rpc, @req_get_routeinfo_by_topic, <<>>, %{"topic" => topic}}
           ) do
      msg
      |> Packet.packet(:code)
      |> case do
        @resp_success ->
          msg
          |> Packet.packet(:body)
          |> fix_invalid_json()
          |> Models.TopicRouteInfo.from_json()
          |> then(&{:ok, &1})

        code ->
          {:error, %{code: code, remark: Packet.packet(msg, :remark)}}
      end
    end
  end

  @doc """
  get the broker cluster info from the namesrvs

  ## Examples

      iex> ExRocketmq.Namesrvs.get_broker_cluster_info(namesrvs)
      {:ok,
        %ExRocketmq.Models.BrokerClusterInfo{
          broker_addr_table: %{
            "sts-broker-d2-0" => %ExRocketmq.Models.BrokerData{
              broker_addrs: %{"0" => "10.88.4.57:20911", "1" => "10.88.4.189:20911"},
              broker_name: "sts-broker-d2-0",
              cluster: "d2"
            }
          },
          cluster_addr_table: %{"d2" => ["sts-broker-d2-0"]}
      }}
  """
  @spec get_broker_cluster_info(pid() | atom()) ::
          Typespecs.ok_t(Models.BrokerClusterInfo.t()) | Typespecs.error_t()
  def get_broker_cluster_info(namesrvs) do
    with {:ok, msg} <-
           GenServer.call(
             namesrvs,
             {:rpc, @req_get_broker_cluster_info, <<>>, %{}}
           ) do
      msg
      |> Packet.packet(:code)
      |> case do
        @resp_success ->
          msg
          |> Packet.packet(:body)
          |> fix_invalid_json()
          |> Models.BrokerClusterInfo.from_json()
          |> then(&{:ok, &1})

        code ->
          {:error, %{code: code, remark: Packet.packet(msg, :remark)}}
      end
    end
  end

  @spec addrs(pid() | atom()) :: Typespecs.ok_t([String.t()]) | Typespecs.error_t()
  def addrs(namesrvs) do
    GenServer.call(namesrvs, :addrs)
  end

  # because of fastjson, brokerAddrs is a integer-keyed map, which is invalid json
  # we replace integer-keyed map with string-keyed map here
  defp fix_invalid_json(input) do
    pattern = ~r/"brokerAddrs":{(\d+:"[^"]+",*)+}/
    matches = Regex.run(pattern, input)

    case matches do
      [] ->
        input

      [broker_addrs, _] ->
        # "\"brokerAddrs\":{0:\"10.88.4.78:20911\",1:\"10.88.4.79:20911\"}"
        fixed_broker_addrs =
          broker_addrs
          |> String.slice(15..-2)
          |> String.split(",")
          |> Enum.map_join(",", fn x ->
            [index, addr] = String.split(x, ":", parts: 2)
            "\"#{index}\":#{addr}"
          end)
          |> then(&"\"brokerAddrs\":{#{&1}}")

        String.replace(input, broker_addrs, fixed_broker_addrs)
    end
  end

  @spec stop(pid() | atom()) :: :ok
  def stop(namesrvs), do: GenServer.stop(namesrvs)

  # -------- server -------

  def init(remotes: remotes) do
    q =
      remotes
      |> Enum.map(fn r ->
        {:ok, pid} = Remote.start_link(r)
        pid
      end)
      |> :queue.from_list()

    {:ok, %{remotes: q, opaque: 0}}
  end

  def handle_call({:rpc, code, body, ext_fields}, _from, %{remotes: q, opaque: opaque} = state) do
    pkt =
      Packet.packet(
        code: code,
        opaque: opaque,
        ext_fields: ext_fields,
        body: body
      )

    {{:value, remote}, q} = :queue.out(q)

    reply = Remote.rpc(remote, pkt)

    {:reply, reply, %{state | opaque: opaque + 1, remotes: :queue.in(remote, q)}}
  end

  def handle_call(:addrs, _from, %{remotes: q} = state) do
    addrs =
      q
      |> :queue.to_list()
      |> Enum.map(fn x ->
        {:ok, %{host: host, port: port}} = Remote.transport_info(x)
        "#{host}:#{port}"
      end)

    {:reply, {:ok, addrs}, state}
  end

  def terminate(reason, %{remotes: remotes}) do
    Logger.warning("namesrvs terminated with reason: #{inspect(reason)}")

    Enum.each(remotes, &Remote.stop/1)
  end
end
