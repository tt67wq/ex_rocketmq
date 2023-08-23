defmodule ExRocketmq.Namesrvs do
  @moduledoc """
  The namesrvs layer of the rocketmq: how client communicates with the namesrvs
  """

  use GenServer

  alias ExRocketmq.{
    Remote,
    Typespecs,
    Remote.Packet,
    Protocol.Request,
    Protocol.Response,
    Models
  }

  require Packet
  require Request
  require Response

  # request constants
  @req_get_routeinfo_by_topic Request.req_get_routeinfo_by_topic()
  @req_get_broker_cluster_info Request.req_get_broker_cluster_info()

  # response constants
  @resp_success Response.resp_success()
  # @resp_error Response.resp_error()

  # import ExRocketmq.Util.Debug

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
  @spec query_topic_route_info(pid(), String.t()) ::
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
  @spec get_broker_cluster_info(pid()) ::
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

  # because of fastjson, brokerAddrs is a integer-keyed map, which is invalid json
  # we replace integer-keyed map with string-keyed map here
  defp fix_invalid_json(input) do
    pattern = ~r/"brokerAddrs":{(\d+:"[^"]+",*)+}/
    matches = Regex.run(pattern, input)

    case matches do
      [] ->
        input

      [brokerAddrs, _] ->
        # "\"brokerAddrs\":{0:\"10.88.4.78:20911\",1:\"10.88.4.79:20911\"}"
        fixedBrokerAddrs =
          brokerAddrs
          |> String.slice(15..-2)
          |> String.split(",")
          |> Enum.map(fn x ->
            [index, addr] = String.split(x, ":", parts: 2)
            "\"#{index}\":#{addr}"
          end)
          |> Enum.join(",")
          |> then(&"\"brokerAddrs\":{#{&1}}")

        String.replace(input, brokerAddrs, fixedBrokerAddrs)
    end
  end

  # -------- server -------

  def init(remotes: remotes) do
    remote_pids =
      remotes
      |> Enum.map(fn r ->
        {:ok, pid} = Remote.start_link(r)
        pid
      end)
      |> List.to_tuple()

    {:ok, %{remotes: remote_pids, opaque: 0, index: 0, size: length(remotes)}}
  end

  def handle_call(
        {:rpc, code, body, ext_fields},
        _from,
        %{remotes: remotes, opaque: opaque, index: index, size: size} = state
      ) do
    pkt =
      Packet.packet(
        code: code,
        opaque: opaque,
        ext_fields: ext_fields,
        body: body
      )

    reply =
      remotes
      |> elem(index)
      |> Remote.rpc(pkt)

    {:reply, reply, %{state | opaque: opaque + 1, index: rem(index + 1, size)}}
  end
end
