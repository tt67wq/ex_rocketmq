defmodule ExRocketmq.Producer do
  @moduledoc """
  Rocketmq Producer
  """
  defmodule State do
    @moduledoc """
    The state of the producer GenServer
    """
    alias ExRocketmq.Models

    @type t :: %__MODULE__{
            producer: ExRocketmq.Producer.t(),
            broker_datas: [Models.BrokerData.t()],
            queues: [Models.MessageQueue.t()],
            registry: atom(),
            dynamic_supervisor: pid(),
            uniq_id: pid(),
            compressor: pid()
          }

    defstruct [
      :producer,
      :broker_datas,
      :queues,
      :registry,
      :dynamic_supervisor,
      :uniq_id,
      :compressor
    ]
  end

  alias ExRocketmq.{
    Typespecs,
    Namesrvs,
    Error,
    Models,
    Models.Letter,
    Models.SendMsg,
    Remote,
    Transport.Tcp,
    Util.UniqId,
    Util.Compressor,
    Util.Bool,
    Protocol.Properties,
    Protocol.Request
  }

  alias ExRocketmq.Producer.{State, Selector}

  use GenServer

  require Properties
  require Request

  @property_unique_client_msgid_key Properties.property_unique_client_msgid_key()
  @property_transaction_prepared Properties.property_transaction_prepared()
  @property_msg_type Properties.property_msg_type()

  @req_send_msg Request.req_send_message()
  @req_send_batch_msg Request.req_send_batch_message()
  @req_send_reply_msg Request.req_send_reply_message()

  @producer_opts_schema [
    registry: [
      type: :atom,
      doc: "The registry model of the producer, for dynamic store remote process",
      default: Producer.Registry
    ],
    group_name: [
      type: :string,
      required: true,
      doc: "The group name of the producer"
    ],
    topic: [
      type: :string,
      required: true,
      doc: "The topic of the producer"
    ],
    namesrvs: [
      type: :any,
      required: true,
      doc: "The namesrvs instance of the producer"
    ],
    namespace: [
      type: :string,
      default: "",
      doc: "The namespace of the producer"
    ],
    retry_times: [
      type: :non_neg_integer,
      default: 3,
      doc: "The retry times of the producer"
    ],
    compress_level: [
      type: {:or, [:non_neg_integer, :atom]},
      default: 5,
      doc: "The compress level of the producer, see zlib.zlevel"
    ],
    compress_body_over_howmuch: [
      type: :non_neg_integer,
      default: 4096,
      doc: "Do compressing when body over howmuch"
    ],
    sendmsg_timeout: [
      type: :timeout,
      default: 3000,
      doc: "The sendmsg timeout of the producer"
    ],
    mq_selector: [
      type: :any,
      default: ExRocketmq.Producer.Selector.Random.new(),
      doc: "The queue selector of the producer"
    ],
    create_topic_key: [
      type: :string,
      default: "TBW102",
      doc: "The create topic key of the producer"
    ],
    create_topic_queue_num: [
      type: :non_neg_integer,
      default: 4,
      doc: "The create topic queue num of the producer"
    ]
  ]

  @master_id "0"

  defstruct [
    :registry,
    :group_name,
    :topic,
    :namesrvs,
    :namespace,
    :retry_times,
    :compress_level,
    :compress_body_over_howmuch,
    :sendmsg_timeout,
    :create_topic_key,
    :create_topic_queue_num
  ]

  @type t :: %__MODULE__{
          registry: atom(),
          group_name: Typespecs.group_name(),
          topic: Typespecs.topic(),
          namesrvs: ExRocketmq.Namesrvs.t(),
          namespace: Typespecs.namespace(),
          retry_times: non_neg_integer(),
          compress_level: Typespecs.compress_level(),
          compress_body_over_howmuch: non_neg_integer(),
          sendmsg_timeout: timeout(),
          create_topic_key: Typespecs.topic(),
          create_topic_queue_num: non_neg_integer()
        }

  @type producer_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@producer_opts_schema))]

  @spec new(producer_opts_schema_t()) :: t()
  def new(opts) do
    opts =
      opts
      |> NimbleOptions.validate!(@producer_opts_schema)

    struct(__MODULE__, opts)
  end

  @spec start_link(Typespecs.opts()) :: Typespecs.on_start()
  def start_link(opts) do
    {producer, opts} = Keyword.pop(opts, :producer)
    GenServer.start_link(__MODULE__, producer, opts)
  end

  def init(producer) do
    {:ok, _} = Namesrvs.start_link(namesrvs: producer.namesrvs)
    {:ok, _} = Selector.start_link(producer.mq_selector)

    {:ok, supervisor} =
      DynamicSupervisor.start_link(strategy: :one_for_one)

    {:ok, _} = start_registry(producer.registry)
    {:ok, uniq_id} = UniqId.start_link()
    {:ok, compressor} = Compressor.start_link(level: producer.compress_level)

    {:ok,
     %State{
       producer: producer,
       broker_datas: [],
       queues: [],
       registry: producer.registry,
       dynamic_supervisor: supervisor,
       uniq_id: uniq_id,
       compressor: compressor
     }, {:continue, :update_route_info}}
  end

  defp start_registry(name) do
    name
    |> Process.whereis()
    |> case do
      nil -> Registry.start_link(keys: :unique, name: name)
      pid -> {:ok, pid}
    end
  end

  def handle_continue(:update_route_info, state) do
    Process.send_after(self(), :update_route_info, 10)
    {:noreply, state}
  end

  def handle_info(:update_route_info, %{producer: producer} = state) do
    {:ok, route_info} =
      Namesrvs.query_topic_route_info(producer.namesrvs, producer.topic)

    queues =
      route_info.queue_datas
      |> Enum.sort_by(& &1.broker_name)
      |> Enum.reduce([], fn queue_data, acc ->
        Models.MessageQueue.from_queue_data(queue_data, producer.topic) ++ acc
      end)

    Process.send_after(self(), :update_route_info, 30_000)
    {:noreply, %{state | broker_datas: route_info.broker_datas, queues: queues}}
  end

  def handle_call({:send_sync, []}, _from, state),
    do: {:reply, {:error, Error.new("empty msgs")}, state}

  def handle_call(
        {:send_sync, msgs},
        _from,
        %{
          producer: producer,
          queues: queues,
          broker_datas: broker_datas,
          registry: registry,
          dynamic_supervisor: supervisor,
          compressor: compressor
        } = state
      ) do
    with :ok <- msgs_valid?(msgs),
         msg <- batch_msgs(msgs),
         {msg, key} <- assign_key(msg, state.uniq_id),
         msg <- compress_msg(msg, compressor, producer.compress_body_over_howmuch),
         q <- Selector.select(producer, msg, queues),
         {:ok, addr} <- get_broker_addr(broker_datas, q.broker_name),
         {:ok, remote} <- get_remote(addr, registry, supervisor),
         req <- %SendMsg.Request{
           producer_group: producer.group_name,
           topic: producer.topic,
           queue_id: q.queue_id,
           sys_flag: get_sysflag(msg),
           born_timestamp: System.system_time(:millisecond),
           flag: msg.flag,
           properties: Letter.encode_properties(msg),
           reconsume_times: 0,
           unit_mode: false,
           batch: msg.batch,
           default_topic: producer.create_topic_key,
           default_topic_queue_num: producer.create_topic_queue_num
         },
         req_msg <- Remote.Message.new_command_message(req_code(msg), req, msg.body),
         {:ok, resp_msg} <- Remote.rpc(remote, req_msg),
         {:ok, %{queue: queue} = resp} <- SendMsg.Response.from_msg(resp_msg),
         queue <- %{queue | topic: producer.topic, broker_name: q.broker_name} do
      {:reply, {:ok, %{resp | msg_id: key, queue: queue}}, state}
    else
      {:error, reason} ->
        {:reply, {:error, Error.new(inspect(reason))}, state}
    end
  end

  @spec get_broker_addr([Models.BrokerData.t()], String.t()) ::
          {:ok, String.t()} | {:error, any()}
  defp get_broker_addr(broker_datas, broker_name) do
    broker_datas
    |> Enum.find(fn x -> x.broker_name == broker_name end)
    |> case do
      nil -> {:error, Error.new("broker #{broker_name} not found")}
      %{broker_addrs: %{@master_id => addr}} -> {:ok, addr}
      _ -> {:error, Error.new("no master broker for #{broker_name}")}
    end
  end

  @spec msgs_valid?([Letter.t()]) :: :ok | {:error, any()}
  defp msgs_valid?(msgs) do
    msgs
    |> Enum.map(& &1.topic)
    |> Enum.uniq()
    |> Enum.count()
    |> Kernel.==(1)
    |> if do
      :ok
    else
      {:error, Error.new("invalid msgs")}
    end
  end

  @spec batch_msgs([Letter.t()]) :: Letter.t()
  defp batch_msgs([msg]), do: msg

  defp batch_msgs([msg | _] = msgs) do
    body = msgs |> Enum.map(&Letter.encode(&1)) |> IO.iodata_to_binary()

    %Letter{
      body: body,
      flag: msg.flag,
      batch: true,
      compress: msg.compress
    }
  end

  @spec parse_addr(String.t()) :: {String.t(), non_neg_integer()}
  defp parse_addr(addr) do
    [host, port] = String.split(addr, ":")
    {host, String.to_integer(port)}
  end

  @spec get_remote(String.t(), atom(), atom()) :: {:ok, pid()} | {:error, any()}
  defp get_remote(addr, registry, supervisor) do
    {host, port} = parse_addr(addr)

    Registry.lookup(registry, addr)
    |> case do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        r =
          Remote.new(
            name: {:via, Registery, {registry, addr}},
            transport:
              Tcp.new(
                host: host,
                port: port
              )
          )

        DynamicSupervisor.start_child(supervisor, {Remote, remote: r})
    end
  end

  @spec assign_key(Letter.t(), pid()) :: {Letter.t(), String.t()}
  defp assign_key(%{batch: false, properties: properties} = msg, uniq_id) do
    properties
    |> case do
      %{@property_unique_client_msgid_key => key} when key not in [nil, ""] ->
        {msg, key}

      _ ->
        key = UniqId.get_uniq_id(uniq_id)

        {%Letter{
           msg
           | properties: Map.put(properties, @property_unique_client_msgid_key, key)
         }, key}
    end
  end

  defp assign_key(msg, _), do: {msg, ""}

  @spec compress_msg(Letter.t(), pid(), non_neg_integer()) :: Letter.t()
  defp compress_msg(%{compress: true} = msg, _, _), do: msg
  defp compress_msg(%{batch: true} = msg, _, _), do: msg

  defp compress_msg(%{body: body} = msg, compressor, compress_over_howmuch) do
    if byte_size(body) > compress_over_howmuch do
      compressed_body = Compressor.compress(compressor, body)
      %Letter{msg | body: compressed_body, compress: true}
    else
      msg
    end
  end

  @spec get_sysflag(Letter.t()) :: non_neg_integer()
  defp get_sysflag(msg) do
    0
    |> set_compress_flag(msg)
    |> set_transaction_prepared_flag(msg)
  end

  defp set_compress_flag(flag, %{compress: true}) do
    flag
    |> SendMsg.Request.set_compress_flag()
  end

  defp set_compress_flag(flag, _), do: flag

  defp set_transaction_prepared_flag(flag, %{
         properties: %{@property_transaction_prepared => bool}
       }) do
    if Bool.boolean?(bool) do
      flag
      |> SendMsg.Request.set_transaction_prepared_flag()
    else
      flag
    end
  end

  defp set_transaction_prepared_flag(flag, _), do: flag

  defp req_code(%{properties: %{@property_msg_type => "reply"}}), do: @req_send_reply_msg
  defp req_code(%{batch: true}), do: @req_send_batch_msg
  defp req_code(_), do: @req_send_msg
end
