defmodule ExRocketmq.Producer do
  @moduledoc """
  Rocketmq Producer
  """
  defmodule State do
    @moduledoc """
    The state of the producer GenServer
    """
    alias ExRocketmq.{Models, Typespecs}

    @type t :: %__MODULE__{
            client_id: String.t(),
            group_name: String.t(),
            namesrvs: pid() | atom(),
            dynamic_supervisor: pid(),
            registry: pid(),
            uniqid: pid(),
            publish_info_map: %{
              Typespecs.topic() => {list(Models.BrokerData.t()), list(Models.MessageQueue.t())}
            },
            selector: struct(),
            compress: keyword()
          }

    defstruct client_id: "",
              group_name: "",
              namesrvs: nil,
              dynamic_supervisor: nil,
              registry: nil,
              uniqid: nil,
              publish_info_map: %{},
              selector: nil,
              compress: nil
  end

  alias ExRocketmq.{
    Typespecs,
    Namesrvs,
    Util,
    Transport,
    Broker,
    Producer.Selector,
    Compressor
  }

  alias ExRocketmq.Models.{
    Message,
    QueueData,
    MessageQueue,
    BrokerData,
    SendMsg,
    Heartbeat,
    ProducerData
  }

  alias ExRocketmq.Protocol.{
    Properties,
    Flag
  }

  require Logger
  require Properties
  require Flag

  use GenServer

  @producer_opts_schema [
    group_name: [
      type: :string,
      required: true,
      doc: "The group name of the producer"
    ],
    namesrvs: [
      type: {:or, [:pid, :atom]},
      required: true,
      doc: "The namesrvs process"
    ],
    namespace: [
      type: :string,
      default: "",
      doc: "The namespace of the producer"
    ],
    selector: [
      type: :any,
      default: Selector.Random.new([]),
      doc: "The mq selector of the producer"
    ],
    compress: [
      type: :keyword_list,
      doc: "The compress opts of the producer",
      keys: [
        compressor: [
          type: :atom,
          default: ExRocketmq.Compress.Zlib,
          doc: "The compress implemention of the producer"
        ],
        compress_over: [
          type: :integer,
          default: 4096,
          doc: "Do compress when message bytesize over this value"
        ],
        compress_level: [
          type: {:in, [:best_compression, :best_speed, :default, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]},
          default: :best_compression,
          doc: "The compress level of the compressor"
        ]
      ]
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The opts of the producer's GenServer"
    ]
  ]

  # properties
  @property_unique_client_msgid_key Properties.property_unique_client_msgid_key()
  @property_transaction_prepared Properties.property_transaction_prepared()
  @property_msg_type Properties.property_msg_type()

  # flag
  @flag_compressed Flag.flag_compressed()
  @flag_transaction_prepared Flag.flag_transaction_prepared()

  @type producer_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@producer_opts_schema))]
  @type publish_map :: %{
          Typespecs.topic() => {list(Models.BrokerData.t()), list(Models.MessageQueue.t())}
        }

  @spec start_link(producer_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    {opts, init} =
      opts
      |> NimbleOptions.validate!(@producer_opts_schema)
      |> Keyword.pop(:opts)

    GenServer.start_link(__MODULE__, init, opts)
  end

  @spec stop(pid() | atom()) :: :ok
  def stop(producer), do: GenServer.stop(producer)

  @doc """
  send msgs to broker, and wait broker response

  ## Examples

      iex> Producer.send_sync(producer, [%Message{topic: topic, body: "Hello from elixir"}])
      {:ok,
       %ExRocketmq.Models.SendMsg.Response{
         status: 0,
         msg_id: "0A5804A1000051AF000000450648EF0E",
         queue: %ExRocketmq.Models.MessageQueue{
           topic: "SOME_TOPIC",
           broker_name: "broker-0",
           queue_id: 0
         },
         queue_offset: 689717,
         transaction_id: "",
         offset_msg_id: "0A5804A1000051AF000000450648EF0E",
         region_id: "DefaultRegion",
         trace_on: true
       }}
  """
  @spec send_sync(pid() | atom(), [Message.t()]) ::
          {:ok, SendMsg.Response.t()} | Typespecs.error_t()
  def send_sync(producer, msgs) do
    GenServer.call(producer, {:send_sync, msgs}, 10_000)
  end

  @doc """
  send msgs to broker, and don't wait broker response

  ## Examples

      iex> Producer.send_oneway(producer, [%Message{topic: topic, body: "Hello from elixir"}])
      :ok
  """
  @spec send_oneway(pid() | atom(), [Message.t()]) :: :ok
  def send_oneway(producer, msgs) do
    GenServer.cast(producer, {:send_oneway, msgs})
  end

  # -------- server ------
  def init(opts) do
    registry = :"Registry.#{Util.Random.generate_id("P")}"

    {:ok, _} =
      Registry.start_link(
        keys: :unique,
        name: registry
      )

    {:ok, uniqid} = Util.UniqId.start_link()

    {:ok,
     %State{
       client_id: Util.ClientId.get(),
       group_name: opts[:group_name],
       namesrvs: opts[:namesrvs],
       dynamic_supervisor: opts[:dynamic_supervisor],
       registry: registry,
       uniqid: uniqid,
       publish_info_map: %{},
       selector: opts[:selector],
       compress: opts[:compress]
     }, {:continue, :on_start}}
  end

  def terminate(reason, %{uniqid: uniqid, registry: registry}) do
    Logger.info("producer terminate, reason: #{inspect(reason)}")

    # stop uniqid
    Util.UniqId.stop(uniqid)

    # stop all broker
    all_broker_pids(registry)
    |> Enum.each(&Broker.stop/1)
  end

  def handle_continue(:on_start, state) do
    Process.send_after(self(), :update_route_info, 1_000)
    Process.send_after(self(), :heartbeat, 1_000)
    {:noreply, state}
  end

  def handle_call({:send_sync, msgs}, _, state) do
    with {:ok, {broker_pid, req, msg, pmap}} <- prepare_send_msg_request(msgs, state),
         {:ok, resp} <- Broker.sync_send_message(broker_pid, req, msg.body) do
      {:reply, {:ok, resp}, %{state | publish_info_map: pmap}}
    else
      {:error, reason} = error ->
        Logger.error("send message error: #{inspect(reason)}")
        {:reply, error, state}

      other_reason ->
        Logger.error("send message error: #{inspect(other_reason)}")
        {:reply, {:error, :unknown}, state}
    end
  end

  def handle_cast({:send_oneway, msgs}, state) do
    with {:ok, {broker_pid, req, msg, pmap}} <- prepare_send_msg_request(msgs, state),
         :ok <- Broker.one_way_send_message(broker_pid, req, msg.body) do
      {:noreply, %{state | publish_info_map: pmap}}
    end
  end

  def handle_info(:update_route_info, %{publish_info_map: pmap, namesrvs: namesrvs} = state) do
    pmap =
      pmap
      |> Map.keys()
      |> Enum.into(%{}, fn topic ->
        case fetch_publish_info(topic, namesrvs) do
          {:ok, {broker_datas, mqs}} ->
            {topic, {broker_datas, mqs}}

          {:error, _} ->
            {topic, nil}
        end
      end)

    Process.send_after(self(), :update_route_info, 30_000)

    {:noreply, %{state | publish_info_map: pmap}}
  end

  def handle_info({:notify, pkt}, state) do
    Logger.debug("producer receive notify: #{inspect(pkt)}")
    {:noreply, state}
  end

  def handle_info(
        :heartbeat,
        %{client_id: cid, group_name: group_name, registry: registry} = state
      ) do
    heartbeat_data = %Heartbeat{
      client_id: cid,
      producer_data_set: MapSet.new([%ProducerData{group: group_name}])
    }

    registry
    |> all_broker_pids()
    |> Enum.map(fn pid ->
      Task.async(fn -> Broker.heartbeat(pid, heartbeat_data) end)
    end)
    |> Task.await_many()

    Process.send_after(self(), :heartbeat, 30_000)

    {:noreply, state}
  end

  # ------- private funcs ------

  @spec prepare_send_msg_request([Message.t()], State.t()) ::
          {:ok, {pid(), SendMsg.Request.t(), Message.t(), publish_map()}} | {:error, any()}
  defp prepare_send_msg_request(msgs, %State{
         group_name: group_name,
         publish_info_map: pmap,
         namesrvs: namesrvs,
         selector: selector,
         registry: registry,
         uniqid: uniqid,
         compress: compress
       }) do
    with msg <- encode_batch(msgs),
         msg <- set_uniqid(msg, uniqid),
         msg <-
           compress_msg(
             msg,
             compress[:compress_over],
             compress[:compressor],
             compress[:compress_level]
           ),
         sysflag <- compress_sysflag(msg, 0),
         sysflag <- transaction_sysflag(msg, sysflag),
         msg_type <- Map.get(msg.properties, @property_msg_type, ""),
         pmap <- get_or_new_publish_info(pmap, msg.topic, namesrvs),
         {:ok, {broker_datas, mqs}} <- Map.fetch(pmap, msg.topic),
         mq <- Selector.select(selector, msg, mqs),
         req <- %SendMsg.Request{
           producer_group: group_name,
           topic: msg.topic,
           queue_id: mq.queue_id,
           sys_flag: sysflag,
           born_timestamp: System.system_time(:millisecond),
           flag: msg.flag,
           properties: Message.encode_properties(msg),
           reconsume_times: 0,
           max_reconsume_times: 3,
           unit_mode: false,
           batch: msg.batch,
           reply: msg_type == "reply",
           default_topic: "TBW102",
           default_topic_queue_num: 4
         },
         {:ok, bd} <- get_broker_data(broker_datas, mq),
         broker_pid <- get_or_new_broker(bd.broker_name, bd.broker_addrs["0"], registry) do
      {:ok, {broker_pid, req, msg, pmap}}
    end
  end

  @spec get_broker_data(list(BrokerData.t()), MessageQueue.t()) ::
          {:ok, BrokerData.t()} | {:error, any()}
  defp get_broker_data(broker_datas, mq) do
    broker_datas
    |> Enum.find(&(&1.broker_name == mq.broker_name))
    |> case do
      nil -> {:error, "broker data not found, broker_name: #{mq.broker_name}"}
      bd -> {:ok, bd}
    end
  end

  @spec fetch_publish_info(Typespecs.topic(), pid()) ::
          {:ok, {list(BrokerData.t()), list(MessageQueue.t())}} | {:error, any()}
  defp fetch_publish_info(topic, namesrvs) do
    Namesrvs.query_topic_route_info(namesrvs, topic)
    |> case do
      {:ok, %{broker_datas: broker_datas, queue_datas: queue_datas}} ->
        mqs =
          queue_datas
          |> Enum.map(&QueueData.to_publish_queues(&1, topic))
          |> List.flatten()

        {:ok, {broker_datas, mqs}}

      {:error, reason} = error ->
        Logger.error("query topic route info error: #{inspect(reason)}")
        error
    end
  end

  @spec get_or_new_publish_info(publish_map(), Typespecs.topic(), pid() | atom()) :: publish_map()
  defp get_or_new_publish_info(pmap, topic, namesrvs) do
    pmap
    |> Map.get(topic)
    |> is_nil()
    |> if do
      case fetch_publish_info(topic, namesrvs) do
        {:ok, {topic_route_info, mqs}} ->
          Map.put(pmap, topic, {topic_route_info, mqs})

        {:error, _} ->
          pmap
      end
    else
      pmap
    end
  end

  @spec encode_batch([Message.t()]) :: Message.t()
  defp encode_batch([msg | _] = msgs) when length(msgs) > 1 do
    body =
      msgs
      |> Enum.map(&Message.encode/1)
      |> IO.iodata_to_binary()

    %Message{
      topic: msg.topic,
      queue_id: msg.queue_id,
      batch: true,
      body: body
    }
  end

  defp encode_batch([msg]), do: msg

  @spec get_or_new_broker(String.t(), String.t(), atom()) :: pid()
  defp get_or_new_broker(broker_name, addr, registry) do
    Registry.lookup(registry, addr)
    |> case do
      [] ->
        {host, port} = Util.Network.parse_addr(addr)

        {:ok, pid} =
          [
            broker_name: broker_name,
            remote_opts: [transport: Transport.Tcp.new(host: host, port: port)],
            opts: [name: {:via, Registry, {registry, addr}}]
          ]
          |> Broker.start_link()

        # bind self to broker, then notify from broker will send to self
        Broker.controlling_process(pid, self())

        pid

      [{pid, _}] ->
        pid
    end
  end

  @spec set_uniqid(Message.t(), pid()) :: Message.t()
  defp set_uniqid(%{batch: false, properties: properties} = msg, uniqid) do
    case Map.get(properties, @property_unique_client_msgid_key) do
      nil ->
        properties =
          properties
          |> Map.put(@property_unique_client_msgid_key, Util.UniqId.get_uniq_id(uniqid))

        %{msg | properties: properties}

      _ ->
        msg
    end
  end

  defp set_uniqid(msg, _), do: msg

  @spec compress_msg(Message.t(), non_neg_integer(), module(), any()) :: Message.t()
  defp compress_msg(
         %{compress: false, batch: false, body: body} = msg,
         compress_over,
         compressor,
         compress_level
       )
       when byte_size(body) > compress_over do
    body = Compressor.compress(compressor, body, level: compress_level)
    %{msg | compress: true, body: body}
  end

  defp compress_msg(msg, _, _, _), do: msg

  @spec compress_sysflag(Message.t(), Typespecs.sysflag()) :: Typespecs.sysflag()
  defp compress_sysflag(%{compress: true}, sysflag), do: Bitwise.bor(sysflag, @flag_compressed)
  defp compress_sysflag(_, sysflag), do: sysflag

  @spec transaction_sysflag(Message.t(), Typespecs.sysflag()) :: Typespecs.sysflag()
  defp transaction_sysflag(%{properties: %{@property_transaction_prepared => val}}, sysflag) do
    val
    |> Util.Bool.boolean?()
    |> if do
      Bitwise.bor(sysflag, @flag_transaction_prepared)
    else
      sysflag
    end
  end

  defp transaction_sysflag(_, sysflag), do: sysflag

  @spec all_broker_pids(atom()) :: list(pid())
  defp all_broker_pids(registry) do
    Registry.select(registry, [{{:"$1", :"$2", :"$3"}, [], [:"$2"]}])
  end
end
