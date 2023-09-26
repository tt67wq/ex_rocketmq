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
            task_supervisor: pid(),
            broker_dynamic_supervisor: pid(),
            registry: pid(),
            uniqid: pid(),
            publish_info_map: %{
              Typespecs.topic() => {list(Models.BrokerData.t()), list(Models.MessageQueue.t())}
            },
            selector: ExRocketmq.Producer.MqSelector.t(),
            compress: keyword(),
            transaction_listener: ExRocketmq.Producer.Transaction.t()
          }

    defstruct client_id: "",
              group_name: "",
              namesrvs: nil,
              task_supervisor: nil,
              broker_dynamic_supervisor: nil,
              registry: nil,
              uniqid: nil,
              publish_info_map: %{},
              selector: nil,
              compress: nil,
              transaction_listener: ExRocketmq.Producer.MockTransaction.new()
  end

  alias ExRocketmq.{
    Typespecs,
    Namesrvs,
    Util,
    Transport,
    Broker,
    Producer.MqSelector,
    Compressor,
    Remote.Packet
  }

  alias ExRocketmq.Models.{
    Message,
    MessageExt,
    QueueData,
    MessageQueue,
    BrokerData,
    SendMsg,
    Heartbeat,
    ProducerData,
    MessageId,
    EndTransaction,
    CheckTransactionState
  }

  alias ExRocketmq.Protocol.{
    Properties,
    Flag,
    Request,
    Response,
    Transaction
  }

  require Logger
  require Properties
  require Flag
  require Response
  require Request
  require Transaction
  require Packet

  use GenServer

  import ExRocketmq.Util.Assertion

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
      default: MqSelector.Random.new([]),
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
    transaction_listener: [
      type: :any,
      doc: "The transaction listener of the producer",
      default: ExRocketmq.Producer.MockTransaction.new()
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
  @property_producer_group Properties.property_producer_group()
  @property_msg_type Properties.property_msg_type()
  @property_transaction_id Properties.property_transaction_id()

  # flag
  @flag_compressed Flag.flag_compressed()
  @flag_transaction_prepared Flag.flag_transaction_prepared()

  # resp
  @resp_success Response.resp_success()

  # transaction type
  @transaction_not_type Transaction.not_type()
  @transaction_commit_type Transaction.commit_type()
  @transaction_rollback_type Transaction.rollback_type()

  # request
  @req_check_transaction_state Request.req_check_transaction_state()

  @type producer_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@producer_opts_schema))]
  @type publish_map :: %{
          Typespecs.topic() => {list(BrokerData.t()), list(MessageQueue.t())}
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

  @spec send_transaction_msg(pid() | atom(), Message.t()) ::
          {:ok, SendMsg.Response.t(), Typespecs.transaction_state()} | Typespecs.error_t()
  def send_transaction_msg(producer, msgs) do
    GenServer.call(producer, {:send_in_transaction, msgs}, 10_000)
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
    {:ok, task_supervisor} = Task.Supervisor.start_link()
    {:ok, broker_dynamic_supervisor} = DynamicSupervisor.start_link([])

    {:ok,
     %State{
       client_id: Util.ClientId.get(),
       group_name: opts[:group_name],
       namesrvs: opts[:namesrvs],
       task_supervisor: task_supervisor,
       broker_dynamic_supervisor: broker_dynamic_supervisor,
       registry: registry,
       uniqid: uniqid,
       publish_info_map: %{},
       selector: opts[:selector],
       compress: opts[:compress],
       transaction_listener: opts[:transaction_listener]
     }, {:continue, :on_start}}
  end

  def terminate(reason, %State{
        uniqid: uniqid,
        broker_dynamic_supervisor: broker_dynamic_supervisor,
        task_supervisor: task_supervisor
      }) do
    Logger.info("producer terminate, reason: #{inspect(reason)}")

    # stop uniqid
    Util.UniqId.stop(uniqid)

    # stop all existing tasks
    Task.Supervisor.children(task_supervisor)
    |> Enum.each(fn pid ->
      Task.Supervisor.terminate_child(task_supervisor, pid)
    end)

    # stop all broker
    broker_dynamic_supervisor
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Enum.each(fn pid ->
      DynamicSupervisor.terminate_child(broker_dynamic_supervisor, pid)
    end)

    DynamicSupervisor.stop(broker_dynamic_supervisor)
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

  def handle_call(
        {:send_in_transaction, msg},
        _,
        %State{
          group_name: group_name,
          transaction_listener: tl,
          registry: registry,
          broker_dynamic_supervisor: broker_dynamic_supervisor
        } = state
      ) do
    with msg <- %{
           msg
           | properties:
               Map.merge(msg.properties, %{
                 @property_transaction_prepared => "true",
                 @property_producer_group => group_name
               })
         },
         {:ok, {broker_pid, req, msg, pmap}} <- prepare_send_msg_request([msg], state),
         {:ok, %SendMsg.Response{queue: q} = resp} <-
           Broker.sync_send_message(broker_pid, req, msg.body),
         :ok <- do_assert(fn -> resp.status == @resp_success end, %{"status" => resp.status}),
         {:ok, %MessageId{} = message_id} <- get_msg_id(resp),
         msg <- %{
           msg
           | properties: Map.put(msg.properties, @property_transaction_id, resp.transaction_id)
         },
         msg <- set_tranaction_id(msg),
         {:ok, transaction_state} <- ExRocketmq.Producer.Transaction.execute_local(tl, msg),
         req <- %EndTransaction{
           producer_group: group_name,
           tran_state_table_offset: resp.queue_offset,
           commit_log_offset: message_id.offset,
           commit_or_rollback: transaction_state_to_type(transaction_state),
           msg_id: resp.msg_id,
           transaction_id: resp.transaction_id
         },
         {:ok, {broker_datas, _mqs}} <- Map.fetch(pmap, msg.topic),
         {:ok, %BrokerData{} = bd} <- get_broker_data(broker_datas, q),
         broker_pid <-
           get_or_new_broker(
             bd.broker_name,
             BrokerData.master_addr(bd),
             registry,
             broker_dynamic_supervisor
           ),
         :ok <- Broker.end_transaction(broker_pid, req) do
      {:reply, {:ok, resp, transaction_state}, %{state | publish_info_map: pmap}}
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

  def handle_info(:update_route_info, %State{publish_info_map: pmap, namesrvs: namesrvs} = state) do
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

  def handle_info(
        {:notify, {pkt, broker_pid}},
        %State{
          group_name: group_name,
          transaction_listener: tl,
          task_supervisor: task_supervisor
        } = state
      ) do
    # Logger.warning("producer receive notify: #{inspect(pkt)}")

    case Packet.packet(pkt, :code) do
      @req_check_transaction_state ->
        # check transaction state
        Task.Supervisor.start_child(
          task_supervisor,
          fn -> notify_check_transaction_state(pkt, tl, group_name, broker_pid) end
        )

      other_code ->
        Logger.warning("unknown notify code: #{other_code}")
    end

    {:noreply, state}
  end

  def handle_info(
        :heartbeat,
        %State{
          client_id: cid,
          group_name: group_name,
          broker_dynamic_supervisor: broker_dynamic_supervisor
        } = state
      ) do
    heartbeat_data = %Heartbeat{
      client_id: cid,
      producer_data_set: [%ProducerData{group: group_name}]
    }

    broker_dynamic_supervisor
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Enum.map(fn pid ->
      Task.async(fn -> Broker.heartbeat(pid, heartbeat_data) end)
    end)
    |> Task.await_many()

    Process.send_after(self(), :heartbeat, 30_000)

    {:noreply, state}
  end

  # ------- private funcs ------

  @spec notify_check_transaction_state(
          Packet.t(),
          ExRocketmq.Producer.Transaction.t(),
          Typespecs.group_name(),
          pid()
        ) :: :ok
  defp notify_check_transaction_state(pkt, transaction_listener, group_name, broker_pid) do
    header =
      pkt
      |> Packet.packet(:ext_fields)
      |> CheckTransactionState.decode()

    [msg_ext] =
      pkt
      |> Packet.packet(:body)
      |> MessageExt.decode_from_binary()

    msg_ext = %MessageExt{
      msg_ext
      | message: set_tranaction_id(msg_ext.message)
    }

    %MessageExt{message: msg} = msg_ext

    with {:ok, transaction_state} <-
           ExRocketmq.Producer.Transaction.check_local(transaction_listener, msg_ext),
         req = %EndTransaction{
           producer_group: group_name,
           tran_state_table_offset: header.tran_state_table_offset,
           commit_log_offset: header.commit_log_offset,
           commit_or_rollback: transaction_state_to_type(transaction_state),
           from_transaction_check: true,
           msg_id: Map.get(msg.properties, @property_unique_client_msgid_key, msg_ext.msg_id),
           transaction_id:
             Map.get(msg.properties, @property_transaction_id, header.transaction_id)
         } do
      Broker.end_transaction(broker_pid, req)
    else
      {:error, reason} -> Logger.error("check transaction state error: #{inspect(reason)}")
      others -> Logger.error("check transaction state error: #{inspect(others)}")
    end
  end

  @spec prepare_send_msg_request([Message.t()], State.t()) ::
          {:ok, {pid(), SendMsg.Request.t(), Message.t(), publish_map()}} | {:error, any()}
  defp prepare_send_msg_request(msgs, %State{
         group_name: group_name,
         publish_info_map: pmap,
         namesrvs: namesrvs,
         selector: selector,
         registry: registry,
         uniqid: uniqid,
         compress: compress,
         broker_dynamic_supervisor: broker_dynamic_supervisor
       }) do
    msg =
      msgs
      |> encode_batch()
      |> set_uniqid(uniqid)
      |> compress_msg(compress[:compress_over], compress[:compressor], compress[:compress_level])

    pmap = update_publish_info(pmap, msg.topic, namesrvs)

    with {:ok, {broker_datas, mqs}} <- get_publish_info(pmap, msg.topic),
         mq = MqSelector.select(selector, msg, mqs),
         req = %SendMsg.Request{
           producer_group: group_name,
           topic: msg.topic,
           queue_id: mq.queue_id,
           sys_flag: transaction_sysflag(msg, compress_sysflag(msg, 0)),
           born_timestamp: System.system_time(:millisecond),
           flag: msg.flag,
           properties: Message.encode_properties(msg),
           reconsume_times: 0,
           max_reconsume_times: 3,
           unit_mode: false,
           batch: msg.batch,
           reply: Map.get(msg.properties, @property_msg_type, "") == "reply",
           default_topic: "TBW102",
           default_topic_queue_num: 4
         },
         {:ok, bd} <- get_broker_data(broker_datas, mq),
         broker_pid <-
           get_or_new_broker(
             bd.broker_name,
             BrokerData.master_addr(bd),
             registry,
             broker_dynamic_supervisor
           ) do
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

  @spec update_publish_info(publish_map(), Typespecs.topic(), pid() | atom()) :: publish_map()
  defp update_publish_info(pmap, topic, namesrvs) do
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

  @spec get_or_new_broker(String.t(), String.t(), atom(), pid()) :: pid()
  defp get_or_new_broker(broker_name, addr, registry, broker_dynamic_supervisor) do
    Registry.lookup(registry, addr)
    |> case do
      [] ->
        {host, port} =
          addr
          |> Util.Network.parse_addr()

        broker_opts = [
          broker_name: broker_name,
          remote_opts: [transport: Transport.Tcp.new(host: host, port: port)],
          opts: [name: {:via, Registry, {registry, addr}}]
        ]

        {:ok, pid} =
          DynamicSupervisor.start_child(broker_dynamic_supervisor, {Broker, broker_opts})

        # bind self to broker, then notify from broker will send to self
        Broker.controlling_process(pid, self())

        pid

      [{pid, _}] ->
        pid
    end
  end

  @spec set_uniqid(Message.t(), pid()) :: Message.t()
  defp set_uniqid(%Message{batch: false, properties: properties} = msg, uniqid) do
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
  defp transaction_sysflag(%{properties: %{@property_transaction_prepared => "true"}}, sysflag),
    do: Bitwise.bor(sysflag, @flag_transaction_prepared)

  defp transaction_sysflag(_, sysflag), do: sysflag

  @spec set_tranaction_id(Message.t()) :: Message.t()
  defp set_tranaction_id(%Message{properties: %{@property_unique_client_msgid_key => key}} = msg)
       when key not in [nil, ""] do
    %{msg | transaction_id: key}
  end

  defp set_tranaction_id(msg), do: msg

  @spec get_msg_id(%SendMsg.Response{}) :: {:ok, MessageId.t()} | {:error, any()}
  defp get_msg_id(%SendMsg.Response{offset_msg_id: offset_msg_id}) when offset_msg_id != "",
    do: MessageId.decode(offset_msg_id)

  defp get_msg_id(%SendMsg.Response{msg_id: msg_id}), do: MessageId.decode(msg_id)

  @spec transaction_state_to_type(Typespecs.transaction_state()) :: Typespecs.transaction_type()
  defp transaction_state_to_type(transaction_state) do
    (fn
       :commit -> @transaction_commit_type
       :rollback -> @transaction_rollback_type
       _ -> @transaction_not_type
     end).(transaction_state)
  end

  @spec get_publish_info(publish_map(), Typespecs.topic()) ::
          {:ok, {list(BrokerData.t()), list(MessageQueue.t())}} | {:error, any()}
  defp get_publish_info(pmap, topic) do
    Map.fetch(pmap, topic)
    |> case do
      {:ok, _} = val -> val
      _ -> {:error, "topic not in publish info"}
    end
  end
end
