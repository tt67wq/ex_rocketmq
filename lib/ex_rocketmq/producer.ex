defmodule ExRocketmq.Producer do
  @moduledoc """
  This module provides a GenServer implementation of a RocketMQ producer.

  ## Example

  - First, write a producer task:
  ```Elixir
  defmodule DemoProducer do

    use Task

    alias ExRocketmq.Producer
    alias ExRocketmq.Models.Message

    @topic "POETRY"
    @msgs "风劲角弓鸣，将军猎渭城。
    草枯鹰眼疾，雪尽马蹄轻。
    忽过新丰市，还归细柳营。
    回看射雕处，千里暮云平。"

    def start_link(opts) do
      Task.start_link(__MODULE__, :run, [opts])
    end

    defp get_msg() do
      @msgs
      |> String.split("\n")
    end

    def run(opts) do
      get_msg()
      |> Enum.each(fn msgs ->
        to_emit =
          msgs
          |> Enum.map(fn msg ->
            %Message{topic: @topic, body: msg}
          end)

        Producer.send_sync(:producer, to_emit)
      end)

      Process.sleep(5000)
      run(opts)
    end
  end
  ```

  - Second, add namesrvs and producer and your msg produce task to supervisor tree:
  ```Elixir
  children = [
    {Namesrvs,
     remotes: [
       [transport: Transport.Tcp.new(host: "test.rocket-mq.net", port: 31_120)]
     ],
     opts: [
       name: :namesrvs
     ]},
    {
      Producer,
      group_name: "GID_POETRY",
      namesrvs: :namesrvs,
      trace_enable: true,
      opts: [
        name: :producer
      ]
    },
    {
      DemoProducer,
      []
    }
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
  ```

  This module support both normal producer and transactional producer, if you want to use transactional producer,
  you must implement `ExRocketmq.Producer.Transaction` behaviour, see this [example](https://github.com/tt67wq/ex_rocketmq/blob/master/examples/trans_producer.exs) for more details.
  """
  defmodule State do
    @moduledoc false

    alias ExRocketmq.{Models, Typespecs}

    @type t :: %__MODULE__{
            client_id: String.t(),
            group_name: String.t(),
            namesrvs: pid() | atom(),
            publish_info_map: %{
              Typespecs.topic() => {list(Models.BrokerData.t()), list(Models.MessageQueue.t())}
            },
            selector: ExRocketmq.Producer.MqSelector.t(),
            compress: keyword(),
            transaction_listener: ExRocketmq.Producer.Transaction.t(),
            trace_enable: boolean(),
            supervisor: pid() | atom()
          }

    defstruct client_id: "",
              group_name: "",
              namesrvs: nil,
              publish_info_map: %{},
              selector: nil,
              compress: nil,
              transaction_listener: nil,
              trace_enable: false,
              supervisor: nil
  end

  require ExRocketmq.Protocol.MsgType

  alias ExRocketmq.{
    Typespecs,
    Namesrvs,
    Util,
    Broker,
    Producer.MqSelector,
    Compressor,
    Remote.Packet,
    Tracer,
    Exception
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
    CheckTransactionState,
    Trace,
    TraceItem
  }

  alias ExRocketmq.Protocol.{
    Properties,
    Flag,
    Request,
    Response,
    Transaction,
    SendStatus,
    MsgType
  }

  require Logger
  require Properties
  require Flag
  require Response
  require Request
  require Transaction
  require Packet
  require SendStatus

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
      doc:
        "The transaction listener of the producer, must implement `ExRocketmq.Producer.Transaction`, see `ExRocketmq.Producer.Transaction` for more details",
      required: false
    ],
    trace_enable: [
      type: :boolean,
      default: false,
      doc: "The trace enable of the producer"
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
  @send_success SendStatus.send_ok()

  # transaction type
  @transaction_not_type Transaction.not_type()
  @transaction_commit_type Transaction.commit_type()
  @transaction_rollback_type Transaction.rollback_type()

  # request
  @req_check_transaction_state Request.req_check_transaction_state()

  # msg type
  @msg_type_normal MsgType.normal_msg()
  @msg_type_trans_half MsgType.trans_msg_half()
  @msg_type_trans_commit MsgType.trans_msg_commit()

  @type producer_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@producer_opts_schema))]
  @type publish_map :: %{
          Typespecs.topic() => {list(BrokerData.t()), list(MessageQueue.t())}
        }

  @doc """
  start producer process

  ## Options
  #{NimbleOptions.docs(@producer_opts_schema)}

  ## Examples

      iex> ExRocketmq.Producer.start_link(
        group_name: "GID_POETRY",
        namesrvs: :namesrvs,
        trace_enable: true,
        opts: [
          name: :producer
        ]
      )
  """
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

  @doc """
  Sends a message to the broker as part of a transaction and waits for the broker's response.

  ## Examples

      iex> Producer.send_transaction_msg(producer, %Message{topic: topic, body: "Hello from elixir"})
      {:ok, response, transaction_state}

  """
  @spec send_transaction_msg(pid() | atom(), Message.t()) ::
          {:ok, SendMsg.Response.t(), Typespecs.transaction_state()} | Typespecs.error_t()
  def send_transaction_msg(producer, msgs) do
    GenServer.call(producer, {:send_in_transaction, msgs}, 10_000)
  end

  # -------- server ------
  def init(opts) do
    cid = Util.ClientId.get("Producer")

    {:ok, supervisor} =
      ExRocketmq.Producer.Supervisor.start_link(
        opts: [
          cid: cid,
          trace_enable: opts[:trace_enable],
          namesrvs: opts[:namesrvs]
        ]
      )

    {:ok,
     %State{
       client_id: cid,
       group_name: opts[:group_name],
       namesrvs: opts[:namesrvs],
       publish_info_map: %{},
       selector: opts[:selector],
       compress: opts[:compress],
       transaction_listener: opts[:transaction_listener],
       trace_enable: opts[:trace_enable],
       supervisor: supervisor
     }, {:continue, :on_start}}
  end

  def terminate(reason, %State{
        client_id: cid,
        trace_enable: trace_enable,
        supervisor: supervisor
      }) do
    Logger.info("producer terminate, reason: #{inspect(reason)}")

    # stop uniqid
    Util.UniqId.stop(:"UniqId.#{cid}")

    # stop all existing tasks
    :"Task.Supervisor.#{cid}"
    |> Task.Supervisor.children()
    |> Enum.each(fn pid ->
      Task.Supervisor.terminate_child(:"Task.Supervisor.#{cid}", pid)
    end)

    # stop all broker
    :"DynamicSupervisor.#{cid}"
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Enum.each(fn pid ->
      DynamicSupervisor.terminate_child(:"DynamicSupervisor.#{cid}", pid)
    end)

    DynamicSupervisor.stop(:"DynamicSupervisor.#{cid}")

    # stop tracer
    if trace_enable do
      Tracer.stop(:"Tracer.#{cid}")
    end

    # stop supervisor
    Supervisor.stop(supervisor, reason)
  end

  def handle_continue(:on_start, state) do
    Process.send_after(self(), :update_route_info, 1_000)
    Process.send_after(self(), :heartbeat, 1_000)
    {:noreply, state}
  end

  def handle_call({:send_sync, msgs}, _, state) do
    begin_at = System.system_time(:millisecond)

    with {:ok,
          %{
            broker_pid: broker_pid,
            req: req,
            msg: msg,
            publish_info: pmap,
            broker_data: bd
          }} <-
           prepare_send_msg_request(msgs, state),
         {:ok, resp} <- Broker.sync_send_message(broker_pid, req, msg.body) do
      send_trace(msg, resp, begin_at, bd, @msg_type_normal, state)
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

  def handle_call({:send_in_transaction, _msg}, _, %State{transaction_listener: nil} = state),
    do: {:reply, {:error, :no_transaction_listener}, state}

  def handle_call(
        {:send_in_transaction, msg},
        _,
        %State{
          client_id: cid,
          group_name: group_name,
          transaction_listener: tl
        } = state
      ) do
    begin_at = System.system_time(:millisecond)

    with msg <-
           Message.with_properties(msg, %{
             @property_transaction_prepared => "true",
             @property_producer_group => group_name
           }),
         {:ok, %{broker_pid: broker_pid, req: req, msg: msg, publish_info: pmap, broker_data: bd}} <-
           prepare_send_msg_request([msg], state),
         {:ok, %SendMsg.Response{queue: q} = resp} <-
           Broker.sync_send_message(broker_pid, req, msg.body),
         :ok <- send_trace(msg, resp, begin_at, bd, @msg_type_trans_half, state),
         begin_at = System.system_time(:millisecond),
         :ok <- do_assert(fn -> resp.status == @resp_success end, %{"status" => resp.status}),
         {:ok, %MessageId{} = message_id} <- get_msg_id(resp),
         msg <- Message.with_property(msg, @property_transaction_id, resp.transaction_id),
         msg <- set_tranaction_id(msg),
         {:ok, transaction_state} <- ExRocketmq.Producer.Transaction.execute_local(tl, msg),
         req <- %EndTransaction{
           producer_group: group_name,
           tran_state_table_offset: resp.queue_offset,
           commit_log_offset: message_id.offset,
           commit_or_rollback: transaction_state_to_type(transaction_state),
           msg_id:
             Message.get_property(msg, @property_unique_client_msgid_key, resp.offset_msg_id),
           transaction_id: resp.transaction_id
         },
         {:ok, {broker_datas, _mqs}} <- Map.fetch(pmap, msg.topic),
         bd <- get_broker_data!(broker_datas, q),
         broker_pid <-
           Broker.get_or_new_broker(
             bd.broker_name,
             BrokerData.master_addr(bd),
             :"Registry.#{cid}",
             :"DynamicSupervisor.#{cid}",
             self()
           ),
         :ok <- Broker.end_transaction(broker_pid, req) do
      send_trace(msg, resp, begin_at, bd, @msg_type_trans_commit, state)
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
    with {:ok, %{broker_pid: broker_pid, req: req, msg: msg, publish_info: pmap}} <-
           prepare_send_msg_request(msgs, state),
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
          client_id: cid,
          group_name: group_name,
          transaction_listener: tl
        } = state
      ) do
    Logger.debug("producer receive notify: #{inspect(pkt)}")

    case Packet.packet(pkt, :code) do
      @req_check_transaction_state ->
        # check transaction state
        Task.Supervisor.start_child(
          :"Task.Supervisor.#{cid}",
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
          group_name: group_name
        } = state
      ) do
    heartbeat_data = %Heartbeat{
      client_id: cid,
      producer_data_set: [%ProducerData{group: group_name}]
    }

    :"DynamicSupervisor.#{cid}"
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Task.async_stream(fn pid ->
      Broker.heartbeat(pid, heartbeat_data)
    end)
    |> Stream.run()

    # heartbeat every 30s
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

    ExRocketmq.Producer.Transaction.check_local(transaction_listener, msg_ext)
    |> case do
      {:ok, transaction_state} ->
        req = %EndTransaction{
          producer_group: group_name,
          tran_state_table_offset: header.tran_state_table_offset,
          commit_log_offset: header.commit_log_offset,
          commit_or_rollback: transaction_state_to_type(transaction_state),
          from_transaction_check: true,
          msg_id: Message.get_property(msg, @property_unique_client_msgid_key, msg_ext.msg_id),
          transaction_id:
            Message.get_property(msg, @property_transaction_id, header.transaction_id)
        }

        Broker.end_transaction(broker_pid, req)

      {:error, reason} ->
        Logger.error("check transaction state error: #{inspect(reason)}")
    end
  end

  @spec prepare_send_msg_request([Message.t()], State.t()) ::
          {:ok,
           %{
             broker_pid: pid(),
             req: SendMsg.Request.t(),
             msg: Message.t(),
             publish_info: publish_map(),
             broker_data: BrokerData.t()
           }}
          | {:error, any()}
  defp prepare_send_msg_request(msgs, %State{
         client_id: cid,
         group_name: group_name,
         publish_info_map: pmap,
         namesrvs: namesrvs,
         selector: selector,
         compress: compress
       }) do
    msg =
      msgs
      |> encode_batch()
      |> set_uniqid(:"UniqId.#{cid}")
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
           reply: Message.get_property(msg, @property_msg_type, "") == "reply",
           default_topic: "TBW102",
           default_topic_queue_num: 4
         },
         bd <- get_broker_data!(broker_datas, mq),
         broker_pid <-
           Broker.get_or_new_broker(
             bd.broker_name,
             BrokerData.master_addr(bd),
             :"Registry.#{cid}",
             :"DynamicSupervisor.#{cid}",
             self()
           ) do
      {:ok,
       %{
         broker_pid: broker_pid,
         req: req,
         msg: msg,
         publish_info: pmap,
         broker_data: bd
       }}
    end
  end

  @spec get_broker_data!(list(BrokerData.t()), MessageQueue.t()) :: BrokerData.t()
  defp get_broker_data!(broker_datas, mq) do
    broker_datas
    |> Enum.find(&(&1.broker_name == mq.broker_name))
    |> case do
      nil ->
        raise Exception.new("broker data not found", %{broker_name: mq.broker_name})

      bd ->
        bd
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

  # Dynamically maintaining the mapping between topics and brokers,
  # with the mappings between all topics and brokers being refreshed periodically in the future.
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
          Logger.warning("update publish info for topic #{topic} error")
          pmap
      end
    else
      pmap
    end
  end

  @spec encode_batch([Message.t()]) :: Message.t()
  defp encode_batch(
         [
           %Message{
             topic: topic,
             queue_id: queue_id,
             batch: false
           }
           | _
         ] = msgs
       )
       when length(msgs) > 1 do
    body =
      msgs
      |> Enum.map(&Message.encode/1)
      |> IO.iodata_to_binary()

    %Message{
      topic: topic,
      queue_id: queue_id,
      batch: true,
      body: body
    }
  end

  defp encode_batch([msg]), do: msg

  @spec set_uniqid(Message.t(), pid() | atom()) :: Message.t()
  defp set_uniqid(%Message{} = msg, uniqid) do
    case Message.get_property(msg, @property_unique_client_msgid_key) do
      nil ->
        Message.with_property(
          msg,
          @property_unique_client_msgid_key,
          Util.UniqId.get_uniq_id(uniqid)
        )

      _ ->
        msg
    end
  end

  # defp set_uniqid(msg, _), do: msg

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

  @spec get_msg_id(SendMsg.Response.t()) :: {:ok, MessageId.t()} | {:error, any()}
  defp get_msg_id(%SendMsg.Response{offset_msg_id: offset_msg_id}) when offset_msg_id != "",
    do: MessageId.decode(offset_msg_id)

  defp get_msg_id(_), do: {:error, :no_msg_id}

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

  @spec send_trace(
          msg :: Message.t(),
          resp :: SendMsg.Response.t(),
          begin_at :: non_neg_integer(),
          broker_data :: BrokerData.t(),
          msg_type :: non_neg_integer(),
          state :: State.t()
        ) :: :ok
  defp send_trace(_, _, _, _, _, %State{trace_enable: false}), do: :ok

  defp send_trace(
         %Message{
           topic: topic,
           body: body
         } = msg,
         %SendMsg.Response{
           status: status,
           region_id: region_id,
           offset_msg_id: offset_msg_id
         },
         begin_at,
         bd,
         msg_type,
         %State{
           client_id: cid,
           trace_enable: true,
           group_name: group_name
         }
       ) do
    store_host =
      bd
      |> BrokerData.master_addr()
      |> String.split(":")
      |> List.first()

    trace = %Trace{
      type: :pub,
      timestamp: System.system_time(:millisecond),
      region_id: region_id,
      region_name: "",
      group_name: group_name,
      cost_time: System.system_time(:millisecond) - begin_at,
      success?: status == @send_success,
      items: [
        %TraceItem{
          topic: topic,
          tags: Message.get_property(msg, "TAGS", ""),
          keys: Message.get_property(msg, "KEYS", ""),
          store_host: store_host,
          client_host: Util.Network.local_ip_addr(),
          body_length: byte_size(body),
          msg_type: msg_type,
          offset_msg_id: offset_msg_id,
          msg_id: Message.get_property(msg, @property_unique_client_msgid_key, ""),
          store_time: System.system_time(:millisecond)
        }
      ]
    }

    Tracer.send_trace(:"Tracer.#{cid}", [trace])
  end
end
