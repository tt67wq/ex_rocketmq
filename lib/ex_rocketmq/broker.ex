defmodule ExRocketmq.Broker do
  @moduledoc """
  RocketMQ Broker Client

  The broker is the core component of RocketMQ. It handles responsibilities such as message storage,
  forwarding, and fulfilling consumer queries.

  Each broker cluster has a name server that handles registration and discovery.
  The broker registers topic and queue metadata with the name server,
  and producers and consumers locate brokers through the name server to send and receive messages.

  Within each broker there can be multiple queues, which store messages.
  For a given topic, it can be deployed with multiple replicated queues across multiple brokers to ensure high availability.

  Additionally, the broker offers message filtering and consumption balancing.
  Message filtering permits only consuming a subset of messages,
  and consumption balancing can distribute messages among multiple consumer groups according to consumption speed.
  """

  defmodule State do
    @moduledoc false

    defstruct [
      :broker_name,
      :remote,
      :opaque,
      :version,
      :controlling_process,
      :task_supervisor,
      :task_ref
    ]

    @type t :: %__MODULE__{
            broker_name: String.t(),
            remote: pid(),
            opaque: non_neg_integer(),
            version: non_neg_integer(),
            controlling_process: pid(),
            task_supervisor: pid() | atom(),
            task_ref: %{}
          }
  end

  alias ExRocketmq.{
    Typespecs,
    Util,
    Transport,
    Remote,
    Remote.Packet,
    Remote.ExtFields,
    Protocol.Request,
    Protocol.Response
  }

  alias ExRocketmq.Models.{
    Heartbeat,
    SendMsg,
    PullMsg,
    QueryConsumerOffset,
    UpdateConsumerOffset,
    SearchOffset,
    GetMaxOffset,
    EndTransaction,
    ConsumerSendMsgBack,
    Lock
  }

  require Packet
  require Request
  require Response
  require Logger

  import ExRocketmq.Util.Assertion

  use GenServer

  # req
  @req_send_message Request.req_send_message()
  @req_send_batch_message Request.req_send_batch_message()
  @req_send_reply_message Request.req_send_reply_message()
  @req_query_consumer_offset Request.req_query_consumer_offset()
  @req_update_consumer_offset Request.req_update_consumer_offset()
  @req_search_offset_by_timestamp Request.req_search_offset_by_timestamp()
  @req_get_max_offset Request.req_get_max_offset()
  @req_pull_message Request.req_pull_message()
  @req_hearbeat Request.req_heartbeat()
  @req_consumer_send_msg_back Request.req_consumer_send_msg_back()
  @req_end_transaction Request.req_end_transaction()
  @req_get_consumer_list_by_group Request.req_get_consumer_list_by_group()
  @req_lock_batch_mq Request.req_lock_batch_mq()
  @req_unlock_batch_mq Request.req_unlock_batch_mq()

  # resp
  @resp_success Response.resp_success()
  @resp_error Response.resp_error()
  @resp_pull_not_found Response.resp_pull_not_found()
  @resp_pull_retry_immediately Response.resp_pull_retry_immediately()
  @resp_pull_offset_moved Response.resp_pull_offset_moved()
  @resp_topic_not_exist Response.resp_topic_not_exist()
  @resp_service_not_available Response.res_service_not_available()
  @resp_no_permission Response.res_no_permission()

  @broker_opts_schema [
    broker_name: [
      type: :string,
      required: true,
      doc: "The name of the broker"
    ],
    remote_opts: [
      type: :any,
      required: true,
      doc: "The remote options of the broker, see ExRocketmq.Remote for details"
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The options for the broker"
    ]
  ]

  @type namesrvs_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@broker_opts_schema))]

  @doc """
  In most cases, broker processes are started as subprocesses of producers or consumers.
  For ease of management, they are generally started using dynamic_supervision,
  while also registering the addr as a key in the registry,
  making it possible to locate a specific broker process according to its addr.

  If producer or consumer start broker process using `get_or_new_broker`,
  it must handle the `:notify` message sent by the broker process, like this:

  ```Elixir
  def handle_info({:notify, {pkt, broker_pid}} do
    # handle pkt
  end
  ```

  ## Examples

      iex> ExRocketmq.Broker.get_or_new_broker("broker0", "localhost:31001", RegistryName, DynamicSupervisorPid, nil)
      #PID<0.123.0>
  """
  @spec get_or_new_broker(
          Typespecs.broker_name(),
          String.t(),
          atom(),
          pid(),
          pid() | nil
        ) ::
          pid()
  def get_or_new_broker(broker_name, addr, registry, dynamic_supervisor, notify_pid \\ nil) do
    pid =
      Registry.lookup(registry, addr)
      |> case do
        [] ->
          Logger.debug("estabilish new broker connection: #{broker_name}, #{addr}}")

          {host, port} =
            addr
            |> Util.Network.parse_addr()

          broker_opts = [
            broker_name: broker_name,
            remote_opts: [transport: Transport.Tcp.new(host: host, port: port)],
            opts: [name: {:via, Registry, {registry, addr}}]
          ]

          {:ok, pid} =
            DynamicSupervisor.start_child(dynamic_supervisor, {__MODULE__, broker_opts})

          pid

        [{pid, _}] ->
          pid
      end

    notify_pid
    |> is_nil()
    |> unless do
      controlling_process(pid, notify_pid)
    end

    pid
  end

  @doc """
  start a broker process, we use `get_or_new_broker` in most cases, `start_link` is used for special cases if needed.

  ## Parameters
  #{NimbleOptions.docs(@broker_opts_schema)}
  """
  @spec start_link(namesrvs_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    {opts, init} =
      opts
      |> NimbleOptions.validate!(@broker_opts_schema)
      |> Keyword.pop(:opts)

    GenServer.start_link(__MODULE__, init, opts)
  end

  @spec stop(pid()) :: :ok
  def stop(broker), do: GenServer.stop(broker)

  defp controlling_process(broker, pid), do: GenServer.cast(broker, {:controlling_process, pid})

  @spec broker_name(pid()) :: String.t()
  def broker_name(broker), do: GenServer.call(broker, :broker_name)

  @doc """
  The heartbeat mechanism in RocketMQ is used to maintain the connection status between message producers and consumers and the message middleware.
  It detects whether the connection is normal by periodically sending heartbeat packets and promptly identifies and handles connection exceptions.

  Specifically, the heartbeat mechanism in RocketMQ has the following key points:

  - Sending heartbeat packets: Message producers and consumers regularly send heartbeat packets to the message middleware.
    These packets contain necessary information such as the ID and version number of the producer/consumer.
    This information helps the middleware identify the client sending the heartbeat.

  - Receiving heartbeat packets: The message middleware periodically receives heartbeat packets from producers and consumers.
    By receiving these packets, the middleware can determine if the client is online and check the connection status.

  - Heartbeat timeout detection: The message middleware sets a heartbeat timeout period to check the reception of heartbeat packets.
    If no heartbeat packet is received within the specified time, the middleware considers the client's connection to be abnormal and takes appropriate action.


  for producer, heartbeat packet carries producer data set, for consumer, heartbeat packet carries consumer data set.

  ## Examples

      iex> ExRocketmq.Broker.heartbeat(broker, %ExRocketmq.Models.Heartbeat{
      ...>   client_id: "producer_client_id",
      ...>   producer_data_set: [%ExRocketmq.Models.ProducerData{group: "group"}],
      ...> })

      iex> ExRocketmq.Broker.heartbeat(broker, %ExRocketmq.Models.Heartbeat{
      ...>   client_id: "consumer_client_id",
      ...>   consumer_data_set: [
      ...>     %ExRocketmq.Models.ConsumerData{
      ...>           group: "group"
      ...>           consume_type: "CONSUME_PASSIVELY",
      ...>           message_model: "Clustering",
      ...>           consume_from_where: "CONSUME_FROM_LAST_OFFSET",
      ...>           subscription_data_set: [
      ...>             %ExRocketmq.Models.Subscription{
      ...>               class_filter_mode: false,
      ...>               topic: "topic",
      ...>               sub_string: "*",
      ...>               tags_set: [],
      ...>               code_set: [],
      ...>               sub_version: 0,
      ...>               expression_type: "TAG"
      ...>             }
      ...>           ],
      ...>           unit_mode: false
      ...>     }
      ...>   ],
      ...> })

  """
  @spec heartbeat(pid(), Heartbeat.t()) :: :ok | Typespecs.error_t()
  def heartbeat(broker, heartbeat) do
    {:ok, body} = Heartbeat.encode(heartbeat)

    with {:ok, pkt} <- GenServer.call(broker, {:rpc, @req_hearbeat, body, %{}}),
         :ok <-
           do_assert(
             fn -> Packet.packet(pkt, :code) == @resp_success end,
             %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}
           ) do
      GenServer.cast(broker, {:set_version, Packet.packet(pkt, :version)})
    end
  end

  @spec send_msg_code(SendMsg.Request.t()) :: non_neg_integer()
  defp send_msg_code(%{reply: true}), do: @req_send_reply_message
  defp send_msg_code(%{batch: true}), do: @req_send_batch_message
  defp send_msg_code(_), do: @req_send_message

  @doc """
  send message to broker and wait for response.
  If you want to send message asynchronously, you can wrap this method in a `Task`,
  so we don't provide a `async_send_message` method.
  """
  @spec sync_send_message(pid(), SendMsg.Request.t(), binary()) ::
          {:ok, SendMsg.Response.t()} | Typespecs.error_t()
  def sync_send_message(broker, req, body) do
    ext_fields = ExtFields.to_map(req)
    code = send_msg_code(req)

    # retry 3 times
    with {:ok, pkt} <- send_with_retry(broker, code, body, ext_fields, 3),
         {:ok, resp} <- SendMsg.Response.from_pkt(pkt) do
      # part of queue information is not included in the response, so we need to add it manually
      q = %{resp.queue | topic: req.topic, broker_name: GenServer.call(broker, :broker_name)}
      {:ok, %{resp | queue: q}}
    end
  end

  @spec send_with_retry(
          pid(),
          Typespecs.req_code(),
          binary(),
          Typespecs.ext_fields(),
          non_neg_integer()
        ) ::
          {:ok, Packet.t()} | Typespecs.error_t()
  defp send_with_retry(_, _, _, _, 0), do: {:error, :retry_times_exceeded}

  defp send_with_retry(broker, code, body, ext_fields, retry_times) do
    case GenServer.call(broker, {:rpc, code, body, ext_fields}) do
      {:ok, pkt} ->
        Packet.packet(pkt, :code)
        |> Kernel.in([
          @resp_error,
          @resp_topic_not_exist,
          @resp_service_not_available,
          @resp_no_permission
        ])
        |> if do
          Logger.warning(
            "send message failed, code: #{Packet.packet(pkt, :code)}, remark: #{Packet.packet(pkt, :remark)},  retrying..."
          )

          send_with_retry(broker, code, body, ext_fields, retry_times - 1)
        else
          {:ok, pkt}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  send message to broker and don't wait for response, the request is totally the same as `sync_send_message`,
  broker actully will send response to client, but client ignore it.
  It's kind of waste of network resource, maybe we can improve it in the future.
  """
  @spec one_way_send_message(pid(), SendMsg.Request.t(), binary()) ::
          :ok
  def one_way_send_message(broker, req, body) do
    ext_fields = ExtFields.to_map(req)
    GenServer.cast(broker, {:one_way, send_msg_code(req), body, ext_fields})
  end

  @doc """
  pull message from broker.
  This request can also update queue's offset via `sys_flag` in `PullMsg.Request.t()`,
  so we can use it to update queue's offset while consuming message.
  """
  @spec pull_message(pid(), PullMsg.Request.t()) ::
          {:ok, PullMsg.Response.t()} | Typespecs.error_t()
  def pull_message(broker, req) do
    ext_fields = ExtFields.to_map(req)

    with {:ok, pkt} <-
           GenServer.call(broker, {:rpc, @req_pull_message, <<>>, ext_fields, 25_000}, 30_000),
         :ok <-
           do_assert(
             fn ->
               Packet.packet(pkt, :code) in [
                 @resp_success,
                 @resp_pull_not_found,
                 @resp_pull_retry_immediately,
                 @resp_pull_offset_moved
               ]
             end,
             %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}
           ) do
      PullMsg.Response.from_pkt(pkt)
    end
  end

  @doc """
  query offset of queue in remote broker.
  If the queue has never been consumed, a error code 22 will return
  """
  @spec query_consumer_offset(pid(), QueryConsumerOffset.t()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  def query_consumer_offset(broker, req) do
    ext_fields = ExtFields.to_map(req)

    with {:ok, pkt} <-
           GenServer.call(broker, {:rpc, @req_query_consumer_offset, <<>>, ext_fields}),
         :ok <-
           do_assert(
             fn -> Packet.packet(pkt, :code) == @resp_success end,
             %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}
           ) do
      ext_fields = Packet.packet(pkt, :ext_fields)
      {:ok, Map.get(ext_fields, "offset", "0") |> String.to_integer()}
    end
  end

  @doc """
  Set offset of queue in remote broker.
  In most cases, we set remote offset using `pull_message` request.
  """
  @spec update_consumer_offset(pid(), UpdateConsumerOffset.t()) ::
          :ok | Typespecs.error_t()
  def update_consumer_offset(broker, req) do
    ext_fields = ExtFields.to_map(req)
    GenServer.cast(broker, {:one_way, @req_update_consumer_offset, <<>>, ext_fields})
  end

  @doc """
  Search offset of queue by timestamp.
  This method is used when consumer want to consume message from a specific timestamp.
  """
  @spec search_offset_by_timestamp(pid(), SearchOffset.t()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  def search_offset_by_timestamp(broker, req) do
    ext_fields = ExtFields.to_map(req)

    with {:ok, pkt} <-
           GenServer.call(broker, {:rpc, @req_search_offset_by_timestamp, <<>>, ext_fields}),
         :ok <-
           do_assert(
             fn -> Packet.packet(pkt, :code) == @resp_success end,
             %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}
           ) do
      offset =
        pkt
        |> Packet.packet(:ext_fields)
        |> Map.get("offset", "0")
        |> String.to_integer()

      {:ok, offset}
    end
  end

  @doc """
  Get last offset of queue in remote broker.
  """
  @spec get_max_offset(pid(), GetMaxOffset.t()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  def get_max_offset(broker, req) do
    ext_fields = ExtFields.to_map(req)

    with {:ok, pkt} <-
           GenServer.call(broker, {:rpc, @req_get_max_offset, <<>>, ext_fields}),
         :ok <-
           do_assert(
             fn -> Packet.packet(pkt, :code) == @resp_success end,
             %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}
           ) do
      offset =
        pkt
        |> Packet.packet(:ext_fields)
        |> Map.get("offset", "0")
        |> String.to_integer()

      {:ok, offset}
    end
  end

  @doc """
  When one consumer do consume jobs and failed many times, it will send a `consumer_send_msg_back` request to broker,
  then broker will try to send the message to another consumer.
  """
  @spec consumer_send_msg_back(pid(), ConsumerSendMsgBack.t()) ::
          :ok | Typespecs.error_t()
  def consumer_send_msg_back(broker, req) do
    ext_fields = ExtFields.to_map(req)

    GenServer.call(broker, {:rpc, @req_consumer_send_msg_back, <<>>, ext_fields})
    |> case do
      {:ok, pkt} ->
        do_assert(
          fn -> Packet.packet(pkt, :code) == @resp_success end,
          %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}
        )

      {:error, reason} = error ->
        Logger.error("consumer_send_msg_back error: #{inspect(reason)}")
        error
    end
  end

  @doc """
  Get consumer list by group name.
  This method is used in rebalance process.
  """
  @spec get_consumer_list_by_group(pid(), String.t()) ::
          {:ok, list(String.t())} | Typespecs.error_t()
  def get_consumer_list_by_group(broker, group) do
    ext_field = %{"consumerGroup" => group}

    with {:ok, pkt} <-
           GenServer.call(
             broker,
             {:rpc, @req_get_consumer_list_by_group, <<>>, ext_field}
           ),
         :ok <-
           do_assert(
             fn -> Packet.packet(pkt, :code) == @resp_success end,
             %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}
           ),
         {:ok, %{"consumerIdList" => consumer_list}} <- Jason.decode(Packet.packet(pkt, :body)) do
      {:ok, consumer_list}
    end
  end

  @doc """
  In the process of sending transactional messages, after the producer completes the local task check,
  it will send an end_transaction request to the broker to mark the completion of the transaction for that message.

  The end_transaction request is a one-way request that may fail.
  If the request fails, the broker will periodically send a check request to the producer
  to trigger the client to resend the end_transaction request.
  """
  @spec end_transaction(pid(), EndTransaction.t()) :: :ok | Typespecs.error_t()
  def end_transaction(broker, req) do
    ext_fields = ExtFields.to_map(req)
    GenServer.cast(broker, {:one_way, @req_end_transaction, <<>>, ext_fields})
  end

  @doc """
  In the scenario of sequential message consumption,
  the consumer needs to exclusively acquire a certain message queue (mq).
  By invoking this method, the consumer can perform a remote broker lock operation on the mq.
  """
  @spec lock_batch_mq(pid(), Lock.Req.t()) :: {:ok, Lock.Resp.t()} | Typespecs.error_t()
  def lock_batch_mq(broker, req) do
    body = Lock.Req.encode(req)

    with {:ok, pkt} <- GenServer.call(broker, {:rpc, @req_lock_batch_mq, body, %{}}),
         :ok <-
           do_assert(
             fn -> Packet.packet(pkt, :code) == @resp_success end,
             %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}
           ) do
      Packet.packet(pkt, :body)
      |> Lock.Resp.decode()
      |> then(&{:ok, &1})
    end
  end

  @doc """
  The reverse operation of lock_batch_mq is typically invoked
  after rebalancing when the message queue (mq) is reassigned from one consumer to another.
  Prior to that, it is necessary to call this method to unlock the mq.
  """
  @spec unlock_batch_mq(pid(), Lock.Req.t()) :: :ok | Typespecs.error_t()
  def unlock_batch_mq(broker, req) do
    body = Lock.Req.encode(req)

    GenServer.call(broker, {:rpc, @req_unlock_batch_mq, body, %{}})
    |> case do
      {:ok, pkt} ->
        do_assert(
          fn -> Packet.packet(pkt, :code) == @resp_success end,
          %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}
        )

      {:error, reason} = error ->
        Logger.error("unlock_batch_mq error: #{inspect(reason)}")
        error
    end
  end

  @spec send_reply_pkt(pid(), Packet.t()) :: :ok
  def send_reply_pkt(broker, pkt) do
    GenServer.cast(broker, {:reply, pkt})
  end

  # ------- server ------

  def init(opts) do
    {:ok, remote_pid} = Remote.start_link(opts[:remote_opts])

    {:ok, task_supervisor} = Task.Supervisor.start_link()

    {:ok,
     %State{
       broker_name: opts[:broker_name],
       remote: remote_pid,
       opaque: 0,
       version: 0,
       controlling_process: nil,
       task_supervisor: task_supervisor,
       task_ref: %{}
     }, {:continue, :on_start}}
  end

  def terminate(reason, %State{remote: remote, task_supervisor: task_supervisor}) do
    Logger.warning("broker terminated with reason: #{inspect(reason)}")
    Remote.stop(remote)

    Task.Supervisor.children(task_supervisor)
    |> Enum.each(fn pid ->
      Task.Supervisor.terminate_child(task_supervisor, pid)
    end)
  end

  def handle_continue(:on_start, state) do
    Process.send_after(self(), :pop_notify, 1000)
    {:noreply, state}
  end

  def handle_call(
        {:rpc, code, body, ext_fields, rpc_timeout},
        from,
        %{
          remote: remote,
          opaque: opaque,
          task_supervisor: task_supervisor,
          task_ref: task_ref
        } =
          state
      ) do
    pkt =
      Packet.packet(
        code: code,
        opaque: opaque,
        ext_fields: ext_fields,
        body: body
      )

    # There is a possibility that the rpc may take a long time, and we do not want it to block the GenServer.
    # Therefore, we utilize Task.Supervisor.async_nolink to initiate an asynchronous task for invoking the rpc,
    # and handle the callback of rpc task results within the GenServer.
    task =
      Task.Supervisor.async_nolink(task_supervisor, fn ->
        Remote.rpc(remote, pkt, rpc_timeout)
      end)

    {:noreply, %{state | opaque: opaque + 1, task_ref: Map.put(task_ref, task.ref, from)}}
  end

  def handle_call(
        {:rpc, code, body, ext_fields},
        from,
        state
      ) do
    handle_call({:rpc, code, body, ext_fields, 5000}, from, state)
  end

  def handle_call(:broker_name, _, %{broker_name: broker_name} = state),
    do: {:reply, broker_name, state}

  def handle_cast({:controlling_process, pid}, state) do
    {:noreply, %{state | controlling_process: pid}}
  end

  def handle_cast(
        {:set_version, version},
        %{version: old_version, broker_name: broker_name} = state
      ) do
    if version != old_version do
      Logger.info("Broker #{broker_name} version changed from #{old_version} to #{version}")
      {:noreply, %{state | version: version}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:one_way, code, body, ext_fields}, %{remote: remote} = state) do
    pkt =
      Packet.packet(
        code: code,
        ext_fields: ext_fields,
        body: body
      )

    remote
    |> Remote.one_way(pkt)

    {:noreply, state}
  end

  def handle_cast({:reply, pkt}, %{remote: remote} = state) do
    Remote.one_way(remote, pkt)
    {:noreply, state}
  end

  # The pop_notify function is called periodically to pull messages sent by the broker from the remote.
  # If the broker process has already bound a certain consumer or producer,
  # we will send the messages to them in the format '{:notify, {msg, self()}}'.
  def handle_info(:pop_notify, %{controlling_process: nil} = state) do
    # Logger.warning("controlling process is nil, broker will not notify")
    Process.send_after(self(), :pop_notify, 5000)
    {:noreply, state}
  end

  def handle_info(:pop_notify, %{remote: remote, controlling_process: cpid} = state) do
    case Remote.pop_notify(remote) do
      :empty ->
        nil

      pkt ->
        send(cpid, {:notify, {pkt, self()}})
    end

    Process.send_after(self(), :pop_notify, 1000)
    {:noreply, state}
  end

  def handle_info(:timeout, state) do
    Logger.warning("broker timeout")
    {:noreply, state}
  end

  # The rpc task completed successfully
  def handle_info({ref, rpc_result}, %{task_ref: task_ref} = state) do
    # We don't care about the DOWN message now, so let's demonitor and flush it
    Process.demonitor(ref, [:flush])

    Map.pop(task_ref, ref)
    |> case do
      {nil, _} ->
        Logger.warning("no task ref found: #{inspect(ref)}")
        {:noreply, state}

      {from, task_ref} ->
        GenServer.reply(from, rpc_result)
        {:noreply, %{state | task_ref: task_ref}}
    end
  end

  # The rpc task failed
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task_ref: task_ref} = state) do
    Map.pop(task_ref, ref)
    |> case do
      {nil, _} ->
        Logger.warning("no task ref found: #{inspect(ref)}")
        {:noreply, state}

      {from, task_ref} ->
        GenServer.reply(from, {:error, reason})
        {:noreply, %{state | task_ref: task_ref}}
    end
  end

  def handle_info(cmd, state) do
    Logger.warning("broker recv unknown cmd: #{inspect(cmd)}")
    {:noreply, state}
  end
end
