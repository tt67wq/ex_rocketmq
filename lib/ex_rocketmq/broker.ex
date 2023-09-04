defmodule ExRocketmq.Broker do
  @moduledoc """
  RocketMQ Broker Client
  """

  defmodule State do
    @moduledoc false

    defstruct [
      :broker_name,
      :remote,
      :opaque,
      :version,
      :controlling_process
    ]

    @type t :: %__MODULE__{
            broker_name: String.t(),
            remote: pid(),
            opaque: non_neg_integer(),
            version: non_neg_integer(),
            controlling_process: pid()
          }
  end

  alias ExRocketmq.{
    Typespecs,
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
    Lock
  }

  require Packet
  require Request
  require Response
  require Logger

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

  @spec controlling_process(pid(), pid()) :: :ok
  def controlling_process(broker, pid), do: GenServer.cast(broker, {:controlling_process, pid})

  @spec broker_name(pid()) :: String.t()
  def broker_name(broker), do: GenServer.call(broker, :broker_name)

  @spec heartbeat(pid(), Heartbeat.t()) :: :ok | Typespecs.error_t()
  def heartbeat(broker, heartbeat) do
    with {:ok, body} <- Heartbeat.encode(heartbeat),
         {:ok, resp_msg} <- GenServer.call(broker, {:rpc, @req_hearbeat, body, %{}}) do
      resp_msg
      |> Packet.packet(:code)
      |> case do
        @resp_success ->
          GenServer.cast(broker, {:set_version, resp_msg |> Packet.packet(:version)})
          :ok

        code ->
          remark = resp_msg |> Packet.packet(:remark)
          {:error, %{code: code, remark: remark}}
      end
    end
  end

  @spec send_msg_code(SendMsg.Request.t()) :: non_neg_integer()
  defp send_msg_code(%{reply: true}), do: @req_send_reply_message
  defp send_msg_code(%{batch: true}), do: @req_send_batch_message
  defp send_msg_code(_), do: @req_send_message

  @spec sync_send_message(pid(), SendMsg.Request.t(), binary()) ::
          {:ok, SendMsg.Response.t()} | Typespecs.error_t()
  def sync_send_message(broker, req, body) do
    with ext_fields <- ExtFields.to_map(req),
         code <- send_msg_code(req),
         {:ok, pkt} <- send_with_retry(broker, code, body, ext_fields, 3),
         {:ok, resp} <- SendMsg.Response.from_pkt(pkt) do
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
    case GenServer.call(broker, {:rpc, code, body, ext_fields}, 5_000) do
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

  @spec one_way_send_message(pid(), SendMsg.Request.t(), binary()) ::
          :ok
  def one_way_send_message(broker, req, body) do
    with ext_fields <- ExtFields.to_map(req),
         code <- send_msg_code(req) do
      GenServer.cast(broker, {:one_way, code, body, ext_fields})
    end
  end

  @spec pull_message(pid(), PullMsg.Request.t()) ::
          {:ok, PullMsg.Response.t()} | Typespecs.error_t()
  def pull_message(broker, req) do
    with ext_fields <- ExtFields.to_map(req),
         {:ok, pkt} <- GenServer.call(broker, {:rpc, @req_pull_message, <<>>, ext_fields}),
         {:ok, resp} <- PullMsg.Response.from_pkt(pkt) do
      {:ok, resp}
    end
  end

  @spec query_consumer_offset(pid(), QueryConsumerOffset.t()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  def query_consumer_offset(broker, req) do
    with ext_fields <- ExtFields.to_map(req),
         {:ok, pkt} <-
           GenServer.call(broker, {:rpc, @req_query_consumer_offset, <<>>, ext_fields}),
         ext_fields <- Packet.packet(pkt, :ext_fields) do
      {:ok, Map.get(ext_fields, "offset", "0") |> String.to_integer()}
    end
  end

  @spec update_consumer_offset(pid(), UpdateConsumerOffset.t()) ::
          :ok | Typespecs.error_t()
  def update_consumer_offset(broker, req) do
    with ext_fields <- ExtFields.to_map(req) do
      GenServer.cast(broker, {:one_way, @req_update_consumer_offset, <<>>, ext_fields})
    end
  end

  @spec search_offset_by_timestamp(pid(), SearchOffset.t()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  def search_offset_by_timestamp(broker, req) do
    with ext_fields <- ExtFields.to_map(req),
         {:ok, pkt} <-
           GenServer.call(broker, {:rpc, @req_search_offset_by_timestamp, <<>>, ext_fields}),
         ext_fields <- Packet.packet(pkt, :ext_fields) do
      {:ok, Map.get(ext_fields, "offset", "0") |> String.to_integer()}
    end
  end

  @spec get_max_offset(pid(), GetMaxOffset.t()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  def get_max_offset(broker, req) do
    with ext_fields <- ExtFields.to_map(req),
         {:ok, pkt} <-
           GenServer.call(broker, {:rpc, @req_get_max_offset, <<>>, ext_fields}),
         ext_fields <- Packet.packet(pkt, :ext_fields) do
      {:ok, Map.get(ext_fields, "offset", "0") |> String.to_integer()}
    end
  end

  @spec consumer_send_msg_back(pid(), ConsumerSendMsgBack.t()) ::
          :ok | Typespecs.error_t()
  def consumer_send_msg_back(broker, req) do
    with ext_fields <- ExtFields.to_map(req),
         {:ok, pkt} <-
           GenServer.call(broker, {:rpc, @req_consumer_send_msg_back, <<>>, ext_fields}) do
      case Packet.packet(pkt, :code) do
        @resp_success ->
          :ok

        code ->
          remark = Packet.packet(pkt, :remark)
          {:error, %{code: code, remark: remark}}
      end
    end
  end

  @spec get_consumer_list_by_group(pid(), String.t()) ::
          {:ok, list(String.t())} | Typespecs.error_t()
  def get_consumer_list_by_group(broker, group) do
    with ext_field <- %{"consumerGroup" => group},
         {:ok, pkt} <-
           GenServer.call(broker, {:rpc, @req_get_consumer_list_by_group, <<>>, ext_field}),
         {:ok, %{"consumerIdList" => consumer_list}} <- Jason.decode(Packet.packet(pkt, :body)) do
      {:ok, consumer_list}
    end
  end

  @spec end_transaction(pid(), EndTransaction.t()) :: :ok | Typespecs.error_t()
  def end_transaction(broker, req) do
    with ext_fields <- ExtFields.to_map(req) do
      GenServer.cast(broker, {:one_way, @req_end_transaction, <<>>, ext_fields})
    end
  end

  @spec lock_batch_mq(pid(), Lock.Req.t()) :: {:ok, Lock.Resp.t()} | Typespecs.error_t()
  def lock_batch_mq(broker, req) do
    with body <- Lock.Req.encode(req),
         {:ok, pkt} <- GenServer.call(broker, {:rpc, @req_lock_batch_mq, body, %{}}),
         body <- Packet.packet(pkt, :body) do
      {:ok, Lock.Resp.decode(body)}
    end
  end

  @spec unlock_batch_mq(pid(), Lock.Req.t()) :: :ok | Typespecs.error_t()
  def unlock_batch_mq(broker, req) do
    with body <- Lock.Req.encode(req),
         {:ok, pkt} <- GenServer.call(broker, {:rpc, @req_unlock_batch_mq, body, %{}}) do
      case Packet.packet(pkt, :code) do
        @resp_success ->
          :ok

        code ->
          remark = Packet.packet(pkt, :remark)
          {:error, %{code: code, remark: remark}}
      end
    end
  end

  # ------- server ------

  def init(opts) do
    {:ok, remote_pid} = Remote.start_link(opts[:remote_opts])

    {:ok,
     %State{
       broker_name: opts[:broker_name],
       remote: remote_pid,
       opaque: 0,
       version: 0,
       controlling_process: nil
     }, {:continue, :on_start}}
  end

  def handle_continue(:on_start, state) do
    Process.send_after(self(), :pop_notify, 1000)
    {:noreply, state}
  end

  def handle_call(
        {:rpc, code, body, ext_fields},
        _from,
        %{remote: remote, opaque: opaque} = state
      ) do
    pkt =
      Packet.packet(
        code: code,
        opaque: opaque,
        ext_fields: ext_fields,
        body: body
      )

    reply =
      remote
      |> Remote.rpc(pkt)

    {:reply, reply, %{state | opaque: opaque + 1}}
  end

  def handle_call(:broker_name, _, %{broker_name: broker_name} = state),
    do: {:reply, broker_name, state}

  def handle_cast({:controlling_process, pid}, state) do
    {:noreply, %{state | controlling_process: pid}}
  end

  def handle_cast({:set_version, version}, %{version: old_version} = state) do
    if version != old_version do
      Logger.info("Broker version changed from #{old_version} to #{version}")
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

  def handle_info(:pop_notify, %{controlling_process: nil} = state) do
    Logger.warning("controlling process is nil, broker will not notify")
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

  def terminate(reason, %{remote: remote}) do
    Logger.warning("broker terminated with reason: #{inspect(reason)}")
    Remote.stop(remote)
  end
end
