defmodule ExRocketmq.Tracer do
  @moduledoc """
  Trace data emiter
  """

  defmodule State do
    @moduledoc false

    alias ExRocketmq.{Typespecs, Models}

    defstruct client_id: "",
              namesrvs: nil,
              broker_datas: [],
              message_queues: [],
              supervisor: nil

    @type t :: %__MODULE__{
            client_id: String.t(),
            broker_datas: list(Models.BrokerData.t()),
            message_queues: list(Models.MessageQueue.t()),
            supervisor: pid() | atom()
          }
  end

  use GenServer

  alias ExRocketmq.{Typespecs, Util, Namesrvs, Broker, Exception}

  alias ExRocketmq.Models.{
    QueueData,
    Trace,
    TraceItem,
    Message,
    BrokerData,
    MessageQueue,
    SendMsg
  }

  require Logger

  @trace_opts_schema [
    namesrvs: [
      type: {:or, [:pid, :atom]},
      required: true,
      doc: "The namesrvs process"
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The opts of the trace's GenServer"
    ]
  ]

  @trace_topic "RMQ_SYS_TRACE_TOPIC"
  @trace_group "_INNER_TRACE_PRODUCER"

  @type trace_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@trace_opts_schema))]

  @spec start_link(trace_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    {opts, init} =
      opts
      |> NimbleOptions.validate!(@trace_opts_schema)
      |> Keyword.pop(:opts)

    GenServer.start_link(__MODULE__, init, opts)
  end

  @spec stop(pid() | atom()) :: :ok
  def stop(pid) do
    GenServer.stop(pid)
  end

  @spec send_trace(pid() | atom(), [Trace.t()]) :: :ok
  def send_trace(pid, traces) do
    GenServer.cast(pid, {:send_trace, traces})
  end

  # ----------- server callbacks -----------
  def init(opts) do
    cid = Util.ClientId.get("Trace")

    {:ok, supervisor} =
      ExRocketmq.Trace.Supervisor.start_link(
        opts: [
          cid: cid
        ]
      )

    {:ok,
     %State{
       client_id: cid,
       namesrvs: opts[:namesrvs],
       broker_datas: [],
       message_queues: [],
       supervisor: supervisor
     }, {:continue, :on_start}}
  end

  def terminate(reason, %State{
        client_id: cid,
        supervisor: supervisor
      }) do
    Logger.info("producer terminate, reason: #{inspect(reason)}")

    # stop all existing tasks
    task_supervisor = :"Task.Supervisor.#{cid}"

    task_supervisor
    |> Task.Supervisor.children()
    |> Enum.each(fn pid ->
      Task.Supervisor.terminate_child(task_supervisor, pid)
    end)

    # stop all broker
    dynamic_supervisor = :"DynamicSupervisor.#{cid}"

    dynamic_supervisor
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Enum.each(fn pid ->
      DynamicSupervisor.terminate_child(dynamic_supervisor, pid)
    end)

    # stop buffer
    Util.Buffer.stop(:"Buffer.#{cid}")

    Supervisor.stop(supervisor, reason)
  end

  def handle_continue(:on_start, state) do
    Process.send_after(self(), :update_route_info, 1_000)
    Process.send_after(self(), :flush, 1_000)

    {:noreply, state}
  end

  def handle_cast(
        {:send_trace, traces},
        %State{
          client_id: cid
        } = state
      ) do
    uniq_id_provider = :"UniqId.#{cid}"

    traces =
      traces
      |> Enum.map(fn x ->
        %Trace{
          x
          | request_id: Util.UniqId.get_uniq_id(uniq_id_provider)
        }
      end)

    buffer = :"Buffer.#{cid}"

    Util.Buffer.put(buffer, traces)
    |> case do
      :ok ->
        :ok

      {:error, :full} ->
        Logger.error("trace buffer is full, discard trace")
    end

    {:noreply, state}
  end

  def handle_info(:update_route_info, %State{namesrvs: namesrvs} = state) do
    state =
      fetch_publish_info(@trace_topic, namesrvs)
      |> case do
        {:ok, {broker_datas, mqs}} ->
          %{state | broker_datas: broker_datas, message_queues: mqs}

        {:error, _} ->
          state
      end

    Process.send_after(self(), :update_route_info, 30_000)

    {:noreply, state}
  end

  def handle_info(
        :flush,
        %State{
          client_id: cid
        } = state
      ) do
    :"Buffer.#{cid}"
    |> Util.Buffer.take()
    |> emit_trace(state)
    |> case do
      0 -> Process.send_after(self(), :flush, 1_000)
      _ -> Process.send_after(self(), :flush, 0)
    end

    {:noreply, state}
  end

  # ------ private functions ------

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

  @spec emit_trace(list(Trace.t()), State.t()) :: non_neg_integer()
  defp emit_trace([], _state), do: 0

  defp emit_trace(traces, %State{
         client_id: cid,
         message_queues: message_queues,
         broker_datas: broker_datas
       }) do
    keys =
      traces
      |> Enum.map(&trace_keys(&1))
      |> List.flatten()
      |> Enum.uniq()

    body =
      traces
      |> Enum.map_join(fn trace ->
        # Util.Debug.debug(trace)
        Trace.encode(trace)
      end)

    msg = %Message{
      topic: @trace_topic,
      body: body,
      properties: %{
        "KEYS" => Enum.join(keys, " ")
      }
    }

    mq = message_queues |> Enum.random()

    bd = get_broker_data!(broker_datas, mq)

    broker =
      Broker.get_or_new_broker(
        mq.broker_name,
        BrokerData.master_addr(bd),
        :"Registry.#{cid}",
        :"DynamicSupervisor.#{cid}"
      )

    req = %SendMsg.Request{
      producer_group: @trace_group,
      topic: @trace_topic,
      queue_id: mq.queue_id,
      born_timestamp: System.system_time(:millisecond),
      flag: msg.flag,
      properties: Message.encode_properties(msg)
    }

    Broker.one_way_send_message(broker, req, msg.body)

    Enum.count(traces)
  end

  @spec trace_keys(Trace.t()) :: list(String.t())
  defp trace_keys(%Trace{items: items}) do
    items
    |> Enum.reduce([], fn
      %TraceItem{msg_id: msg_id, keys: ""}, acc ->
        [msg_id | acc]

      %TraceItem{msg_id: msg_id, keys: keys}, acc ->
        [keys, msg_id | acc]
    end)
    |> Enum.reverse()
  end

  @spec get_broker_data!(list(BrokerData.t()), MessageQueue.t()) :: BrokerData.t()
  defp get_broker_data!(broker_datas, mq) do
    broker_datas
    |> Enum.find(&(&1.broker_name == mq.broker_name))
    |> case do
      nil ->
        raise Exception.new("broker data not found, broker_name: #{mq.broker_name}")

      bd ->
        bd
    end
  end
end
