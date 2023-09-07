defmodule ExRocketmq.PushConsumer do
  @moduledoc """
  RocketMQ push_consumer
  """

  defmodule State do
    @moduledoc false

    alias ExRocketmq.{Typespecs, Models}

    @type t :: %__MODULE__{
            client_id: String.t(),
            group_name: Typespecs.group_name(),
            retry_topic: Typespecs.topic(),
            namespace: Typespecs.namespace(),
            model: Typespecs.consumer_model(),
            namesrvs: pid() | atom(),
            dynamic_supervisor: pid() | atom(),
            registry: pid(),
            subscriptions: %{Typespecs.topic() => Models.Subscription.t()},
            processor: ExRocketmq.Consumer.Processor.t(),
            consume_info_map: %{
              Typespecs.topic() => {list(Models.BrokerData.t()), list(Models.MessageQueue.t())}
            },
            consume_from_where: Typespecs.consume_from_where(),
            balance_strategy: ExRocketmq.Consumer.BalanceStrategy.t()
          }

    defstruct client_id: "",
              group_name: "",
              namespace: "",
              retry_topic: "",
              namesrvs: nil,
              model: :cluster,
              dynamic_supervisor: nil,
              registry: nil,
              subscriptions: %{},
              processor: nil,
              # topic => route_info
              consume_info_map: %{},
              # topic => assigned queues
              process_info_map: %{},
              consume_from_where: :last_offset,
              balance_strategy: nil
  end

  use GenServer

  alias ExRocketmq.Models.BrokerData

  alias ExRocketmq.{
    Util,
    Typespecs,
    Namesrvs,
    Broker,
    Transport,
    Consumer.BalanceStrategy
  }

  alias ExRocketmq.Models.{
    Subscription,
    MsgSelector,
    QueueData,
    Heartbeat,
    ConsumerData
  }

  require Logger

  @consumer_opts_schema [
    consumer_group: [
      type: :string,
      doc: "Consumer group name",
      required: true
    ],
    namespace: [
      type: :string,
      doc: "The namespace of the consumer group",
      default: ""
    ],
    namesrvs: [
      type: {:or, [:pid, :atom]},
      required: true,
      doc: "The namesrvs process"
    ],
    model: [
      type: {:in, [:cluster, :broadcast]},
      doc: "Consumer model, :cluster or :broadcast",
      default: :cluster
    ],
    consume_orderly: [
      type: :boolean,
      doc: "Whether to consume orderly",
      default: false
    ],
    consume_from_where: [
      type: {:in, [:last_offset, :first_offset, :timestamp]},
      doc: "Where to start consuming, :last_offset or :first_offset",
      default: :last_offset
    ],
    processor: [
      type: :any,
      doc: "The processor implemention of the consumer, See ExRocketmq.Consumer.Processor",
      required: true
    ],
    balance_strategy: [
      type: :any,
      doc: "The implemention of ExRocketmq.Consumer.BalanceStrategy",
      default: ExRocketmq.Consumer.BalanceStrategy.Average.new()
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The opts of the comsumer's GenServer"
    ]
  ]

  @type consumer_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@consumer_opts_schema))]
  @type consume_info_t :: %{
          Typespecs.topic() => {list(Models.BrokerData.t()), list(Models.MessageQueue.t())}
        }

  @spec start_link(consumer_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    opts =
      opts
      |> NimbleOptions.validate!(@consumer_opts_schema)

    {init, opts} = Keyword.pop(opts, :opts)
    GenServer.start_link(__MODULE__, init, opts)
  end

  @spec stop(pid() | atom()) :: :ok
  def stop(consumer), do: GenServer.stop(consumer)

  @spec subscribe(pid() | atom(), Typespecs.topic(), ExRocketmq.Models.MsgSelector.t()) :: :ok
  def subscribe(consumer, topic, msg_selector) do
    GenServer.call(consumer, {:subscribe, topic, msg_selector})
  end

  # ------- server callbacks -------

  def init(opts) do
    registry = :"Registry.#{Util.Random.generate_id("C")}"

    {:ok, _} =
      Registry.start_link(
        keys: :unique,
        name: registry
      )

    {:ok,
     %State{
       client_id: Util.ClientId.get(),
       group_name: with_namespace(opts[:consumer_group], opts[:namespace]),
       retry_topic: retry_topic(opts[:consumer_group]),
       model: opts[:model],
       namesrvs: opts[:namesrvs],
       dynamic_supervisor: opts[:dynamic_supervisor],
       registry: registry,
       subscriptions: %{},
       consume_info_map: %{},
       consume_from_where: opts[:consume_from_where],
       balance_strategy: opts[:balance_strategy]
     }, {:continue, :on_start}}
  end

  def terminate(reason, _state) do
    Logger.info("consumer terminate, reason: #{inspect(reason)}")
  end

  def handle_continue(
        :on_start,
        %{model: :cluster, retry_topic: topic, subscriptions: subs} = state
      ) do
    sub = %Subscription{
      class_filter_mode: false,
      topic: topic,
      sub_string: "*",
      sub_version: System.system_time(:nanosecond),
      expression_type: "TAG"
    }

    Process.send_after(self(), :update_route_info, 1000)
    {:noreply, %State{state | subscriptions: Map.put(subs, topic, sub)}}
  end

  def handle_continue(:on_start, state) do
    Process.send_after(self(), :update_route_info, 1000)
    {:noreply, state}
  end

  def handle_call(
        {:subscribe, topic, msg_selector},
        _from,
        %State{namespace: namespace, subscriptions: subs} = state
      ) do
    with topic <- with_namespace(topic, namespace),
         sub <- %Subscription{
           class_filter_mode: false,
           topic: topic,
           sub_string: msg_selector.expression,
           tags_set: MsgSelector.tags(msg_selector),
           code_set: MsgSelector.codes(msg_selector),
           sub_version: System.system_time(:nanosecond),
           expression_type: (msg_selector.type == :tag && "TAG") || "SQL92"
         } do
      {:reply, :ok, %State{state | subscriptions: Map.put(subs, topic, sub)}}
    end
  end

  def handle_info(:update_route_info, %State{subscriptions: subs, namesrvs: namesrvs} = state) do
    new_subs =
      subs
      |> Map.keys()
      |> Enum.into(%{}, fn topic ->
        case fetch_consume_info(namesrvs, topic) do
          {:ok, {broker_datas, mqs}} ->
            {topic, {broker_datas, mqs}}

          {:error, _} ->
            {topic, nil}
        end
      end)

    Process.send_after(self(), :update_route_info, 30_000)

    {:noreply, %State{state | consume_info_map: new_subs}}
  end

  def handle_info(
        :heartbeat,
        %State{
          client_id: cid,
          group_name: group_name,
          registry: registry,
          consume_from_where: cfw
        } = state
      ) do
    heartbeat_data = %Heartbeat{
      client_id: cid,
      consumer_data_set: [
        %ConsumerData{
          group: group_name,
          consume_type: "CONSUME_PASSIVELY",
          message_model: (state.model == :cluster && "Clustering") || "BroadCasting",
          consume_from_where: consume_from_where_str(cfw),
          subscription_data_set: state.subscriptions |> Map.values(),
          unit_mode: false
        }
      ]
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

  def handle_info(
        :rebalance,
        %State{
          client_id: client_id,
          group_name: group_name,
          model: :cluster,
          consume_info_map: cmap,
          registry: registry,
          balance_strategy: strategy
        } = state
      ) do
    cmap
    |> Map.to_list()
    |> Enum.map(fn {topic, {broker_datas, mqs}} ->
      bd = Enum.random(broker_datas)
      broker = get_or_new_broker(bd.broker_name, BrokerData.slave_addr(bd), registry)
      {:ok, cids} = Broker.get_consumer_list_by_group(broker, group_name)
      {topic, cids, Enum.sort_by(mqs, & &1.queue_id)}
      {:ok, allocated_mqs} = BalanceStrategy.allocate(strategy, client_id, mqs, cids)
    end)

    {:noreply, state}
  end

  # ---- private functions ----

  @spec with_namespace(Typespecs.group_name() | Typespecs.topic(), Typespecs.namespace()) ::
          Typespecs.group_name()
  defp with_namespace(name, ""), do: name
  defp with_namespace(name, namespace), do: name <> "%" <> namespace

  @spec retry_topic(Typespecs.group_name()) :: Typespecs.topic()
  defp retry_topic(group_name), do: "%RETRY%" <> group_name

  defp fetch_consume_info(namesrvs, topic) do
    Namesrvs.query_topic_route_info(namesrvs, topic)
    |> case do
      {:ok, %{broker_datas: broker_datas, queue_datas: queue_datas}} ->
        mqs =
          queue_datas
          |> Enum.map(&QueueData.to_consume_queues(&1, topic))
          |> List.flatten()

        {:ok, {broker_datas, mqs}}

      {:error, reason} = error ->
        Logger.error("query topic route info error: #{inspect(reason)}")
        error
    end
  end

  @spec consume_from_where_str(Typespecs.consume_from_where()) :: String.t()
  defp consume_from_where_str(:last_offset), do: "CONSUME_FROM_LAST_OFFSET"
  defp consume_from_where_str(:first_offset), do: "CONSUME_FROM_FIRST_OFFSET"
  defp consume_from_where_str(:timestamp), do: "CONSUME_FROM_TIMESTAMP"
  defp consume_from_where_str(_), do: "UNKNOWN"

  @spec all_broker_pids(atom()) :: list(pid())
  defp all_broker_pids(registry) do
    Registry.select(registry, [{{:"$1", :"$2", :"$3"}, [], [:"$2"]}])
  end

  @spec get_or_new_broker(String.t(), String.t(), atom()) :: pid()
  defp get_or_new_broker(broker_name, addr, registry) do
    Registry.lookup(registry, addr)
    |> case do
      [] ->
        {host, port} =
          addr
          |> Util.Network.parse_addr()

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
end
