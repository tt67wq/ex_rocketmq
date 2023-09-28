defmodule ExRocketmq.Consumer do
  @moduledoc """
  RocketMQ consumer
  """

  defmodule State do
    @moduledoc false

    alias ExRocketmq.{Typespecs, Models}

    @type t :: %__MODULE__{
            client_id: String.t(),
            namesrvs: pid() | atom(),
            broker_dynamic_supervisor: pid() | atom(),
            task_supervisor: pid() | atom(),
            registry: pid(),
            processor: ExRocketmq.Consumer.Processor.t(),
            consume_info_map: %{
              Typespecs.topic() => {
                Models.Subscription.t(),
                list(Models.BrokerData.t()),
                list(Models.MessageQueue.t()),
                %{Models.MessageQueue.t() => pid()}
              }
            },
            consume_opts: %{
              group_name: Typespecs.group_name(),
              retry_topic: Typespecs.topic(),
              namespace: Typespecs.namespace(),
              model: Typespecs.consumer_model(),
              consume_orderly: boolean(),
              consume_from_where: Typespecs.consume_from_where(),
              consume_timestamp: non_neg_integer(),
              balance_strategy: ExRocketmq.Consumer.BalanceStrategy.t(),
              post_subscription_when_pull: boolean(),
              pull_batch_size: non_neg_integer(),
              consume_batch_size: non_neg_integer()
            }
          }

    defstruct client_id: "",
              namesrvs: nil,
              broker_dynamic_supervisor: nil,
              task_supervisor: nil,
              registry: nil,
              consume_info_map: %{},
              processor: nil,
              consume_opts: %{
                group_name: "",
                retry_topic: "",
                namespace: "",
                model: :cluster,
                consume_orderly: false,
                consume_from_where: :last_offset,
                balance_strategy: nil,
                consume_timestamp: 0,
                post_subscription_when_pull: false,
                pull_batch_size: 32,
                consume_batch_size: 16
              }
  end

  use GenServer

  alias ExRocketmq.{
    Util,
    Typespecs,
    Namesrvs,
    Broker,
    Consumer.BalanceStrategy,
    Protocol.Request,
    Protocol.PullStatus,
    Remote.Packet,
    InnerConsumer
  }

  alias ExRocketmq.Models.{
    Subscription,
    MsgSelector,
    QueueData,
    Heartbeat,
    ConsumerData,
    BrokerData,
    MessageQueue,
    ConsumeState
  }

  require Logger
  require Request
  require Packet
  require PullStatus

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
    subscriptions: [
      type: {:map, :string, :any},
      doc:
        "The subscriptions of the consumer, such as %{\"SomeTopic\" => %ExRocketmq.Models.MsgSelector{}}",
      default: %{}
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
    balance_strategy: [
      type: :any,
      doc: "The implemention of ExRocketmq.Consumer.BalanceStrategy",
      default: ExRocketmq.Consumer.BalanceStrategy.Average.new()
    ],
    processor: [
      type: :any,
      doc: "Message consume processor, must implement ExRocketmq.Consumer.Processor",
      required: true
    ],
    consume_timestamp: [
      type: :non_neg_integer,
      doc: "The timestamp(ms) to consume from, only used when consume_from_where is :timestamp",
      default: 0
    ],
    post_subscription_when_pull: [
      type: :boolean,
      doc: "Whether to post subscription when pull message",
      default: false
    ],
    pull_batch_size: [
      type: :non_neg_integer,
      doc: "The batch size to pull message",
      default: 32
    ],
    consume_batch_size: [
      type: :non_neg_integer,
      doc: "The batch size to consume message",
      default: 16
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The opts of the comsumer's GenServer"
    ]
  ]

  @req_notify_consumer_ids_changed Request.req_notify_consumer_ids_changed()

  @type consumer_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@consumer_opts_schema))]

  @spec start_link(consumer_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    opts =
      opts
      |> NimbleOptions.validate!(@consumer_opts_schema)

    {opts, init} = Keyword.pop(opts, :opts)
    GenServer.start_link(__MODULE__, init, opts)
  end

  @spec stop(pid() | atom()) :: :ok
  def stop(consumer), do: GenServer.stop(consumer)

  @spec subscribe(pid() | atom(), Typespecs.topic(), MsgSelector.t()) :: :ok
  def subscribe(consumer, topic, msg_selector \\ %MsgSelector{}) do
    GenServer.call(consumer, {:subscribe, topic, msg_selector})
  end

  @spec unsubscribe(pid() | atom(), Typespecs.topic()) :: :ok
  def unsubscribe(consumer, topic) do
    GenServer.call(consumer, {:unsubscribe, topic})
  end

  # ------- server callbacks -------

  def init(opts) do
    registry = :"Registry.#{Util.Random.generate_id("C")}"

    {:ok, _} =
      Registry.start_link(
        keys: :unique,
        name: registry
      )

    {:ok, task_supervisor} = Task.Supervisor.start_link()
    {:ok, broker_dynamic_supervisor} = DynamicSupervisor.start_link([])

    # prepare subscriptions
    cmap =
      opts[:subscriptions]
      |> Enum.into(%{}, fn {topic, msg_selector} ->
        {topic, {message_selector_to_subscription(topic, msg_selector), [], [], %{}}}
      end)

    {:ok,
     %State{
       client_id: Util.ClientId.get(),
       namesrvs: opts[:namesrvs],
       broker_dynamic_supervisor: broker_dynamic_supervisor,
       task_supervisor: task_supervisor,
       registry: registry,
       consume_info_map: cmap,
       processor: opts[:processor],
       consume_opts: %{
         group_name: with_namespace(opts[:consumer_group], opts[:namespace]),
         retry_topic: retry_topic(opts[:consumer_group]),
         model: opts[:model],
         namespace: opts[:namespace],
         consume_from_where: opts[:consume_from_where],
         consume_timestamp: opts[:consume_timestamp],
         consume_orderly: opts[:consume_orderly],
         balance_strategy: opts[:balance_strategy],
         post_subscription_when_pull: opts[:post_subscription_when_pull],
         pull_batch_size: opts[:pull_batch_size],
         consume_batch_size: opts[:consume_batch_size]
       }
     }, {:continue, :on_start}}
  end

  def terminate(reason, %State{broker_dynamic_supervisor: ds}) do
    Logger.info("consumer terminate, reason: #{inspect(reason)}")

    # stop broker
    ds
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Enum.each(fn pid ->
      DynamicSupervisor.terminate_child(ds, pid)
    end)
  end

  def handle_continue(
        :on_start,
        %State{
          consume_info_map: cmap,
          consume_opts: %{
            model: :cluster,
            retry_topic: topic
          }
        } = state
      ) do
    sub = %Subscription{
      class_filter_mode: false,
      topic: topic,
      sub_string: "*",
      sub_version: System.system_time(:nanosecond),
      expression_type: "TAG"
    }

    Process.send_after(self(), :update_route_info, 1000)
    Process.send_after(self(), :heartbeat, 2000)
    Process.send_after(self(), :rebalance, 10_000)
    {:noreply, %State{state | consume_info_map: Map.put(cmap, topic, {sub, [], [], %{}})}}
  end

  def handle_continue(:on_start, state) do
    Process.send_after(self(), :heartbeat, 1000)
    Process.send_after(self(), :update_route_info, 1000)
    {:noreply, state}
  end

  def handle_call(
        {:subscribe, topic, msg_selector},
        _from,
        %State{
          consume_opts: %{
            namespace: namespace
          },
          namesrvs: namesrvs,
          consume_info_map: cmap
        } = state
      ) do
    topic = with_namespace(topic, namespace)

    case Map.fetch(cmap, topic) do
      {:ok, _} ->
        Logger.warning("topic already been subscribed: #{topic}")
        {:reply, {:error, "topic already subscibed"}, state}

      :error ->
        sub = message_selector_to_subscription(topic, msg_selector)

        fetch_consume_info(namesrvs, topic)
        |> case do
          {:ok, {broker_datas, mqs}} ->
            state = %State{
              state
              | consume_info_map: Map.put(cmap, topic, {sub, broker_datas, mqs, %{}})
            }

            do_heartbeat(state)

            {:reply, :ok, state}

          {:error, reason} = error ->
            Logger.error("fetch consume info for topic: #{topic} error: #{inspect(reason)}")
            {:reply, error, state}
        end
    end
  end

  def handle_call(
        {:unsubscribe, topic},
        _from,
        %State{
          consume_opts: %{
            namespace: namespace
          },
          consume_info_map: cmap,
          task_supervisor: task_supervisor
        } = state
      ) do
    topic = with_namespace(topic, namespace)

    cmap =
      Map.pop(cmap, topic)
      |> case do
        {nil, cmap} ->
          cmap

        {{_sub, _broker_datas, _mqs, consume_tasks}, cmap} ->
          # stop consume task
          consume_tasks
          |> Map.values()
          |> Enum.each(fn pid ->
            Task.Supervisor.terminate_child(task_supervisor, pid)
          end)

          cmap
      end

    {:reply, :ok, %State{state | consume_info_map: cmap}}
  end

  def handle_info(
        :update_route_info,
        %State{
          namesrvs: namesrvs,
          consume_info_map: cmap,
          registry: registry,
          broker_dynamic_supervisor: broker_dynamic_supervisor
        } = state
      ) do
    cmap =
      cmap
      |> Map.to_list()
      |> Enum.reduce_while(%{}, fn {topic, {sub, _, _, consume_tasks} = old}, acc ->
        fetch_consume_info(namesrvs, topic)
        |> case do
          {:ok, {broker_datas, mqs}} ->
            Logger.debug("fetch consume info for topic: #{topic}, mqs: #{inspect(mqs)}")
            # establish connection to new broker
            connect_to_brokers(broker_datas, registry, broker_dynamic_supervisor)

            {:cont, Map.put(acc, topic, {sub, broker_datas, mqs, consume_tasks})}

          {:error, reason} ->
            Logger.error("fetch consume info for topic: #{topic} error: #{inspect(reason)}")
            {:cont, Map.put(acc, topic, old)}
        end
      end)

    Process.send_after(self(), :update_route_info, 30_000)

    {:noreply, %State{state | consume_info_map: cmap}}
  end

  def handle_info(
        :heartbeat,
        state
      ) do
    do_heartbeat(state)
    Process.send_after(self(), :heartbeat, 30_000)

    {:noreply, state}
  end

  def handle_info(:rebalance, state) do
    state = do_balance(state)
    Process.send_after(self(), :rebalance, 20_000)
    {:noreply, state}
  end

  def handle_info(
        {:notify, {pkt, _broker_pid}},
        %State{} = state
      ) do
    Logger.warning("consumer receive notify: #{inspect(pkt)}")

    case Packet.packet(pkt, :code) do
      @req_notify_consumer_ids_changed ->
        {:noreply, do_balance(state)}

      other_code ->
        Logger.warning("unknown notify code: #{other_code}")
        {:noreply, state}
    end
  end

  # ---- private functions ----

  @spec with_namespace(Typespecs.group_name() | Typespecs.topic(), Typespecs.namespace()) ::
          Typespecs.group_name()
  defp with_namespace(name, ""), do: name
  defp with_namespace(name, namespace), do: name <> "%" <> namespace

  @spec retry_topic(Typespecs.group_name()) :: Typespecs.topic()
  defp retry_topic(group_name), do: "%RETRY%" <> group_name

  @spec fetch_consume_info(pid() | atom(), Typespecs.topic()) ::
          {:ok, {[BrokerData.t()], [MessageQueue.t()]}} | Typespecs.error_t()
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

  @spec do_balance(State.t()) :: State.t()
  defp do_balance(
         %State{
           client_id: client_id,
           consume_info_map: consume_info_map,
           registry: registry,
           broker_dynamic_supervisor: broker_dynamic_supervisor,
           task_supervisor: task_supervisor,
           consume_opts: %{
             group_name: group_name,
             model: :cluster,
             balance_strategy: strategy
           }
         } = state
       ) do
    consume_info_map
    |> Map.to_list()
    |> Enum.reduce_while(
      %{},
      fn {topic, {sub, broker_datas, mqs, consume_tasks} = old}, acc ->
        Logger.debug(
          "rebalance topic: #{topic}, broker_datas: #{inspect(broker_datas)}, mqs: #{inspect(mqs)}"
        )

        bd = Enum.random(broker_datas)

        broker =
          Broker.get_or_new_broker(
            bd.broker_name,
            BrokerData.slave_addr(bd),
            registry,
            broker_dynamic_supervisor
          )

        with {:ok, cids} <- Broker.get_consumer_list_by_group(broker, group_name),
             {:ok, allocated_mqs} <- BalanceStrategy.allocate(strategy, client_id, mqs, cids) do
          Logger.debug("rebalance topic: #{topic}, allocated_mqs: #{inspect(allocated_mqs)}")

          # to stop consume task: in consume_tasks but not in allocated_mqs
          to_stop =
            consume_tasks
            |> Map.reject(fn {mq, _pid} -> Enum.member?(allocated_mqs, mq) end)

          # terminate consume task
          Enum.each(to_stop, fn {mq, pid} ->
            Logger.warning("stop consume task: #{inspect(mq)}")
            Task.Supervisor.terminate_child(task_supervisor, pid)
          end)

          # to start consume task: in allocated_mqs but not in consume_tasks
          new_tasks =
            allocated_mqs
            |> Enum.reject(fn mq -> Map.has_key?(consume_tasks, mq) end)
            |> Enum.into(%{}, fn mq ->
              bd =
                broker_datas
                |> Enum.find(fn bd -> bd.broker_name == mq.broker_name end)

              Logger.warning("new consume task for mq: #{inspect(mq)}")

              {:ok, tid} = new_consume_task(task_supervisor, mq, bd, topic, sub, state)
              {mq, tid}
            end)

          current_consume_task =
            consume_tasks
            |> Map.merge(new_tasks)
            |> Map.reject(fn {mq, _} -> Map.has_key?(to_stop, mq) end)

          {:cont, Map.put(acc, topic, {sub, broker_datas, mqs, current_consume_task})}
        else
          {:error, reason} ->
            Logger.error("rebalance topic: #{topic} error: #{inspect(reason)}")
            {:cont, Map.put(acc, topic, old)}

          _ ->
            {:cont, Map.put(acc, topic, old)}
        end
      end
    )
    |> then(fn new_consume_info_map ->
      # Logger.info("new consume info map: #{inspect(new_consume_info_map)}")
      %State{state | consume_info_map: new_consume_info_map}
    end)
  end

  @spec new_consume_task(
          pid(),
          MessageQueue.t(),
          BrokerData.t(),
          Typespecs.topic(),
          Subscription.t(),
          State.t()
        ) :: {:ok, pid()}
  defp new_consume_task(task_supervisor, mq, bd, topic, sub, %State{
         registry: registry,
         broker_dynamic_supervisor: broker_dynamic_supervisor,
         task_supervisor: task_supervisor,
         processor: processor,
         consume_opts: %{
           group_name: group_name,
           model: :cluster,
           consume_from_where: cfw,
           consume_timestamp: consume_timestamp,
           consume_orderly: consume_orderly,
           post_subscription_when_pull: post_subscription_when_pull,
           pull_batch_size: pull_batch_size,
           consume_batch_size: consume_batch_size
         }
       }) do
    Task.Supervisor.start_child(
      task_supervisor,
      fn ->
        InnerConsumer.Concurrent.pull_msg(%ConsumeState{
          task_id: Util.Random.generate_id("T"),
          group_name: group_name,
          topic: topic,
          broker_data: bd,
          registry: registry,
          broker_dynamic_supervisor: broker_dynamic_supervisor,
          mq: mq,
          subscription: sub,
          consume_from_where: cfw,
          consume_timestamp: consume_timestamp,
          consume_orderly: consume_orderly,
          post_subscription_when_pull: post_subscription_when_pull,
          commit_offset_enable: false,
          commit_offset: 0,
          pull_batch_size: pull_batch_size,
          consume_batch_size: consume_batch_size,
          # use a negative number to indicate that we need to get remote offset
          next_offset: -1,
          processor: processor
        })
      end,
      restart: :transient
    )
  end

  @spec do_heartbeat(State.t()) :: :ok
  defp do_heartbeat(%State{
         client_id: cid,
         broker_dynamic_supervisor: broker_dynamic_supervisor,
         consume_info_map: cmap,
         consume_opts: %{
           model: model,
           group_name: group_name,
           consume_from_where: cfw
         }
       }) do
    Logger.debug("do heartbeat")

    heartbeat_data = %Heartbeat{
      client_id: cid,
      consumer_data_set: [
        %ConsumerData{
          group: group_name,
          consume_type: "CONSUME_PASSIVELY",
          message_model: (model == :cluster && "Clustering") || "BroadCasting",
          consume_from_where: consume_from_where_str(cfw),
          subscription_data_set: cmap |> Map.values() |> Enum.map(&elem(&1, 0)),
          unit_mode: false
        }
      ]
    }

    broker_dynamic_supervisor
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Task.async_stream(fn pid ->
      # Logger.debug("send heartbeat to broker: #{inspect(pid)}")

      Broker.heartbeat(pid, heartbeat_data)
      |> case do
        :ok ->
          :ok

        {:error, reason} = err ->
          Logger.error("heartbeat error: #{inspect(reason)}")
          err
      end
    end)
    |> Stream.run()
  end

  @spec message_selector_to_subscription(Typespecs.topic(), MsgSelector.t()) :: Subscription.t()
  defp message_selector_to_subscription(topic, msg_selector) do
    %Subscription{
      class_filter_mode: false,
      topic: topic,
      sub_string: msg_selector.expression,
      tags_set: MsgSelector.tags(msg_selector),
      code_set: MsgSelector.codes(msg_selector),
      sub_version: System.system_time(:nanosecond),
      expression_type: (msg_selector.type == :tag && "TAG") || "SQL92"
    }
  end

  @spec connect_to_brokers(
          list(BrokerData.t()),
          pid(),
          pid()
        ) :: :ok
  defp connect_to_brokers(broker_datas, registry, dynamic_supervisor) do
    broker_datas
    |> Task.async_stream(fn bd ->
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.master_addr(bd),
        registry,
        dynamic_supervisor
      )

      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.slave_addr(bd),
        registry,
        dynamic_supervisor
      )
    end)
    |> Stream.run()
  end
end
