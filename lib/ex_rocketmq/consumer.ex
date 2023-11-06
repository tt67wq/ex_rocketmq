defmodule ExRocketmq.Consumer do
  @moduledoc """
  RocketMQ consumer

  This module provides functionality for consuming messages from RocketMQ.

  It defines a `State` struct that holds the state of the consumer and its configuration options.

  ## Example

  1. Start a namesrvrs:
    ```Elixir
    {:ok, namesrvs_pid} = Namesrvs.start_link(
      remotes: [
       [transport: Transport.Tcp.new(host: "test.rocket-mq.net", port: 31_120)]
     ],
     opts: [
       name: :namesrvs
     ]
    )
    ```
  2. Define a processor:
    ```Elixir
    defmodule MyProcessor do

      alias ExRocketmq.{
        Consumer.Processor,
        Models.MessageExt,
        Typespecs
      }

      @behaviour Processor

      defstruct []

      @type t :: %__MODULE__{}

      @spec new() :: t()
      def new, do: %__MODULE__{}

      @spec process(t(), Typespecs.topic(), [MessageExt.t()]) ::
              Processor.consume_result() | {:error, term()}
      def process(_, topic, msgs) do
        msgs
        |> Enum.each(fn msg ->
          IO.inspect(msg)
        end)

        :success
      end
    end
    ```

  3. Start consumer process
     ```Elixir
     Consumer.start_link(
       consumer_group: "GID_POETRY",
       namesrvs: :namesrvs,
       processor: MyProcessor.new(),
       subscriptions: %{"POETRY" => MsgSelector.new(:tag, "*")},
       trace_enable: true,
       opts: [
         name: :consumer
       ]}
     )
     ```

  """

  defmodule State do
    @moduledoc false

    alias ExRocketmq.{Typespecs, Models}

    @type t :: %__MODULE__{
            client_id: String.t(),
            namesrvs: pid() | atom(),
            processor: ExRocketmq.Consumer.Processor.t(),
            # consume_info_map stores the consume info of each topic
            # consume info contains:
            #   1. subscription
            #   2. broker_datas
            #   3. mqs
            #   4. consume task for assigned queue
            consume_info_map: %{
              Typespecs.topic() => {
                Models.Subscription.t(),
                list(Models.BrokerData.t()),
                list(Models.MessageQueue.t()),
                %{Models.MessageQueue.t() => pid()}
              }
            },
            trace_enable: boolean(),
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
              consume_batch_size: non_neg_integer(),
              max_reconsume_times: non_neg_integer()
            }
          }

    defstruct client_id: "",
              namesrvs: nil,
              consume_info_map: %{},
              processor: nil,
              trace_enable: false,
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
                consume_batch_size: 16,
                max_reconsume_times: 16
              }
  end

  use GenServer

  alias ExRocketmq.{
    Util,
    Typespecs,
    Namesrvs,
    Broker,
    Consumer.BalanceStrategy,
    Consumer.Supervisor,
    Remote.Packet,
    InnerConsumer,
    Tracer
  }

  alias ExRocketmq.Protocol.{ConsumeResult, Request, PullStatus, Response}

  alias ExRocketmq.Models.{
    Subscription,
    MsgSelector,
    QueueData,
    Heartbeat,
    ConsumerData,
    BrokerData,
    MessageQueue,
    ConsumeState,
    ConsumeMessageDirectly,
    ConsumeMessageDirectlyResult,
    MessageExt
  }

  require Logger
  require Request
  require Packet
  require PullStatus
  require ConsumeResult
  require Response

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
    max_reconsume_times: [
      type: :non_neg_integer,
      doc: "The max times to reconsume message",
      default: 16
    ],
    trace_enable: [
      type: :boolean,
      doc: "Whether to enable trace collection",
      default: false
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The opts of the comsumer's GenServer"
    ]
  ]

  # request const
  @req_notify_consumer_ids_changed Request.req_notify_consumer_ids_changed()
  @req_consume_message_directly Request.req_consume_message_directly()

  # consume result
  @consume_success ConsumeResult.success()
  @consume_retry_later ConsumeResult.retry_later()
  @consume_suspend ConsumeResult.suspend_current_queue_a_moment()

  # resp
  @resp_success Response.resp_success()

  @type consumer_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@consumer_opts_schema))]

  @doc """
  Starts a RocketMQ consumer process.

  ## Options
  #{NimbleOptions.docs(@consumer_opts_schema)}

  ## Examples

      iex> ExRocketmq.Consumer.start_link(%{group_name: "my_group", namesrvs: "localhost:9876"})
      {:ok, pid}

  """
  @spec start_link(consumer_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    opts =
      opts
      |> NimbleOptions.validate!(@consumer_opts_schema)

    {opts, init} = Keyword.pop(opts, :opts)
    GenServer.start_link(__MODULE__, init, opts)
  end

  @doc """
  stop a RocketMQ consumer process.
  """
  @spec stop(pid() | atom()) :: :ok
  def stop(consumer), do: GenServer.stop(consumer)

  @doc """
  Subscribes to a RocketMQ topic with an optional message selector.

  After subscribing, the consumer will start to pull messages from the topic.
  And if the broker address changes, consumer update the connection to the broker.

  ## Examples

      iex> ExRocketmq.Consumer.subscribe(consumer, "my_topic")
      :ok

  """
  @spec subscribe(pid() | atom(), Typespecs.topic(), MsgSelector.t()) :: :ok
  def subscribe(consumer, topic, msg_selector \\ %MsgSelector{}) do
    GenServer.call(consumer, {:subscribe, topic, msg_selector})
  end

  @doc """
  Unsubscribes from a RocketMQ topic.

  ## Examples

      iex> ExRocketmq.Consumer.unsubscribe(consumer, "my_topic")
      :ok

  """
  @spec unsubscribe(pid() | atom(), Typespecs.topic()) :: :ok
  def unsubscribe(consumer, topic) do
    GenServer.call(consumer, {:unsubscribe, topic})
  end

  # ------- server callbacks -------

  def init(opts) do
    cid = Util.ClientId.get("Consumer")
    # prepare subscriptions
    cmap =
      opts[:subscriptions]
      |> Enum.into(%{}, fn {topic, msg_selector} ->
        {topic, {message_selector_to_subscription(topic, msg_selector), [], [], %{}}}
      end)

    {:ok, _} =
      Supervisor.start_link(
        opts: [
          cid: cid,
          trace_enable: opts[:trace_enable],
          namesrvs: opts[:namesrvs]
        ]
      )

    {:ok,
     %State{
       client_id: cid,
       namesrvs: opts[:namesrvs],
       consume_info_map: cmap,
       processor: opts[:processor],
       trace_enable: opts[:trace_enable],
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
         consume_batch_size: opts[:consume_batch_size],
         max_reconsume_times: opts[:max_reconsume_times]
       }
     }, {:continue, :on_start}}
  end

  def terminate(reason, %State{
        client_id: cid,
        trace_enable: trace_enable
      }) do
    Logger.info("consumer terminate, reason: #{inspect(reason)}")

    # stop broker
    dynamic_supervisor = :"DynamicSupervisor.#{cid}"

    dynamic_supervisor
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Enum.each(fn pid ->
      DynamicSupervisor.terminate_child(dynamic_supervisor, pid)
    end)

    # stop trace
    if trace_enable do
      Tracer.stop(:"Tracer.#{cid}")
    end

    # stop all tasks
    task_supervisor = :"Task.Supervisor.#{cid}"

    task_supervisor
    |> Task.Supervisor.children()
    |> Enum.each(fn pid ->
      Task.Supervisor.terminate_child(task_supervisor, pid)
    end)
  end

  # cluster consumer, we have to register retry topic to consume retry msgs
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
    Process.send_after(self(), :update_route_info, 1000)
    Process.send_after(self(), :heartbeat, 2000)
    Process.send_after(self(), :rebalance, 10_000)
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

            # establish connection to new broker immediately
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
          client_id: cid,
          consume_opts: %{
            namespace: namespace
          },
          consume_info_map: cmap
        } = state
      ) do
    topic = with_namespace(topic, namespace)

    {ret, cmap} =
      Map.pop(cmap, topic)
      |> case do
        {nil, cmap} ->
          {{:error, "topic #{topic} has never been subscribed"}, cmap}

        {{_sub, _broker_datas, _mqs, consume_tasks}, cmap} ->
          # stop consume task
          consume_tasks
          |> Map.values()
          |> Enum.each(fn pid ->
            Task.Supervisor.terminate_child(:"Task.Supervisor.#{cid}", pid)
          end)

          {:ok, cmap}
      end

    {:reply, ret, %State{state | consume_info_map: cmap}}
  end

  def handle_info(
        :update_route_info,
        %State{
          client_id: cid,
          namesrvs: namesrvs,
          consume_info_map: cmap
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
            # establish connection to new broker for later use
            connect_to_brokers(broker_datas, cid, self())

            {:cont, Map.put(acc, topic, {sub, broker_datas, mqs, consume_tasks})}

          {:error, reason} ->
            Logger.error("fetch consume info for topic: #{topic} error: #{inspect(reason)}")
            {:cont, Map.put(acc, topic, old)}
        end
      end)

    Process.send_after(self(), :update_route_info, 30_000)

    {:noreply, %State{state | consume_info_map: cmap}}
  end

  # send heartbeat every 30s
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
        {:notify, {pkt, broker_pid}},
        %State{} = state
      ) do
    Logger.warning("consumer receive notify: #{inspect(pkt)}")

    case Packet.packet(pkt, :code) do
      @req_notify_consumer_ids_changed ->
        {:noreply, do_balance(state)}

      @req_consume_message_directly ->
        consume_message_directly(pkt, broker_pid, state)
        {:noreply, state}

      other_code ->
        Logger.warning("unimplemented notify code: #{other_code}")
        {:noreply, state}
    end
  end

  def handle_info(cmd, state) do
    Logger.error("unimplemented cmd: #{inspect(cmd)}")
    {:noreply, state}
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
           client_id: cid,
           consume_info_map: consume_info_map,
           consume_opts: %{
             model: :broadcast
           }
         } = state
       ) do
    # For broadcast-type consumers, rebalancing does not require allocating messages from message queues.
    # It simply needs to perform the consumption tasks for all message queues.
    consume_info_map
    |> Map.to_list()
    |> Enum.reduce(
      %{},
      fn {topic, {sub, broker_datas, mqs, consume_tasks}}, acc ->
        to_stop =
          consume_tasks
          |> Map.reject(fn {mq, _pid} -> Enum.member?(mqs, mq) end)

        # terminate consume task
        Enum.each(to_stop, fn {mq, pid} ->
          Logger.warning("stop consume task: #{inspect(mq)}")
          Task.Supervisor.terminate_child(:"Task.Supervisor.#{cid}", pid)
        end)

        # to start consume task: in allocated_mqs but not in consume_tasks
        new_tasks =
          mqs
          |> Enum.reject(fn mq -> Map.has_key?(consume_tasks, mq) end)
          |> Enum.into(%{}, fn mq ->
            bd =
              broker_datas
              |> Enum.find(fn bd -> bd.broker_name == mq.broker_name end)

            Logger.warning("new consume task for mq: #{inspect(mq)}")

            {:ok, tid} = new_consume_task(:"Task.Supervisor.#{cid}", mq, bd, topic, sub, state)
            {mq, tid}
          end)

        current_consume_task =
          consume_tasks
          |> Map.merge(new_tasks)
          |> Map.reject(fn {mq, _} -> Map.has_key?(to_stop, mq) end)

        Map.put(acc, topic, {sub, broker_datas, mqs, current_consume_task})
      end
    )
    |> then(fn new_consume_info_map ->
      %State{state | consume_info_map: new_consume_info_map}
    end)
  end

  defp do_balance(
         %State{
           client_id: cid,
           consume_info_map: consume_info_map,
           consume_opts: %{
             group_name: group_name,
             model: :cluster,
             balance_strategy: strategy
           }
         } = state
       ) do
    # For cluster-type consumers, each consumer needs to evenly distribute all messages
    # across brokers according to the same allocation strategy.
    # Whenever the number of consumers changes or the messages in brokers are adjusted,
    # it will trigger a reassignment.
    # The consumers assigned messages will perform consumption tasks for each message.
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
            :"Registry.#{cid}",
            :"DynamicSupervisor.#{cid}",
            self()
          )

        with {:ok, cids} <- Broker.get_consumer_list_by_group(broker, group_name),
             {:ok, allocated_mqs} <- BalanceStrategy.allocate(strategy, cid, mqs, cids) do
          Logger.debug("rebalance topic: #{topic}, allocated_mqs: #{inspect(allocated_mqs)}")

          # to stop consume task: in consume_tasks but not in allocated_mqs
          to_stop =
            consume_tasks
            |> Map.reject(fn {mq, _pid} -> Enum.member?(allocated_mqs, mq) end)

          # terminate consume task
          Enum.each(to_stop, fn {mq, pid} ->
            Logger.warning("stop consume task: #{inspect(mq)}")
            Task.Supervisor.terminate_child(:"Task.Supervisor.#{cid}", pid)
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

              {:ok, tid} = new_consume_task(:"Task.Supervisor.#{cid}", mq, bd, topic, sub, state)
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
          pid() | atom(),
          MessageQueue.t(),
          BrokerData.t(),
          Typespecs.topic(),
          Subscription.t(),
          State.t()
        ) :: {:ok, pid()}

  defp new_consume_task(task_supervisor, mq, bd, topic, sub, %State{
         client_id: cid,
         processor: processor,
         consume_opts: %{
           group_name: group_name,
           model: :broadcast,
           consume_from_where: cfw,
           consume_timestamp: consume_timestamp,
           consume_orderly: consume_orderly,
           post_subscription_when_pull: post_subscription_when_pull,
           pull_batch_size: pull_batch_size,
           consume_batch_size: consume_batch_size,
           max_reconsume_times: max_reconsume_times
         }
       }) do
    inner_consumer =
      if consume_orderly do
        InnerConsumer.BroadcastOrder
      else
        InnerConsumer.BroadcastConcurrent
      end

    Task.Supervisor.start_child(
      task_supervisor,
      fn ->
        apply(inner_consumer, :pull_msg, [
          %ConsumeState{
            task_id: Util.Random.generate_id("T"),
            client_id: cid,
            group_name: group_name,
            topic: topic,
            broker_data: bd,
            mq: mq,
            subscription: sub,
            consume_from_where: cfw,
            consume_timestamp: consume_timestamp,
            post_subscription_when_pull: post_subscription_when_pull,
            commit_offset_enable: false,
            commit_offset: 0,
            pull_batch_size: pull_batch_size,
            consume_batch_size: consume_batch_size,
            # use a negative number to indicate that we need to get remote offset
            next_offset: -1,
            processor: processor,
            max_reconsume_times: max_reconsume_times
          }
        ])
      end,
      restart: :transient
    )
  end

  defp new_consume_task(task_supervisor, mq, bd, topic, sub, %State{
         client_id: cid,
         processor: processor,
         trace_enable: trace_enable,
         consume_opts: %{
           group_name: group_name,
           model: :cluster,
           consume_from_where: cfw,
           consume_timestamp: consume_timestamp,
           consume_orderly: consume_orderly,
           post_subscription_when_pull: post_subscription_when_pull,
           pull_batch_size: pull_batch_size,
           consume_batch_size: consume_batch_size,
           max_reconsume_times: max_reconsume_times
         }
       }) do
    inner_consumer =
      if consume_orderly do
        InnerConsumer.Order
      else
        InnerConsumer.Concurrent
      end

    Task.Supervisor.start_child(
      task_supervisor,
      fn ->
        apply(inner_consumer, :pull_msg, [
          %ConsumeState{
            task_id: Util.Random.generate_id("T"),
            client_id: cid,
            group_name: group_name,
            topic: topic,
            broker_data: bd,
            mq: mq,
            trace_enable: trace_enable,
            subscription: sub,
            consume_from_where: cfw,
            consume_timestamp: consume_timestamp,
            post_subscription_when_pull: post_subscription_when_pull,
            commit_offset_enable: false,
            commit_offset: 0,
            pull_batch_size: pull_batch_size,
            consume_batch_size: consume_batch_size,
            # use a negative number to indicate that we need to get remote offset
            next_offset: -1,
            processor: processor,
            max_reconsume_times: max_reconsume_times
          }
        ])
      end,
      restart: :transient
    )
  end

  @spec do_heartbeat(State.t()) :: :ok
  defp do_heartbeat(%State{
         client_id: cid,
         consume_info_map: cmap,
         consume_opts: %{
           model: model,
           group_name: group_name,
           consume_from_where: cfw
         }
       }) do
    heartbeat_data = %Heartbeat{
      client_id: cid,
      consumer_data_set: [
        %ConsumerData{
          group: group_name,
          # push consumer
          consume_type: "CONSUME_PASSIVELY",
          message_model: (model == :cluster && "Clustering") || "BroadCasting",
          consume_from_where: consume_from_where_str(cfw),
          subscription_data_set: cmap |> Map.values() |> Enum.map(&elem(&1, 0)),
          unit_mode: false
        }
      ]
    }

    # send heartbeat to all brokers
    :"DynamicSupervisor.#{cid}"
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Task.async_stream(fn pid ->
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
          String.t(),
          pid()
        ) :: :ok
  defp connect_to_brokers(broker_datas, cid, pid) do
    broker_datas
    |> Task.async_stream(fn bd ->
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.master_addr(bd),
        :"Registry.#{cid}",
        :"DynamicSupervisor.#{cid}",
        pid
      )

      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.slave_addr(bd),
        :"Registry.#{cid}",
        :"DynamicSupervisor.#{cid}",
        pid
      )
    end)
    |> Stream.run()
  end

  defp consume_message_directly(pkt, broker_pid, %State{
         client_id: cid,
         trace_enable: trace_enable,
         processor: processor
       }) do
    req =
      pkt
      |> Packet.packet(:ext_fields)
      |> ConsumeMessageDirectly.decode()

    Logger.info("consume message directly, req: #{inspect(req)}")

    if req.client_id != cid do
      Logger.warning("client id not match, ignore")
    else
      [msg] = MessageExt.decode_from_binary(Packet.packet(pkt, :body))

      begin_at = System.system_time(:millisecond)

      tracer =
        if trace_enable do
          :"Tracer.#{cid}"
        else
          nil
        end

      ret =
        InnerConsumer.Common.process_with_trace(
          tracer,
          processor,
          req.consumer_group,
          msg.message.topic,
          [msg]
        )
        |> case do
          :success ->
            @consume_success

          {:retry_later, _} ->
            @consume_retry_later

          {:suspend, _, _} ->
            @consume_suspend
        end

      body =
        %ConsumeMessageDirectlyResult{
          order: false,
          auto_commit: true,
          consume_result: ret,
          spend_time_millis: System.system_time(:millisecond) - begin_at
        }
        |> ConsumeMessageDirectlyResult.encode()

      reply_pkt =
        Packet.packet(
          code: @resp_success,
          opaque: Packet.packet(pkt, :opaque),
          flag: 1,
          body: body
        )

      Broker.send_reply_pkt(broker_pid, reply_pkt)
    end
  end
end
