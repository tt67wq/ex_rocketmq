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
    Transport,
    Consumer.BalanceStrategy,
    Consumer.Processor,
    Protocol.Request,
    Protocol.PullStatus,
    Remote.Packet
  }

  alias ExRocketmq.Models.{
    Subscription,
    MsgSelector,
    QueueData,
    Heartbeat,
    ConsumerData,
    BrokerData,
    MessageQueue,
    PullMsgTask,
    PullMsg,
    QueryConsumerOffset,
    GetMaxOffset,
    SearchOffset
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
  @pull_status_found PullStatus.pull_found()
  @pull_status_no_new_msg PullStatus.pull_no_new_msg()
  @pull_status_no_matched_msg PullStatus.pull_no_matched_msg()

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

    Process.send_after(self(), :heartbeat, 1_000)
    Process.send_after(self(), :update_route_info, 1000)
    Process.send_after(self(), :rebalance, 5000)
    {:noreply, %State{state | consume_info_map: Map.put(cmap, topic, {sub, [], [], %{}})}}
  end

  def handle_continue(:on_start, state) do
    Process.send_after(self(), :heartbeat, 1_000)
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

        with {:ok, {broker_datas, mqs}} <- fetch_consume_info(namesrvs, topic) do
          state = %State{
            state
            | consume_info_map: Map.put(cmap, topic, {sub, broker_datas, mqs, %{}})
          }

          do_heartbeat(state)

          {:reply, :ok, state}
        else
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
          consume_info_map: cmap
        } = state
      ) do
    cmap =
      cmap
      |> Map.to_list()
      |> Enum.reduce_while(%{}, fn {topic, {sub, _, _, consume_tasks} = old}, acc ->
        case fetch_consume_info(namesrvs, topic) do
          {:ok, {broker_datas, mqs}} ->
            # RETHINK: should we check if the consume info changed and do rebalance?
            Logger.debug("fetch consume info for topic: #{topic}, mqs: #{inspect(mqs)}")
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
    Process.send_after(self(), :rebalance, 30_000)
    {:noreply, do_balance(state)}
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

  @spec retry_topic?(Typespecs.topic()) :: boolean()
  defp retry_topic?(topic), do: String.starts_with?(topic, "%RETRY%")

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

  @spec get_or_new_broker(String.t(), String.t(), atom(), pid()) :: pid()
  defp get_or_new_broker(broker_name, addr, registry, dynamic_supervisor) do
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

        {:ok, pid} = DynamicSupervisor.start_child(dynamic_supervisor, {Broker, broker_opts})

        # bind self to broker, then notify from broker will send to self
        Broker.controlling_process(pid, self())

        pid

      [{pid, _}] ->
        pid
    end
  end

  @spec do_balance(State.t()) :: State.t()
  defp do_balance(
         %State{
           client_id: client_id,
           consume_info_map: consume_info_map,
           registry: registry,
           broker_dynamic_supervisor: broker_dynamic_supervisor,
           task_supervisor: task_supervisor,
           processor: processor,
           consume_opts: %{
             group_name: group_name,
             model: :cluster,
             balance_strategy: strategy,
             consume_from_where: cfw,
             consume_timestamp: consume_timestamp,
             consume_orderly: consume_orderly,
             post_subscription_when_pull: post_subscription_when_pull,
             pull_batch_size: pull_batch_size,
             consume_batch_size: consume_batch_size
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
          get_or_new_broker(
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
            |> tap(fn to_stop ->
              Enum.each(to_stop, fn {mq, pid} ->
                Logger.info("stop consume task: #{inspect(mq)}")
                Task.Supervisor.terminate_child(task_supervisor, pid)
              end)
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

              {:ok, tid} =
                Task.Supervisor.start_child(
                  task_supervisor,
                  fn ->
                    pull_msg(%PullMsgTask{
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
                      next_offset: 0,
                      processor: processor
                    })
                  end,
                  restart: :transient
                )

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

  defp pull_msg(
         %PullMsgTask{
           broker_data: bd,
           registry: registry,
           broker_dynamic_supervisor: dynamic_supervisor,
           group_name: group_name,
           topic: topic,
           mq: %MessageQueue{
             queue_id: queue_id
           },
           next_offset: 0,
           consume_from_where: cfw,
           consume_timestamp: consume_timestamp
         } = task
       ) do
    # get remote offset
    {:ok, offset} =
      get_or_new_broker(
        bd.broker_name,
        BrokerData.slave_addr(bd),
        registry,
        dynamic_supervisor
      )
      |> get_next_offset(
        group_name,
        topic,
        queue_id,
        cfw,
        consume_timestamp
      )

    pull_msg(%{task | next_offset: offset})
  end

  defp pull_msg(
         %PullMsgTask{
           topic: topic,
           group_name: group_name,
           mq: %MessageQueue{
             queue_id: queue_id
           },
           broker_data: bd,
           consume_orderly: false,
           next_offset: next_offset,
           commit_offset_enable: commit_offset_enable,
           post_subscription_when_pull: post_subscription_when_pull,
           subscription: %Subscription{
             sub_string: sub_string,
             class_filter_mode: cfm,
             expression_type: expression_type
           },
           pull_batch_size: pull_batch_size,
           consume_batch_size: consume_batch_size,
           commit_offset: commit_offset,
           registry: registry,
           broker_dynamic_supervisor: dynamic_supervisor,
           processor: processor
         } = pt
       ) do
    sub_expression =
      if post_subscription_when_pull and not cfm do
        sub_string
      else
        ""
      end

    pull_req = %PullMsg.Request{
      consumer_group: group_name,
      topic: topic,
      queue_id: queue_id,
      queue_offset: next_offset,
      max_msg_nums: pull_batch_size,
      sys_flag: build_pullmsg_sys_flag(commit_offset_enable, sub_expression, cfm),
      commit_offset: commit_offset,
      suspend_timeout_millis: 20_000,
      sub_expression: sub_expression,
      expression_type: expression_type
    }

    # Util.Debug.debug("pull msg: #{inspect(pull_req)}")

    broker =
      if commit_offset_enable do
        BrokerData.master_addr(bd)
      else
        BrokerData.slave_addr(bd)
      end
      |> then(fn addr ->
        get_or_new_broker(
          bd.broker_name,
          addr,
          registry,
          dynamic_supervisor
        )
      end)

    with {:ok,
          %PullMsg.Response{
            status: status,
            next_begin_offset: next_begin_offset,
            messages: message_exts
          }} <- Broker.pull_message(broker, pull_req) do
      case status do
        @pull_status_found ->
          consume_msgs_concurrently(message_exts, consume_batch_size, topic, processor)

          pull_msg(%{
            pt
            | next_offset: next_begin_offset,
              commit_offset: next_begin_offset,
              commit_offset_enable: true
          })

        @pull_status_no_new_msg ->
          Process.sleep(5000)
          pull_msg(pt)

        @pull_status_no_matched_msg ->
          Process.sleep(1000)
          pull_msg(pt)

        status ->
          Logger.critical("pull message error: #{inspect(status)}, terminate pull task")
          :stop
      end
    else
      {:error, reason} ->
        Logger.error("pull message error: #{inspect(reason)}")
        Process.sleep(1000)
        pull_msg(pt)

      other ->
        Logger.error("pull message error: #{inspect(other)}")
        Process.sleep(1000)
        pull_msg(pt)
    end
  end

  @spec get_next_offset(
          pid(),
          Typespecs.group_name(),
          Typespecs.topic(),
          non_neg_integer(),
          Typespecs.consume_from_where(),
          non_neg_integer()
        ) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  defp get_next_offset(broker, group_name, topic, queue_id, cfw, consume_timestamp) do
    with {:ok, last_offset} <-
           Broker.query_consumer_offset(broker, %QueryConsumerOffset{
             consumer_group: group_name,
             topic: topic,
             queue_id: queue_id
           }) do
      if last_offset > 0 do
        {:ok, last_offset}
      else
        # no offset record
        case cfw do
          :last_offset ->
            get_last_offset(topic, broker, queue_id)

          :first_offset ->
            {:ok, 0}

          :timestamp ->
            get_offset_by_timestamp(topic, broker, queue_id, consume_timestamp)
        end
      end
    end
  end

  @spec get_last_offset(Typespecs.topic(), pid(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  defp get_last_offset(topic, broker, queue_id) do
    if retry_topic?(topic) do
      {:ok, 0}
    else
      case Broker.get_max_offset(broker, %GetMaxOffset{
             topic: topic,
             queue_id: queue_id
           }) do
        {:ok, offset} -> {:ok, offset}
        _ -> {:error, :get_max_offset_error}
      end
    end
  end

  @spec get_offset_by_timestamp(Typespecs.topic(), pid(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  defp get_offset_by_timestamp(topic, broker, queue_id, consume_timestamp) do
    if retry_topic?(topic) do
      Broker.get_max_offset(broker, %GetMaxOffset{
        topic: topic,
        queue_id: queue_id
      })
      |> case do
        {:ok, offset} -> {:ok, offset}
        _ -> {:error, :get_max_offset_error}
      end
    else
      Broker.search_offset_by_timestamp(broker, %SearchOffset{
        topic: topic,
        queue_id: queue_id,
        timestamp: consume_timestamp
      })
      |> case do
        {:ok, offset} -> {:ok, offset}
        _ -> {:error, :search_offset_by_timestamp_error}
      end
    end
  end

  @spec build_pullmsg_sys_flag(boolean(), String.t(), boolean()) :: non_neg_integer()
  defp build_pullmsg_sys_flag(commit_offset_enable, sub_expression, class_filter_mode) do
    0
    |> Util.BitHelper.set_bit(0, commit_offset_enable)
    |> Bitwise.band(2)
    |> Util.BitHelper.set_bit(2, sub_expression != "")
    |> Util.BitHelper.set_bit(3, class_filter_mode)
  end

  @spec consume_msgs_concurrently(
          list(Models.MessageExt.t()),
          non_neg_integer(),
          Typespecs.topic(),
          any()
        ) ::
          any()
  defp consume_msgs_concurrently(message_exts, batch, topic, processor) do
    message_exts
    |> Enum.chunk_every(batch)
    |> Enum.map(fn msgs ->
      Task.async(fn -> Processor.process(processor, topic, msgs) end)
    end)
    |> Task.await_many()
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
      Task.async(fn -> Broker.heartbeat(pid, heartbeat_data) end)
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
end
