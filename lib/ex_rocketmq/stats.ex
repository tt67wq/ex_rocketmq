defmodule ExRocketmq.Stats do
  @moduledoc """
  {mq, :pull} => {locked, last_lock_timestamp, last_pull_timestamp}
  {mq, :consume} => last_consumed_timestamp

  pull_point: {ts, round, cost}
  consume_point: {ts, round, ok_round, failed_round, cost, failed_cnt}

  topic => {begin_pull_point, end_pull_point}
  topic => {begin_consume_point, end_consume_point}
  """

  use GenServer

  alias ExRocketmq.Consumer.BuffManager
  alias ExRocketmq.Models.MessageQueue
  alias ExRocketmq.Models.ProcessInfo
  alias ExRocketmq.Models.RunningInfo
  alias ExRocketmq.Models.Stat
  alias ExRocketmq.Util.Buffer

  defmodule State do
    @moduledoc false

    alias ExRocketmq.Models.MessageQueue
    alias ExRocketmq.Typespecs

    defstruct client_id: "",
              topic_pull: %{},
              topic_consume: %{},
              mq_pull: %{},
              mq_consume: %{}

    @type pull_point :: {timestamp :: non_neg_integer(), pull_cnt :: non_neg_integer(), pull_rt :: non_neg_integer()}
    @type consume_point :: {
            timestamp :: non_neg_integer(),
            ok_cnt :: non_neg_integer(),
            failed_cnt :: non_neg_integer(),
            consume_rt :: non_neg_integer()
          }
    @type queue(type) :: {list(type), list(type)}
    @type mq_pull_point ::
            {locked :: boolean(), last_lock_timestamp :: non_neg_integer(), last_pull_timestamp :: non_neg_integer()}
    @type t :: %__MODULE__{
            topic_pull: %{Typespecs.topic() => queue(pull_point())},
            topic_consume: %{Typespecs.topic() => queue(consume_point())},
            mq_pull: %{MessageQueue.t() => mq_pull_point()},
            mq_consume: %{MessageQueue.t() => non_neg_integer()}
          }
  end

  defstruct client_id: "",
            mq_set: MapSet.new(),
            topic_set: MapSet.new()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec puller_report(atom(), MessageQueue.t(), boolean(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def puller_report(name, mq, locked, last_lock_timestamp, pull_cnt, pull_rt) do
    GenServer.cast(name, {:puller, mq, locked, last_lock_timestamp, pull_cnt, pull_rt})
  end

  @spec consume_report(
          atom(),
          MessageQueue.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def consume_report(name, mq, ok_cnt, failed_cnt, consume_rt) do
    GenServer.cast(name, {:consumer, mq, ok_cnt, failed_cnt, consume_rt})
  end

  @spec get_running_info(atom()) :: RunningInfo.t()
  def get_running_info(name) do
    GenServer.call(name, :get)
  end

  def init(opts) do
    cid = Keyword.fetch!(opts, :client_id)

    {:ok,
     %State{
       client_id: cid,
       topic_pull: %{},
       topic_consume: %{},
       mq_pull: %{},
       mq_consume: %{}
     }}
  end

  def handle_cast(
        {:puller, %MessageQueue{topic: topic} = mq, locked, last_lock_timestamp, pull_cnt, pull_rt},
        %State{mq_pull: mq_pull, topic_pull: topic_pull} = state
      ) do
    now_ts = System.system_time()
    mq_pull = Map.put(mq_pull, mq, {locked, last_lock_timestamp, now_ts})

    pull_point = {now_ts, pull_cnt, pull_rt}

    topic_pull =
      Map.update(topic_pull, topic, :queue.from_list([pull_point]), fn
        q -> :queue.in(pull_point, q)
      end)

    {:noreply, %State{state | mq_pull: mq_pull, topic_pull: topic_pull}}
  end

  def handle_cast(
        {:consumer, mq, ok_cnt, failed_cnt, consume_rt},
        %State{mq_consume: mq_consume, topic_consume: topic_consume} = state
      ) do
    now_ts = System.system_time()
    mq_consume = Map.put(mq_consume, mq, now_ts)
    consume_point = {now_ts, ok_cnt, failed_cnt, consume_rt}

    topic_consume =
      Map.update(topic_consume, mq.topic, :queue.from_list([consume_point]), fn
        q -> :queue.in(consume_point, q)
      end)

    {:noreply, %State{state | mq_consume: mq_consume, topic_consume: topic_consume}}
  end

  def handle_call(
        :get,
        _,
        %State{
          client_id: cid,
          topic_pull: topic_pull,
          topic_consume: topic_consume,
          mq_pull: mq_pull,
          mq_consume: mq_consume
        } = state
      ) do
    # mq_table
    mq_table = get_mq_table(mq_pull, mq_consume)
    # filling buffer stats
    mq_table = Map.new(mq_table, fn {mq, process_info} -> {mq, fill_buffer_stats(cid, mq, process_info)} end)
    # status_table
    status_table = get_status_table(topic_pull, topic_consume)

    {:reply, %RunningInfo{mq_table: mq_table, status_table: status_table},
     %State{state | mq_pull: %{}, mq_consume: %{}, topic_pull: %{}, topic_consume: %{}}}
  end

  defp get_mq_table(mq_pull, mq_consume) do
    mq_table =
      Enum.reduce(mq_pull, %{}, fn {mq, {lock, last_lock_timestamp, last_pull_timestamp}}, acc ->
        Map.update(
          acc,
          mq,
          %ProcessInfo{
            locked: lock,
            last_lock_timestamp: last_lock_timestamp,
            last_pull_timestamp: last_pull_timestamp
          },
          fn
            info ->
              %ProcessInfo{
                info
                | locked: lock,
                  last_lock_timestamp: last_lock_timestamp,
                  last_pull_timestamp: last_pull_timestamp
              }
          end
        )
      end)

    # consume
    Enum.reduce(mq_consume, mq_table, fn {mq, last_consumed_timestamp}, acc ->
      Map.update(
        acc,
        mq,
        %ProcessInfo{
          last_consumed_timestamp: last_consumed_timestamp
        },
        fn
          info ->
            %ProcessInfo{
              info
              | last_consumed_timestamp: last_consumed_timestamp
            }
        end
      )
    end)
  end

  defp get_status_table(topic_pull, topic_consume) do
    caculate_pull_stats = fn
      topic, q, acc ->
        {begin_ts, begin_pull_cnt, begin_pull_rt} = :queue.head(q)
        {end_ts, end_pull_cnt, end_pull_rt} = :queue.last(q)

        pull_rt = (end_pull_rt - begin_pull_rt) / :queue.len(q)
        pull_tps = (end_pull_cnt - begin_pull_cnt) * 1_000_000_000 / (end_ts - begin_ts)

        Map.update(
          acc,
          topic,
          %Stat{
            pull_rt: pull_rt,
            pull_tps: pull_tps
          },
          fn
            stat ->
              %Stat{
                stat
                | pull_rt: pull_rt,
                  pull_tps: pull_tps
              }
          end
        )
    end

    calculate_consume_stats = fn topic, q, acc ->
      {begin_ts, begin_ok_cnt, begin_failed_cnt, begin_consume_rt} = :queue.head(q)
      {end_ts, end_ok_cnt, end_failed_cnt, end_consume_rt} = :queue.last(q)

      consume_rt = (end_consume_rt - begin_consume_rt) / :queue.len(q)
      consume_ok_tps = (end_ok_cnt - begin_ok_cnt) * 1_000_000_000 / (end_ts - begin_ts)
      consume_failed_tps = (end_failed_cnt - begin_failed_cnt) * 1_000_000_000 / (end_ts - begin_ts)

      Map.update(
        acc,
        topic,
        %Stat{
          consume_rt: consume_rt,
          consume_ok_tps: consume_ok_tps,
          consume_failed_tps: consume_failed_tps
        },
        fn
          stat ->
            %Stat{
              stat
              | consume_rt: consume_rt,
                consume_ok_tps: consume_ok_tps,
                consume_failed_tps: consume_failed_tps
            }
        end
      )
    end

    status_table =
      Enum.reduce(topic_pull, %{}, fn
        {topic, q}, acc ->
          case :queue.len(q) do
            0 ->
              acc

            1 ->
              acc

            _ ->
              caculate_pull_stats.(topic, q, acc)
          end
      end)

    Enum.reduce(topic_consume, status_table, fn
      {topic, q}, acc ->
        case :queue.len(q) do
          0 ->
            acc

          1 ->
            acc

          _ ->
            calculate_consume_stats.(topic, q, acc)
        end
    end)
  end

  defp fill_buffer_stats(cid, mq, process_info) do
    case BuffManager.get(:"BuffManager.#{cid}", mq) do
      {buff, _, _} ->
        head = Buffer.first(buff)
        tail = Buffer.last(buff)

        %ProcessInfo{
          process_info
          | cached_msg_min_offset: (is_nil(head) && 0) || head.queue_offset,
            cached_msg_max_offset: (is_nil(tail) && 0) || tail.queue_offset,
            cached_msg_count: Buffer.size(buff)
        }

      _ ->
        process_info
    end
  end
end
