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

  alias ExRocketmq.Models.MessageQueue

  defmodule State do
    @moduledoc false

    alias ExRocketmq.Models.MessageQueue
    alias ExRocketmq.Typespecs

    defstruct topic_pull: %{},
              topic_consume: %{},
              mq_pull: %{},
              mq_consume: %{}

    @type pull_point :: {timestamp :: non_neg_integer(), round :: non_neg_integer(), pull_rt :: non_neg_integer()}
    @type consume_point :: {
            timestamp :: non_neg_integer(),
            round :: non_neg_integer(),
            ok_round :: non_neg_integer(),
            failed_round :: non_neg_integer(),
            consume_rt :: non_neg_integer(),
            failed_cnt :: non_neg_integer()
          }
    @type mq_pull_point ::
            {locked :: boolean(), last_lock_timestamp :: non_neg_integer(), last_pull_timestamp :: non_neg_integer()}
    @type t :: %__MODULE__{
            topic_pull: %{Typespecs.topic() => {pull_point(), pull_point()}},
            topic_consume: %{Typespecs.topic() => {consume_point(), consume_point()}},
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
    GenServer.start_link(__MODULE__, {name, opts}, name: name)
  end

  @spec puller_report(atom(), MessageQueue.t(), boolean(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def puller_report(name, mq, locked, last_lock_timestamp, round, pull_rt) do
    GenServer.cast(name, {:puller, mq, locked, last_lock_timestamp, round, pull_rt})
  end

  @spec consume_report(
          atom(),
          MessageQueue.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def consume_report(name, mq, round, ok_round, failed_round, consume_rt, failed_msg_cnt) do
    GenServer.cast(name, {:consumer, mq, round, ok_round, failed_round, consume_rt, failed_msg_cnt})
  end

  def init(_) do
    {:ok, %State{}}
  end

  def handle_cast(
        {:puller, %MessageQueue{topic: topic} = mq, locked, last_lock_timestamp, round, pull_rt},
        %State{mq_pull: mq_pull, topic_pull: topic_pull} = state
      ) do
    now_ts = System.system_time(:millisecond)
    mq_pull = Map.put(mq_pull, mq, {locked, last_lock_timestamp, now_ts})

    pull_point = {now_ts, round, pull_rt}

    topic_pull =
      Map.update(topic_pull, topic, {pull_point, pull_point}, fn
        {begin_pull_point, _} ->
          {begin_pull_point, pull_point}
      end)

    {:noreply, %State{state | mq_pull: mq_pull, topic_pull: topic_pull}}
  end

  def handle_cast(
        {:consumer, mq, round, ok_round, failed_round, consume_rt, failed_msg_cnt},
        %State{mq_consume: mq_consume, topic_consume: topic_consume} = state
      ) do
    now_ts = System.system_time(:millisecond)
    mq_consume = Map.put(mq_consume, mq, now_ts)
    consume_point = {now_ts, round, ok_round, failed_round, consume_rt, failed_msg_cnt}

    topic_consume =
      Map.update(topic_consume, mq.topic, {consume_point, consume_point}, fn
        {begin_consume_point, _} ->
          {begin_consume_point, consume_point}
      end)

    {:noreply, %State{state | mq_consume: mq_consume, topic_consume: topic_consume}}
  end
end
