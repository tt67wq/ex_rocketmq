defmodule ExRocketmq.Stats do
  @moduledoc """
  {mq, :pull} => {locked, last_lock_timestamp, last_pull_timestamp}
  {mq, :consume} => last_consumed_timestamp

  pull_point: {ts, round, cost}
  consume_point: {ts, round, ok_round, failed_round, cost, failed_cnt}

  topic => {begin_pull_point, end_pull_point}
  topic => {begin_consume_point, end_consume_point}
  """

  use Agent

  alias ExRocketmq.Models.MessageQueue

  defstruct []

  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop!(opts, :name)
    Agent.start_link(__MODULE__, :init, [name, opts], name: name)
  end

  def puller_report(name, %MessageQueue{topic: topic} = mq, locked, last_lock_timestamp, round, pull_rt) do
    table = :"#{name}_stats"
    last_pull_timestamp = System.system_time(:millisecond)

    :ets.insert(table, {{mq, :pull}, {locked, last_lock_timestamp, last_pull_timestamp}})

    :ets.insert_new(table, {{topic, :pull, :begin}, {last_pull_timestamp, round, pull_rt}})
    :ets.insert(table, {{topic, :pull, :end}, {last_pull_timestamp, round, pull_rt}})
  end

  def consume_report(name, %MessageQueue{topic: topic} = mq, round, ok_round, failed_round, consume_rt, failed_msg_cnt) do
    table = :"#{name}_stats"
    last_consumed_timestamp = System.system_time(:millisecond)

    :ets.insert(table, {{mq, :consume}, last_consumed_timestamp})

    :ets.insert_new(
      table,
      {{topic, :consume, :begin}, {last_consumed_timestamp, round, ok_round, failed_round, consume_rt, failed_msg_cnt}}
    )

    :ets.insert(
      table,
      {{topic, :consume, :end}, {last_consumed_timestamp, round, ok_round, failed_round, consume_rt, failed_msg_cnt}}
    )
  end

  def init(name, _opts) do
    :ets.new(:"#{name}_stats", [:named_table, :set, {:write_concurrency, true}, :public])
    %__MODULE__{}
  end
end
