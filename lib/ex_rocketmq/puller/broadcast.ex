defmodule ExRocketmq.Puller.Broadcast do
  @moduledoc false

  alias ExRocketmq.Broker
  alias ExRocketmq.Consumer.BuffManager
  alias ExRocketmq.Models.BrokerData
  alias ExRocketmq.Puller.Common
  alias ExRocketmq.Puller.State
  alias ExRocketmq.Stats
  alias ExRocketmq.Util

  require Logger

  def run(%State{next_offset: -1} = state) do
    # get remote offset
    {:ok, offset} = Common.get_next_offset(state)
    run(%{state | next_offset: offset})
  end

  def run(%State{client_id: cid, mq: mq, broker_data: bd, holding_msgs: [], pull_cnt: pull_cnt, rt: rt} = state) do
    # report
    Stats.puller_report(:"Stats.#{cid}", mq, false, 0, pull_cnt, rt)

    # no need to commit offset for broadcast puller
    req = Common.new_pull_request(state, 0, false)

    broker =
      bd
      |> BrokerData.slave_addr()
      |> then(fn addr ->
        Broker.get_or_new_broker(
          bd.broker_name,
          addr,
          :"Registry.#{cid}",
          :"DynamicSupervisor.#{cid}"
        )
      end)

    broker
    |> Common.pull_from_broker(req, state)
    |> case do
      {[], _, cost} ->
        # pull failed or no new msgs, suspend for a while
        Process.sleep(5000)
        run(%State{state | rt: rt + cost})

      {msgs, next_offset, cost} ->
        run(%State{
          state
          | holding_msgs: msgs,
            next_offset: next_offset,
            pull_cnt: pull_cnt + Enum.count(msgs),
            rt: rt + cost
        })
    end
  end

  def run(%State{mq: mq, holding_msgs: msgs, buff_manager: buff_manager} = state) do
    {buff, _, _} = BuffManager.get_or_new(buff_manager, mq)

    buff
    |> Util.Buffer.put(msgs)
    |> case do
      :ok ->
        run(%State{state | holding_msgs: []})

      _ ->
        # buffer is full, suspend for a while
        Logger.warning("process queue is busy, buffer is full, suspend for a while")
        Process.sleep(1000)
        run(state)
    end
  end
end
