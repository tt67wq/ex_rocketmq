defmodule ExRocketmq.Puller.Normal do
  @moduledoc """
  This module contains the implementation of the normal puller for RocketMQ.
  It handles pulling messages from the broker and processing them.

  The `run/1` function is the entry point for the puller. It retrieves the next offset
  and continues running.

  Note: This module assumes the presence of other modules and structs from the
  `ExRocketmq` and `ExRocketmq.Models` namespaces.

  For more information, refer to the RocketMQ documentation.
  """

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

  def run(
        %State{
          client_id: cid,
          mq: mq,
          buff_manager: buff_manager,
          broker_data: bd,
          holding_msgs: [],
          round: round,
          rt: rt
        } = state
      ) do
    # report first
    Stats.puller_report(:"Stats.#{cid}", mq, false, 0, round, rt)

    {buff, commit_offset, commit?} = BuffManager.get_or_new(buff_manager, mq)
    req = Common.new_pull_request(state, commit_offset, commit?)

    if_result =
      if commit? do
        BrokerData.master_addr(bd)
      else
        BrokerData.slave_addr(bd)
      end

    broker =
      then(if_result, fn addr ->
        Broker.get_or_new_broker(bd.broker_name, addr, :"Registry.#{cid}", :"DynamicSupervisor.#{cid}")
      end)

    broker
    |> Common.pull_from_broker(req, state)
    |> case do
      {[], _, cost} ->
        # pull failed or no new msgs, suspend for a while
        Process.sleep(5000)
        run(%State{state | round: round + 1, rt: rt + cost})

      {msgs, next_offset, cost} ->
        run(%State{state | holding_msgs: msgs, next_offset: next_offset, buff: buff, round: round + 1, rt: rt + cost})
    end
  end

  def run(%State{mq: mq, holding_msgs: msgs, buff_manager: buff_manager, buff: buff} = state) do
    buff =
      buff
      |> is_nil()
      |> if do
        {buff, _, _} = BuffManager.get_or_new(buff_manager, mq)
        buff
      else
        buff
      end

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
