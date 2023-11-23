defmodule ExRocketmq.ProcessQueue.Concurrent do
  @moduledoc """
  This module provides functions for concurrent message processing.

  This module contains the `run/1` function, which is used to process messages concurrently.
  """

  alias ExRocketmq.Consumer.BuffManager
  alias ExRocketmq.Models.MessageExt
  alias ExRocketmq.Models.MessageQueue
  alias ExRocketmq.ProcessQueue.Common
  alias ExRocketmq.ProcessQueue.State
  alias ExRocketmq.Stats
  alias ExRocketmq.Util

  def run(%State{mq: mq, buff_manager: buff_manager, buff: nil} = state) do
    # get mq buff
    {buff, _, _} = BuffManager.get_or_new(buff_manager, mq)
    run(%{state | buff: buff})
  end

  def run(
        %State{
          client_id: cid,
          buff: buff,
          mq: mq,
          buff_manager: buff_manager,
          round: round,
          rt: rt,
          failed_msg_cnt: failed_msg_cnt
        } = state
      ) do
    # report
    Stats.consume_report(
      :"Stats.#{cid}",
      mq,
      round,
      round,
      0,
      rt,
      failed_msg_cnt
    )

    buff
    |> Util.Buffer.take()
    |> case do
      [] ->
        # buffer is empty, suspend for a while
        Process.sleep(5000)
        run(state)

      msgs ->
        {cost, failed_cnt} = consume_msgs_concurrently(msgs, state)

        %MessageExt{queue_offset: last_offset} =
          Enum.max_by(msgs, & &1.queue_offset)

        BuffManager.update_offset(
          buff_manager,
          mq,
          last_offset
        )

        run(%State{
          state
          | round: round + 1,
            rt: rt + cost,
            failed_msg_cnt: failed_msg_cnt + failed_cnt
        })
    end
  end

  @spec consume_msgs_concurrently(
          list(MessageExt.t()),
          State.t()
        ) :: {cost :: non_neg_integer(), failed_cnt :: non_neg_integer()}
  defp consume_msgs_concurrently(
         message_exts,
         %State{
           client_id: cid,
           mq: %MessageQueue{topic: topic},
           consume_batch_size: consume_batch_size,
           group_name: group_name,
           processor: processor,
           trace_enable: trace_enable
         } = state
       ) do
    tracer =
      if trace_enable do
        :"Tracer.#{cid}"
      end

    since = System.system_time(:millisecond)

    failed_cnt =
      message_exts
      |> Enum.chunk_every(consume_batch_size)
      |> Task.async_stream(fn msgs ->
        tracer
        |> Common.process_with_trace(processor, group_name, topic, msgs)
        |> case do
          :success ->
            0

          {:retry_later, delay_level_map} ->
            # send msg back
            msgs
            |> Enum.map(&%MessageExt{&1 | delay_level: Map.get(delay_level_map, &1.msg_id, 0)})
            |> Enum.reject(&(&1.delay_level == 0))
            |> Common.send_msgs_back(state)

            Enum.count(msgs)
        end
      end)
      |> Enum.reduce(0, fn {:ok, x}, acc -> acc + x end)

    {System.system_time(:millisecond) - since, failed_cnt}
  end
end
