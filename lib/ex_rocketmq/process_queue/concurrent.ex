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
          rt: rt,
          ok_cnt: ok_cnt_o,
          failed_cnt: failed_cnt_o
        } = state
      ) do
    Stats.consume_report(
      :"Stats.#{cid}",
      mq,
      ok_cnt_o,
      failed_cnt_o,
      rt
    )

    buff
    |> Util.Buffer.take()
    |> case do
      [] ->
        # buffer is empty, suspend for a while
        Process.sleep(5000)
        run(state)

      msgs ->
        {cost, ok_cnt, failed_cnt} = consume_msgs_concurrently(msgs, state)

        %MessageExt{queue_offset: last_offset} =
          Enum.max_by(msgs, & &1.queue_offset)

        BuffManager.update_offset(
          buff_manager,
          mq,
          last_offset
        )

        run(%State{
          state
          | rt: rt + cost,
            ok_cnt: ok_cnt_o + ok_cnt,
            failed_cnt: failed_cnt_o + failed_cnt
        })
    end
  end

  @spec consume_msgs_concurrently(
          list(MessageExt.t()),
          State.t()
        ) :: {cost :: non_neg_integer(), ok_cnt :: non_neg_integer(), failed_cnt :: non_neg_integer()}
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

    {ok_cnt, failed_cnt} =
      message_exts
      |> Enum.chunk_every(consume_batch_size)
      |> Task.async_stream(fn msgs ->
        tracer
        |> Common.process_with_trace(processor, group_name, topic, msgs)
        |> case do
          :success ->
            {:success, Enum.count(msgs)}

          {:retry_later, delay_level_map} ->
            # send msg back
            msgs
            |> Enum.map(&%MessageExt{&1 | delay_level: Map.get(delay_level_map, &1.msg_id, 0)})
            |> Enum.reject(&(&1.delay_level == 0))
            |> Common.send_msgs_back(state)

            {:failed, Enum.count(msgs)}
        end
      end)
      |> Enum.reduce({0, 0}, fn
        {:ok, {:success, cnt}}, {ok, failed} -> {ok + cnt, failed}
        {:ok, {:failed, cnt}}, {ok, failed} -> {ok, failed + cnt}
      end)

    {System.system_time(:millisecond) - since, ok_cnt, failed_cnt}
  end
end
