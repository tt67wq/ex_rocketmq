defmodule ExRocketmq.ProcessQueue.Order do
  @moduledoc false
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
          mq: mq,
          buff_manager: buff_manager,
          buff: buff,
          round: round,
          rt: rt,
          failed_msg_cnt: failed_msg_cnt
        } = state
      ) do
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
        {cost, failed_cnt} = consume_msgs_orderly(msgs, state)

        %MessageExt{queue_offset: last_offset} =
          Enum.max_by(msgs, & &1.queue_offset)

        BuffManager.update_offset(
          buff_manager,
          mq,
          last_offset
        )

        run(%State{state | round: round + 1, rt: rt + cost, failed_msg_cnt: failed_msg_cnt + failed_cnt})
    end
  end

  @spec consume_msgs_orderly(
          list(MessageExt.t()),
          State.t()
        ) :: {cost :: non_neg_integer(), failed_cnt :: non_neg_integer()}
  defp consume_msgs_orderly(message_exts, %State{consume_batch_size: consume_batch_size} = state) do
    since = System.system_time(:millisecond)

    failed_cnt =
      message_exts
      |> Enum.sort_by(fn msg -> msg.queue_offset end)
      |> Enum.chunk_every(consume_batch_size)
      |> do_consume(state, 0)

    {System.system_time(:millisecond) - since, failed_cnt}
  end

  @spec do_consume(
          list(list(MessageExt.t())),
          State.t(),
          non_neg_integer()
        ) :: non_neg_integer()
  defp do_consume([], _, failed_cnt), do: failed_cnt

  defp do_consume(
         [msgs | tail],
         %State{
           client_id: cid,
           mq: %MessageQueue{topic: topic},
           group_name: group_name,
           processor: processor,
           trace_enable: trace_enable,
           max_reconsume_times: max_reconsume_times
         } = state,
         failed_cnt
       ) do
    tracer =
      if trace_enable do
        :"Tracer.#{cid}"
      end

    tracer
    |> Common.process_with_trace(processor, group_name, topic, msgs)
    |> case do
      :success ->
        do_consume(tail, state, failed_cnt)

      {:suspend, delay, msg_ids} ->
        Process.sleep(delay)

        {to_retry, to_sendback} =
          msgs
          |> Enum.filter(fn %MessageExt{msg_id: msg_id} -> msg_id in msg_ids end)
          |> Enum.map(fn %MessageExt{reconsume_times: rt} = msg ->
            %MessageExt{msg | reconsume_times: rt + 1}
          end)
          |> Enum.split_with(fn %MessageExt{reconsume_times: rt} -> rt <= max_reconsume_times end)

        if length(to_sendback) > 0 do
          Common.send_msgs_back(to_sendback, state)
        end

        if length(to_retry) > 0 do
          do_consume([to_retry | tail], state, failed_cnt + Enum.count(to_sendback))
        else
          do_consume(tail, state, failed_cnt + Enum.count(to_sendback))
        end
    end
  end
end
