defmodule ExRocketmq.ProcessQueue.Order do
  @moduledoc false
  alias ExRocketmq.{
    Util,
    ProcessQueue.State,
    ProcessQueue.Common,
    Consumer.BuffManager
  }

  alias ExRocketmq.Models.{
    MessageExt,
    MessageQueue
  }

  def run(
        %State{
          mq: mq,
          buff_manager: buff_manager,
          buff: nil
        } = state
      ) do
    # get mq buff
    {buff, _, _} = BuffManager.get_or_new(buff_manager, mq)
    run(%{state | buff: buff})
  end

  def run(
        %State{
          mq: mq,
          buff_manager: buff_manager,
          buff: buff
        } = state
      ) do
    buff
    |> Util.Buffer.take()
    |> case do
      [] ->
        # buffer is empty, suspend for a while
        Process.sleep(5000)
        run(state)

      msgs ->
        consume_msgs_orderly(msgs, state)

        %MessageExt{queue_offset: last_offset} =
          msgs |> Enum.max_by(& &1.queue_offset)

        BuffManager.update_offset(
          buff_manager,
          mq,
          last_offset
        )

        run(state)
    end
  end

  @spec consume_msgs_orderly(
          list(MessageExt.t()),
          State.t()
        ) :: :ok
  defp consume_msgs_orderly(
         message_exts,
         %State{
           consume_batch_size: consume_batch_size
         } = state
       ) do
    message_exts
    |> Enum.sort_by(fn msg -> msg.queue_offset end)
    |> Enum.chunk_every(consume_batch_size)
    |> do_consume(state)
  end

  @spec do_consume(
          list(list(MessageExt.t())),
          State.t()
        ) :: :ok
  defp do_consume([], _), do: :ok

  defp do_consume(
         [msgs | tail],
         %State{
           client_id: cid,
           mq: %MessageQueue{topic: topic},
           group_name: group_name,
           processor: processor,
           trace_enable: trace_enable,
           max_reconsume_times: max_reconsume_times
         } = state
       ) do
    tracer =
      if trace_enable do
        :"Tracer.#{cid}"
      else
        nil
      end

    Common.process_with_trace(tracer, processor, group_name, topic, msgs)
    |> case do
      :success ->
        do_consume(tail, state)

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
          do_consume([to_retry | tail], state)
        else
          do_consume(tail, state)
        end
    end
  end
end
