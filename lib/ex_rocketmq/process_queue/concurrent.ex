defmodule ExRocketmq.ProcessQueue.Concurrent do
  @moduledoc false

  alias ExRocketmq.{
    Util,
    ProcessQueue.State,
    ProcessQueue.Common,
    Consumer.BuffManager
  }

  alias ExRocketmq.Models.{
    MessageExt
  }

  def run(
        %State{
          topic: topic,
          queue_id: queue_id,
          buff_manager: buff_manager,
          buff: nil
        } = state
      ) do
    # get mq buff
    {buff, _, _} = BuffManager.get_or_new(buff_manager, topic, queue_id)
    run(%{state | buff: buff})
  end

  def run(
        %State{
          buff: buff,
          topic: topic,
          queue_id: queue_id,
          buff_manager: buff_manager
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
        :ok = consume_msgs_concurrently(msgs, state)

        %MessageExt{queue_offset: last_offset} =
          msgs |> Enum.max_by(& &1.queue_offset)

        BuffManager.update_offset(
          buff_manager,
          topic,
          queue_id,
          last_offset
        )

        run(state)
    end
  end

  @spec consume_msgs_concurrently(
          list(MessageExt.t()),
          State.t()
        ) :: :ok
  defp consume_msgs_concurrently(
         message_exts,
         %State{
           client_id: cid,
           topic: topic,
           consume_batch_size: consume_batch_size,
           group_name: group_name,
           processor: processor,
           trace_enable: trace_enable
         } = state
       ) do
    tracer =
      if trace_enable do
        :"Tracer.#{cid}"
      else
        nil
      end

    message_exts
    |> Enum.chunk_every(consume_batch_size)
    |> Task.async_stream(fn msgs ->
      Common.process_with_trace(tracer, processor, group_name, topic, msgs)
      |> case do
        :success ->
          :ok

        {:retry_later, delay_level_map} ->
          # send msg back
          msgs
          |> Enum.map(&%MessageExt{&1 | delay_level: Map.get(delay_level_map, &1.msg_id, 0)})
          |> Enum.reject(&(&1.delay_level == 0))
          |> Common.send_msgs_back(state)
      end
    end)
    |> Stream.run()
  end
end
