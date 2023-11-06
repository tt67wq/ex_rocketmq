defmodule ExRocketmq.InnerConsumer.Concurrent do
  @moduledoc """
  This is an implementation of PullConsumer.

  Rocketmq recommends using PushConsumer, which essentially adds a buffer between
  pulling messages and processing messages to control the pulling speed
  through the water level of the buffer.

  Although PushConsumer can bring some performance improvement in consumption,
  its flow control mechanism and offset management become more complex.

  If you care about this consumption performance, consider using gen_stage as an intermediate buffer layer.
  """

  alias ExRocketmq.{
    # Typespecs,
    Broker,
    Protocol.PullStatus,
    InnerConsumer.Common
  }

  alias ExRocketmq.Models.{
    BrokerData,
    MessageQueue,
    Subscription,
    ConsumeState,
    PullMsg,
    MessageExt
  }

  require PullStatus
  require Logger

  @pull_status_found PullStatus.pull_found()
  @pull_status_no_new_msg PullStatus.pull_no_new_msg()
  @pull_status_no_matched_msg PullStatus.pull_no_matched_msg()

  def pull_msg(
        %ConsumeState{
          client_id: cid,
          broker_data: bd,
          group_name: group_name,
          topic: topic,
          mq: %MessageQueue{
            queue_id: queue_id
          },
          # if next_offset is -1, get remote offset first
          next_offset: -1,
          consume_from_where: cfw,
          consume_timestamp: consume_timestamp
        } = task
      ) do
    # get remote offset
    {:ok, offset} =
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.slave_addr(bd),
        :"Registry.#{cid}",
        :"DynamicSupervisor.#{cid}"
      )
      |> Common.get_next_offset(
        group_name,
        topic,
        queue_id,
        cfw,
        consume_timestamp
      )

    Logger.info("mq #{topic}-#{queue_id}'s next offset: #{inspect(offset)}")

    pull_msg(%{
      task
      | next_offset: offset,
        commit_offset: offset,
        commit_offset_enable: offset > 0
    })
  end

  def pull_msg(
        %ConsumeState{
          client_id: cid,
          topic: topic,
          group_name: group_name,
          mq: %MessageQueue{
            queue_id: queue_id
          },
          broker_data: bd,
          next_offset: next_offset,
          commit_offset_enable: commit_offset_enable,
          post_subscription_when_pull: post_subscription_when_pull,
          subscription: %Subscription{
            sub_string: sub_string,
            class_filter_mode: cfm,
            expression_type: expression_type
          },
          pull_batch_size: pull_batch_size,
          commit_offset: commit_offset
        } = pt
      ) do
    pull_req = %PullMsg.Request{
      consumer_group: group_name,
      topic: topic,
      queue_id: queue_id,
      queue_offset: next_offset,
      max_msg_nums: pull_batch_size,
      sys_flag:
        Common.build_pullmsg_sys_flag(
          commit_offset_enable,
          post_subscription_when_pull and cfm,
          cfm
        ),
      commit_offset: commit_offset,
      suspend_timeout_millis: 20_000,
      sub_expression: sub_string,
      expression_type: expression_type
    }

    broker =
      if commit_offset_enable do
        BrokerData.master_addr(bd)
      else
        BrokerData.slave_addr(bd)
      end
      |> then(fn addr ->
        Broker.get_or_new_broker(
          bd.broker_name,
          addr,
          :"Registry.#{cid}",
          :"DynamicSupervisor.#{cid}"
        )
      end)

    pull_from_broker(broker, pull_req, pt)
    |> case do
      {:ok, pt, delay} ->
        Process.sleep(delay)
        pull_msg(pt)

      :stop ->
        Logger.critical("pull task terminated, stop consumer")
    end
  end

  @spec pull_from_broker(
          pid(),
          PullMsg.Request.t(),
          ConsumeState.t()
        ) ::
          {:ok, ConsumeState.t(), non_neg_integer()} | :stop
  defp pull_from_broker(
         broker,
         req,
         %ConsumeState{
           topic: topic,
           mq: %MessageQueue{
             queue_id: queue_id
           }
         } = pt
       ) do
    Broker.pull_message(broker, req)
    |> case do
      {:ok,
       %PullMsg.Response{
         status: status,
         next_begin_offset: next_begin_offset,
         messages: message_exts
       }} ->
        case status do
          @pull_status_found ->
            consume_msgs_concurrently(message_exts, pt)

            {:ok,
             %{
               pt
               | next_offset: next_begin_offset,
                 commit_offset: next_begin_offset,
                 commit_offset_enable: true
             }, 0}

          @pull_status_no_new_msg ->
            Logger.debug("no new msg for topic #{topic}, sleep 5s")

            {:ok, pt, 5000}

          @pull_status_no_matched_msg ->
            {:ok, pt, 1000}

          status ->
            Logger.error("invalid pull message status result: #{inspect(status)}")
            :stop
        end

      {:error, reason} ->
        Logger.error(
          "pull message error: #{inspect(reason)}, topic: #{topic}, queue: #{queue_id}"
        )

        {:ok, pt, 1000}
    end
  end

  @spec consume_msgs_concurrently(
          list(MessageExt.t()),
          ConsumeState.t()
        ) :: any()
  defp consume_msgs_concurrently(
         message_exts,
         %ConsumeState{
           client_id: cid,
           topic: topic,
           consume_batch_size: consume_batch_size,
           group_name: group_name,
           processor: processor,
           trace_enable: trace_enable
         } = pt
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
          |> Enum.map(&%{&1 | delay_level: Map.get(delay_level_map, &1.msg_id, 1)})
          |> Common.send_msgs_back(pt)
      end
    end)
    |> Stream.run()
  end
end
