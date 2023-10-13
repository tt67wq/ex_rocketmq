defmodule ExRocketmq.InnerConsumer.Order do
  @moduledoc """
  orderly mq consumer
  """

  alias ExRocketmq.{
    Broker,
    Protocol.PullStatus,
    InnerConsumer.Common
  }

  alias ExRocketmq.Models.{
    ConsumeState,
    MessageQueue,
    BrokerData,
    Lock,
    Subscription,
    PullMsg,
    MessageExt
  }

  require Logger
  require PullStatus

  @pull_status_found PullStatus.pull_found()
  @pull_status_no_new_msg PullStatus.pull_no_new_msg()
  @pull_status_no_matched_msg PullStatus.pull_no_matched_msg()

  def pull_msg(
        %ConsumeState{
          lock_ttl_ms: ttl,
          broker_data: bd,
          registry: registry,
          broker_dynamic_supervisor: dynamic_supervisor,
          group_name: group_name,
          client_id: client_id,
          mq: mq
        } = task
      )
      when ttl <= 0 do
    # lock expired, request lock again
    broker =
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.master_addr(bd),
        registry,
        dynamic_supervisor
      )

    req = %Lock.Req{
      consumer_group: group_name,
      client_id: client_id,
      mq: [mq]
    }

    Broker.lock_batch_mq(broker, req)
    |> case do
      {:ok, _} ->
        pull_msg(%{task | lock_ttl_ms: 30_000})

      {:error, reason} ->
        Logger.error("lock mq failed, reason: #{inspect(reason)}, retry later")
        Process.sleep(5000)
        pull_msg(task)
    end
  end

  def pull_msg(
        %ConsumeState{
          next_offset: -1,
          broker_data: bd,
          registry: registry,
          broker_dynamic_supervisor: dynamic_supervisor,
          group_name: group_name,
          topic: topic,
          mq: %MessageQueue{
            queue_id: queue_id
          },
          consume_from_where: cfw,
          consume_timestamp: consume_timestamp,
          lock_ttl_ms: ttl
        } = task
      ) do
    now = System.system_time(:millisecond)

    # get remote offset
    {:ok, offset} =
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.master_addr(bd),
        registry,
        dynamic_supervisor
      )
      |> Common.get_next_offset(
        group_name,
        topic,
        queue_id,
        cfw,
        consume_timestamp
      )

    cost = System.system_time(:millisecond) - now

    pull_msg(%{
      task
      | next_offset: offset,
        commit_offset: offset,
        commit_offset_enable: offset > 0,
        lock_ttl_ms: ttl - cost
    })
  end

  def pull_msg(
        %ConsumeState{
          group_name: group_name,
          topic: topic,
          mq: %MessageQueue{
            queue_id: queue_id
          },
          broker_data: bd,
          next_offset: next_offset,
          commit_offset_enable: commit_offset_enable,
          commit_offset: commit_offset,
          post_subscription_when_pull: post_subscription_when_pull,
          pull_batch_size: pull_batch_size,
          subscription: %Subscription{
            sub_string: sub_string,
            class_filter_mode: cfm,
            expression_type: expression_type
          },
          registry: registry,
          broker_dynamic_supervisor: dynamic_supervisor,
          lock_ttl_ms: ttl
        } = task
      ) do
    now = System.system_time(:millisecond)

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
          registry,
          dynamic_supervisor
        )
      end)

    pull_from_broker(broker, pull_req, task)
    |> case do
      {:ok, pt, delay} ->
        Process.sleep(delay)
        cost = System.system_time(:millisecond) - now
        pull_msg(%{pt | lock_ttl_ms: ttl - cost})

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
            consume_msgs_orderly(message_exts, pt)

            {:ok,
             %{
               pt
               | next_offset: next_begin_offset,
                 commit_offset: next_begin_offset,
                 commit_offset_enable: true
             }, 0}

          @pull_status_no_new_msg ->
            Logger.debug("no new msg for retry topic #{topic}, sleep 5s")

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

  defp consume_msgs_orderly(
         message_exts,
         %ConsumeState{consume_batch_size: consume_batch_size} = pt
       ) do
    message_exts
    |> Enum.sort_by(fn msg -> msg.queue_offset end)
    |> Enum.chunk_every(consume_batch_size)
    |> do_consume(pt)
  end

  @spec do_consume(
          list(list(MessageExt.t())),
          ConsumeState.t()
        ) :: :ok
  defp do_consume([], _), do: :ok

  defp do_consume(
         [msgs | tail],
         %ConsumeState{
           topic: topic,
           group_name: group_name,
           processor: processor,
           tracer: tracer,
           max_reconsume_times: max_reconsume_times
         } = pt
       ) do
    Common.process_with_trace(tracer, processor, group_name, topic, msgs)
    |> case do
      :success ->
        do_consume(tail, pt)

      {:suspend, delay, msg_ids} ->
        Process.sleep(delay)

        {to_retry, to_sendback} =
          msgs
          |> Enum.filter(fn msg -> msg.msg_id in msg_ids end)
          |> Enum.map(fn %MessageExt{reconsume_times: rt} = msg ->
            %MessageExt{msg | reconsume_times: rt + 1}
          end)
          |> Enum.split_with(fn %MessageExt{reconsume_times: rt} -> rt <= max_reconsume_times end)

        if length(to_sendback) > 0 do
          Common.send_msgs_back(to_sendback, pt)
        end

        if length(to_retry) > 0 do
          do_consume([to_retry | tail], pt)
        else
          do_consume(tail, pt)
        end
    end
  end
end
