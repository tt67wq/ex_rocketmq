defmodule ExRocketmq.InnerConsumer.Concurrent do
  @moduledoc """
  concurrently mq consumer
  """

  alias ExRocketmq.{
    Typespecs,
    Broker,
    Protocol.PullStatus,
    Util,
    Consumer.Processor
  }

  alias ExRocketmq.Models.{
    BrokerData,
    MessageQueue,
    Subscription,
    ConsumeState,
    QueryConsumerOffset,
    GetMaxOffset,
    SearchOffset,
    PullMsg,
    ConsumerSendMsgBack,
    MessageExt
  }

  require PullStatus
  require Logger

  @pull_status_found PullStatus.pull_found()
  @pull_status_no_new_msg PullStatus.pull_no_new_msg()
  @pull_status_no_matched_msg PullStatus.pull_no_matched_msg()

  def pull_msg(
        %ConsumeState{
          broker_data: bd,
          registry: registry,
          broker_dynamic_supervisor: dynamic_supervisor,
          group_name: group_name,
          topic: topic,
          mq: %MessageQueue{
            queue_id: queue_id
          },
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
        registry,
        dynamic_supervisor
      )
      |> get_next_offset(
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
          topic: topic,
          group_name: group_name,
          mq: %MessageQueue{
            queue_id: queue_id
          },
          broker_data: bd,
          consume_orderly: false,
          next_offset: next_offset,
          commit_offset_enable: commit_offset_enable,
          post_subscription_when_pull: post_subscription_when_pull,
          subscription: %Subscription{
            sub_string: sub_string,
            class_filter_mode: cfm,
            expression_type: expression_type
          },
          pull_batch_size: pull_batch_size,
          consume_batch_size: consume_batch_size,
          commit_offset: commit_offset,
          registry: registry,
          broker_dynamic_supervisor: dynamic_supervisor,
          processor: processor
        } = pt
      ) do
    pull_req = %PullMsg.Request{
      consumer_group: group_name,
      topic: topic,
      queue_id: queue_id,
      queue_offset: next_offset,
      max_msg_nums: pull_batch_size,
      sys_flag:
        build_pullmsg_sys_flag(
          commit_offset_enable,
          post_subscription_when_pull and cfm,
          cfm
        ),
      commit_offset: commit_offset,
      suspend_timeout_millis: 20_000,
      sub_expression: sub_string,
      expression_type: expression_type
    }

    Util.Debug.debug(pull_req)

    # broker =
    #   Broker.get_or_new_broker(
    #     bd.broker_name,
    #     BrokerData.master_addr(bd),
    #     registry,
    #     dynamic_supervisor
    #   )

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

    with {:ok,
          %PullMsg.Response{
            status: status,
            next_begin_offset: next_begin_offset,
            messages: message_exts
          }} <- Broker.pull_message(broker, pull_req) do
      case status do
        @pull_status_found ->
          consume_msgs_concurrently(
            message_exts,
            consume_batch_size,
            topic,
            processor,
            bd,
            group_name,
            registry,
            dynamic_supervisor
          )

          pull_msg(%{
            pt
            | next_offset: next_begin_offset,
              commit_offset: next_begin_offset,
              commit_offset_enable: true
          })

        @pull_status_no_new_msg ->
          Process.sleep(5000)
          pull_msg(pt)

        @pull_status_no_matched_msg ->
          Process.sleep(1000)
          pull_msg(pt)

        status ->
          Logger.critical("pull message error: #{inspect(status)}, terminate pull task")
          :stop
      end
    else
      {:error, reason} ->
        Logger.error("pull message error: #{inspect(reason)}")
        Process.sleep(1000)
        pull_msg(pt)

      other ->
        Logger.error("pull message error: #{inspect(other)}")
        Process.sleep(1000)
        pull_msg(pt)
    end
  end

  @spec get_next_offset(
          pid(),
          Typespecs.group_name(),
          Typespecs.topic(),
          non_neg_integer(),
          Typespecs.consume_from_where(),
          non_neg_integer()
        ) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  defp get_next_offset(broker, group_name, topic, queue_id, cfw, consume_timestamp) do
    with {:ok, last_offset} <-
           Broker.query_consumer_offset(broker, %QueryConsumerOffset{
             consumer_group: group_name,
             topic: topic,
             queue_id: queue_id
           }) do
      if last_offset > 0 do
        {:ok, last_offset}
      else
        Logger.warning("no offset record for mq #{topic}-#{queue_id}")
        # no offset record
        case cfw do
          :last_offset ->
            get_last_offset(topic, broker, queue_id)

          :first_offset ->
            {:ok, 0}

          :timestamp ->
            get_offset_by_timestamp(topic, broker, queue_id, consume_timestamp)
        end
      end
    end
  end

  @spec get_last_offset(Typespecs.topic(), pid(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  defp get_last_offset(topic, broker, queue_id) do
    if retry_topic?(topic) do
      {:ok, 0}
    else
      case Broker.get_max_offset(broker, %GetMaxOffset{
             topic: topic,
             queue_id: queue_id
           }) do
        {:ok, offset} -> {:ok, offset}
        _ -> {:error, :get_max_offset_error}
      end
    end
  end

  @spec get_offset_by_timestamp(Typespecs.topic(), pid(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  defp get_offset_by_timestamp(topic, broker, queue_id, consume_timestamp) do
    if retry_topic?(topic) do
      Broker.get_max_offset(broker, %GetMaxOffset{
        topic: topic,
        queue_id: queue_id
      })
      |> case do
        {:ok, offset} -> {:ok, offset}
        _ -> {:error, :get_max_offset_error}
      end
    else
      Broker.search_offset_by_timestamp(broker, %SearchOffset{
        topic: topic,
        queue_id: queue_id,
        timestamp: consume_timestamp
      })
      |> case do
        {:ok, offset} -> {:ok, offset}
        _ -> {:error, :search_offset_by_timestamp_error}
      end
    end
  end

  @spec retry_topic?(Typespecs.topic()) :: boolean()
  defp retry_topic?(topic), do: String.starts_with?(topic, "%RETRY%")

  @spec build_pullmsg_sys_flag(boolean(), boolean(), boolean()) :: non_neg_integer()
  defp build_pullmsg_sys_flag(commit_offset_enable, subscription, class_filter_mode) do
    0
    |> Util.BitHelper.set_bit(0, commit_offset_enable)
    |> Bitwise.bor(2)
    |> Util.BitHelper.set_bit(2, subscription)
    |> Util.BitHelper.set_bit(3, class_filter_mode)
  end

  @spec consume_msgs_concurrently(
          list(MessageExt.t()),
          non_neg_integer(),
          Typespecs.topic(),
          any(),
          BrokerData.t(),
          Typespecs.group_name(),
          atom(),
          pid()
        ) ::
          any()
  defp consume_msgs_concurrently(
         message_exts,
         batch,
         topic,
         processor,
         bd,
         group,
         registry,
         dynamic_supervisor
       ) do
    message_exts
    |> Enum.chunk_every(batch)
    |> Enum.map(fn msgs ->
      Task.async(fn ->
        Processor.process(processor, topic, msgs)
        |> case do
          :success ->
            :ok

          {:retry_later, delay_level_map} ->
            # send msg back
            msgs
            |> Enum.map(&%{&1 | delay_level: Map.get(delay_level_map, &1.msg_id, 1)})
            |> send_msgs_back(bd, group, registry, dynamic_supervisor)
        end
      end)
    end)
    |> Task.await_many()
  end

  @spec send_msgs_back(
          list(MessageExt.t()),
          BrokerData.t(),
          Typespecs.group_name(),
          atom(),
          pid()
        ) :: :ok
  defp send_msgs_back([], _bd, _group, _registry, _dynamic_supervisor), do: :ok

  defp send_msgs_back(msgs, bd, group, registry, dynamic_supervisor) do
    broker =
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.master_addr(bd),
        registry,
        dynamic_supervisor
      )

    msgs
    |> Enum.map(fn msg ->
      Task.async(fn ->
        Logger.info("send msg back: #{inspect(msg)}")

        Broker.consumer_send_msg_back(broker, %ConsumerSendMsgBack{
          group: group,
          offset: msg.commit_log_offset,
          delay_level: msg.delay_level,
          origin_msg_id: msg.msg_id,
          origin_topic: msg.message.topic,
          unit_mode: false,
          max_reconsume_times: 16
        })
        |> case do
          :ok ->
            nil

          {:error, reason} ->
            Logger.error("send msg back error: #{inspect(reason)}")
            msg
        end
      end)
    end)
    |> Task.await_many()
    |> Enum.filter(&(&1 != nil))
    |> send_msgs_back(bd, group, registry, dynamic_supervisor)
  end
end
