defmodule ExRocketmq.Puller.Common do
  @moduledoc false

  alias ExRocketmq.{
    Broker,
    Typespecs,
    Puller.State,
    Util,
    Protocol.PullStatus
  }

  alias ExRocketmq.Models.{
    QueryConsumerOffset,
    GetMaxOffset,
    SearchOffset,
    BrokerData,
    PullMsg,
    MessageExt,
    Subscription,
    MessageQueue
  }

  require Logger
  require PullStatus

  @pull_status_found PullStatus.pull_found()
  @pull_status_no_new_msg PullStatus.pull_no_new_msg()
  @pull_status_no_matched_msg PullStatus.pull_no_matched_msg()

  @spec get_next_offset(State.t()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  def get_next_offset(%State{
        client_id: cid,
        group_name: group_name,
        mq: %MessageQueue{topic: topic, queue_id: queue_id},
        broker_data: bd,
        consume_from_where: consume_from_where,
        consume_timestamp: consume_timestamp
      }) do
    broker =
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.slave_addr(bd),
        :"Registry.#{cid}",
        :"DynamicSupervisor.#{cid}"
      )

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
        case consume_from_where do
          :last_offset ->
            get_last_offset(broker, topic, queue_id)

          :first_offset ->
            {:ok, 0}

          :timestamp ->
            get_offset_by_timestamp(broker, topic, queue_id, consume_timestamp)
        end
      end
    end
  end

  @spec get_last_offset(pid(), Typespecs.topic(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  defp get_last_offset(broker, topic, queue_id) do
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

  @spec get_offset_by_timestamp(pid(), Typespecs.topic(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  defp get_offset_by_timestamp(broker, topic, queue_id, consume_timestamp) do
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

  @spec pull_from_broker(
          pid(),
          PullMsg.Request.t(),
          State.t()
        ) ::
          {[MessageExt.t()], non_neg_integer()}
  def pull_from_broker(
        broker,
        %PullMsg.Request{} = req,
        %State{
          mq: %MessageQueue{topic: topic, queue_id: queue_id}
        }
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
            {message_exts, next_begin_offset}

          @pull_status_no_new_msg ->
            {[], 0}

          @pull_status_no_matched_msg ->
            {[], 0}

          status ->
            Logger.error("invalid pull message status result: #{inspect(status)}")
            {[], 0}
        end

      {:error, reason} ->
        Logger.error(
          "pull message error: #{inspect(reason)}, topic: #{topic}, queue: #{queue_id}"
        )

        {[], 0}
    end
  end

  @spec new_pull_request(
          State.t(),
          non_neg_integer(),
          boolean()
        ) ::
          PullMsg.Request.t()
  def new_pull_request(
        %State{
          group_name: group_name,
          mq: %MessageQueue{topic: topic, queue_id: queue_id},
          next_offset: next_offset,
          pull_batch_size: pull_batch_size,
          post_subscription_when_pull: post_subscription_when_pull,
          subscription: %Subscription{
            sub_string: sub_string,
            class_filter_mode: cfm,
            expression_type: expression_type
          }
        },
        commit_offset,
        commit?
      ) do
    %PullMsg.Request{
      consumer_group: group_name,
      topic: topic,
      queue_id: queue_id,
      queue_offset: next_offset,
      max_msg_nums: pull_batch_size,
      sys_flag:
        build_pullmsg_sys_flag(
          commit?,
          post_subscription_when_pull and cfm,
          cfm
        ),
      commit_offset: commit_offset,
      suspend_timeout_millis: 20_000,
      sub_expression: sub_string,
      expression_type: expression_type
    }
  end
end
