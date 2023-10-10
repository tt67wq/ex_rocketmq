defmodule ExRocketmq.InnerConsumer.Common do
  @moduledoc """
  some funcs used by both orderly and concurrently consumer
  """
  alias ExRocketmq.{Typespecs, Broker, Util}

  alias ExRocketmq.Models.{
    QueryConsumerOffset,
    GetMaxOffset,
    SearchOffset,
    ConsumeState,
    ConsumerSendMsgBack,
    BrokerData
  }

  require Logger

  @spec get_next_offset(
          pid(),
          Typespecs.group_name(),
          Typespecs.topic(),
          non_neg_integer(),
          Typespecs.consume_from_where(),
          non_neg_integer()
        ) ::
          {:ok, non_neg_integer()} | Typespecs.error_t()
  def get_next_offset(broker, group_name, topic, queue_id, consume_from_where, consume_timestamp) do
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
  def build_pullmsg_sys_flag(commit_offset_enable, subscription, class_filter_mode) do
    0
    |> Util.BitHelper.set_bit(0, commit_offset_enable)
    |> Bitwise.bor(2)
    |> Util.BitHelper.set_bit(2, subscription)
    |> Util.BitHelper.set_bit(3, class_filter_mode)
  end

  @spec send_msgs_back(list(MessageExt.t()), ConsumeState.t()) :: :ok
  def send_msgs_back([], _), do: :ok

  def send_msgs_back(
        msgs,
        %ConsumeState{
          group_name: group_name,
          broker_data: bd,
          registry: registry,
          broker_dynamic_supervisor: dynamic_supervisor,
          max_reconsume_times: max_reconsume_times
        } = pt
      ) do
    broker =
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.master_addr(bd),
        registry,
        dynamic_supervisor
      )

    msgs
    |> Task.async_stream(fn msg ->
      Logger.debug("send msg back: #{msg.message.topic}-#{msg.commit_log_offset}")

      Broker.consumer_send_msg_back(broker, %ConsumerSendMsgBack{
        group: group_name,
        offset: msg.commit_log_offset,
        delay_level: msg.delay_level,
        origin_msg_id: msg.msg_id,
        origin_topic: msg.message.topic,
        unit_mode: false,
        max_reconsume_times: max_reconsume_times
      })
      |> case do
        :ok ->
          nil

        _ ->
          msg
      end
    end)
    |> Enum.map(fn {_, val} -> val end)
    |> Enum.reject(&is_nil(&1))
    |> tap(fn msgs ->
      if length(msgs) > 0 do
        Logger.warning("send msgs back failed: #{inspect(msgs)}")
        Process.sleep(5000)
      end
    end)
    |> send_msgs_back(pt)
  end

  def async_send_msgs_back(msgs, pt) do
    Task.start(fn ->
      send_msgs_back(msgs, pt)
    end)
  end
end
