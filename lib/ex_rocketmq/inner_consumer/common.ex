defmodule ExRocketmq.InnerConsumer.Common do
  @moduledoc """
  some funcs used by both orderly and concurrently consumer
  """
  alias ExRocketmq.{Typespecs, Broker, Util}

  alias ExRocketmq.Models.{
    QueryConsumerOffset,
    GetMaxOffset,
    SearchOffset
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
  def get_next_offset(broker, group_name, topic, queue_id, cfw, consume_timestamp) do
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
  def build_pullmsg_sys_flag(commit_offset_enable, subscription, class_filter_mode) do
    0
    |> Util.BitHelper.set_bit(0, commit_offset_enable)
    |> Bitwise.bor(2)
    |> Util.BitHelper.set_bit(2, subscription)
    |> Util.BitHelper.set_bit(3, class_filter_mode)
  end
end
