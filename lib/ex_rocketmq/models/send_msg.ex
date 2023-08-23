defmodule ExRocketmq.Models.SendMsg do
  @moduledoc """
  request/response header model for send message
  """

  defmodule Request do
    @moduledoc false

    alias ExRocketmq.{Remote.ExtFields}

    @behaviour ExtFields

    @type t :: %__MODULE__{
            producer_group: String.t(),
            topic: String.t(),
            queue_id: non_neg_integer(),
            sys_flag: non_neg_integer(),
            born_timestamp: non_neg_integer(),
            flag: non_neg_integer(),
            properties: String.t(),
            reconsume_times: non_neg_integer(),
            unit_mode: boolean(),
            max_reconsume_times: non_neg_integer(),
            batch: boolean(),
            default_topic: String.t(),
            default_topic_queue_num: non_neg_integer()
          }

    defstruct [
      :producer_group,
      :topic,
      :queue_id,
      :sys_flag,
      :born_timestamp,
      :flag,
      :properties,
      :reconsume_times,
      :unit_mode,
      :max_reconsume_times,
      :batch,
      :default_topic,
      :default_topic_queue_num
    ]

    @compress_flag 0x1
    @transaction_prepared_flag 0x100

    @spec set_compress_flag(non_neg_integer()) :: non_neg_integer()
    def set_compress_flag(flag) do
      flag
      |> Bitwise.|||(@compress_flag)
    end

    @spec set_transaction_prepared_flag(non_neg_integer()) :: non_neg_integer()
    def set_transaction_prepared_flag(flag) do
      flag
      |> Bitwise.|||(@transaction_prepared_flag)
    end

    @impl ExtFields
    def to_map(%{batch: true} = t) do
      %{
        "a" => t.producer_group,
        "b" => t.topic,
        "c" => t.default_topic,
        "d" => "#{t.default_topic_queue_num}",
        "e" => "#{t.queue_id}",
        "f" => "#{t.sys_flag}",
        "g" => "#{t.born_timestamp}",
        "h" => "#{t.flag}",
        "i" => t.properties,
        "j" => "#{t.reconsume_times}",
        "k" => "#{t.unit_mode}",
        "l" => "#{t.max_reconsume_times}",
        "m" => "true"
      }
    end

    def to_map(t) do
      %{
        "producerGroup" => t.producer_group,
        "topic" => t.topic,
        "queueId" => "#{t.queue_id}",
        "sysFlag" => "#{t.sys_flag}",
        "bornTimestamp" => "#{t.born_timestamp}",
        "flag" => "#{t.flag}",
        "reconsumeTimes" => "#{t.reconsume_times}",
        "unitMode" => "#{t.unit_mode}",
        "maxReconsumeTimes" => "#{t.max_reconsume_times}",
        "defaultTopic" => t.default_topic,
        "defaultTopicQueueNums" => "#{t.default_topic_queue_num}",
        "batch" => "false",
        "properties" => t.properties
      }
    end
  end

  defmodule Response do
    @moduledoc false

    alias ExRocketmq.{Remote.Packet, Protocol, Typespecs}

    require Protocol.Response
    require Protocol.SendStatus
    require Packet

    @type t :: %__MODULE__{
            status: non_neg_integer(),
            msg_id: String.t(),
            queue: ExRocketmq.Models.MessageQueue.t(),
            queue_offset: non_neg_integer(),
            transaction_id: String.t(),
            offset_msg_id: String.t(),
            region_id: String.t(),
            trace_on: boolean()
          }

    defstruct [
      :status,
      :msg_id,
      :queue,
      :queue_offset,
      :transaction_id,
      :offset_msg_id,
      :region_id,
      :trace_on
    ]

    @send_result_map %{
      Protocol.Response.res_flush_disk_timeout() => Protocol.SendStatus.send_flush_disk_timeout(),
      Protocol.Response.res_flush_slave_timeout() =>
        Protocol.SendStatus.send_flush_slave_timeout(),
      Protocol.Response.res_slave_not_available() =>
        Protocol.SendStatus.send_slave_not_available(),
      Protocol.Response.resp_success() => Protocol.SendStatus.send_ok()
    }

    @spec from_pkt(Packet.t()) :: {:ok, t()} | Typespecs.error_t()
    def from_pkt(pkt) do
      with {:ok, status} <- get_status(pkt),
           ext_fields <- Packet.packet(pkt, :ext_fields),
           region_id <- Map.get(ext_fields, "MSG_REGION", "DefaultRegion"),
           trace <- Map.get(ext_fields, "TRACE_ON", ""),
           msg_id <- Map.get(ext_fields, "msgId", ""),
           queue_id <- Map.get(ext_fields, "queueId", "0") |> String.to_integer(),
           offset <- Map.get(ext_fields, "queueOffset") |> String.to_integer() do
        {:ok,
         %__MODULE__{
           status: status,
           msg_id: msg_id,
           queue: %ExRocketmq.Models.MessageQueue{
             # topic and broker_name will be set later
             topic: "",
             broker_name: "",
             queue_id: queue_id
           },
           queue_offset: offset,
           transaction_id: Map.get(ext_fields, "transactionId", ""),
           offset_msg_id: Map.get(ext_fields, "msgId", ""),
           region_id: region_id,
           trace_on: trace not in ["false", ""]
         }}
      end
    end

    @spec get_status(Packet.t()) :: {:ok, non_neg_integer()} | Typespecs.error_t()
    defp get_status(pkt) do
      @send_result_map
      |> Map.get(Packet.packet(pkt, :code))
      |> case do
        nil ->
          {:error,
           %{"code" => Packet.packet(pkt, :code), "remark" => Packet.packet(pkt, :remark)}}

        status ->
          {:ok, status}
      end
    end
  end
end
