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
            reply: boolean(),
            default_topic: String.t(),
            default_topic_queue_num: non_neg_integer()
          }

    defstruct producer_group: "",
              topic: "",
              queue_id: 0,
              sys_flag: 0,
              born_timestamp: 0,
              flag: 0,
              properties: "",
              reconsume_times: 0,
              unit_mode: false,
              max_reconsume_times: 0,
              batch: false,
              reply: false,
              default_topic: "",
              default_topic_queue_num: 0

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
            queue: ExRocketmq.Models.MessageQueue.t(),
            queue_offset: non_neg_integer(),
            transaction_id: String.t(),
            offset_msg_id: String.t(),
            region_id: String.t(),
            trace_on: boolean()
          }

    defstruct status: 0,
              queue: %ExRocketmq.Models.MessageQueue{},
              queue_offset: 0,
              transaction_id: "",
              offset_msg_id: "",
              region_id: "",
              trace_on: false

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
      get_status(pkt)
      |> case do
        {:ok, status} ->
          ext_fields = Packet.packet(pkt, :ext_fields)
          region_id = Map.get(ext_fields, "MSG_REGION", "DefaultRegion")
          trace = Map.get(ext_fields, "TRACE_ON", "")

          {:ok,
           %__MODULE__{
             status: status,
             queue: %ExRocketmq.Models.MessageQueue{
               # topic and broker_name will be set later
               topic: "",
               broker_name: "",
               queue_id: get_int_ext_field(ext_fields, "queueId")
             },
             queue_offset: get_int_ext_field(ext_fields, "queueOffset"),
             transaction_id: Map.get(ext_fields, "transactionId", ""),
             offset_msg_id: Map.get(ext_fields, "msgId", ""),
             region_id: region_id,
             trace_on: trace not in ["false", ""]
           }}

        error ->
          error
      end
    end

    @spec get_int_ext_field(Typespecs.ext_fields(), String.t()) :: non_neg_integer()
    defp get_int_ext_field(ext_fields, key) do
      case ext_fields do
        %{^key => value} -> String.to_integer(value)
        _ -> 0
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

defmodule ExRocketmq.Models.ConsumerSendMsgBack do
  @moduledoc """
  consumer send message back model
  """

  alias ExRocketmq.{Remote.ExtFields}

  @behaviour ExtFields

  defstruct [
    :group,
    :offset,
    :delay_level,
    :origin_msg_id,
    :origin_topic,
    :unit_mode,
    :max_reconsume_times
  ]

  @type t :: %__MODULE__{
          group: String.t(),
          offset: non_neg_integer(),
          delay_level: non_neg_integer(),
          origin_msg_id: String.t(),
          origin_topic: String.t(),
          unit_mode: boolean(),
          max_reconsume_times: non_neg_integer()
        }

  @impl ExtFields
  def to_map(t) do
    %{
      "group" => t.group,
      "offset" => "#{t.offset}",
      "delayLevel" => "#{t.delay_level}",
      "originMsgId" => t.origin_msg_id,
      "originTopic" => t.origin_topic,
      "unitMode" => "#{t.unit_mode}",
      "maxReconsumeTimes" => "#{t.max_reconsume_times}"
    }
  end
end
