defmodule ExRocketmq.Models.PullMsg do
  @moduledoc """
  request/response header model for pull message
  """

  defmodule Request do
    @moduledoc false

    alias ExRocketmq.{Remote.ExtFields}

    @behaviour ExtFields

    defstruct [
      :consumer_group,
      :topic,
      :queue_id,
      :queue_offset,
      :max_msg_nums,
      :sys_flag,
      :commit_offset,
      :suspend_timeout_millis,
      :sub_expression,
      :sub_version,
      :expression_type
    ]

    @type t :: %__MODULE__{
            consumer_group: String.t(),
            topic: String.t(),
            queue_id: non_neg_integer(),
            queue_offset: non_neg_integer(),
            max_msg_nums: non_neg_integer(),
            sys_flag: non_neg_integer(),
            commit_offset: non_neg_integer(),
            suspend_timeout_millis: non_neg_integer(),
            sub_expression: String.t(),
            sub_version: non_neg_integer(),
            expression_type: String.t()
          }

    @impl ExtFields
    def to_map(t) do
      %{
        "consumerGroup" => t.consumer_group,
        "topic" => t.topic,
        "queueId" => "#{t.queue_id}",
        "queueOffset" => "#{t.queue_offset}",
        "maxMsgNums" => "#{t.max_msg_nums}",
        "sysFlag" => "#{t.sys_flag}",
        "commitOffset" => "#{t.commit_offset}",
        "suspendTimeoutMillis" => "#{t.suspend_timeout_millis}",
        "subscription" => t.sub_expression,
        "subVersion" => "#{t.sub_version}",
        "expressionType" => "#{t.expression_type}"
      }
    end
  end

  defmodule Response do
    @moduledoc false

    alias ExRocketmq.{Remote.Packet, Protocol.Response, Protocol.PullStatus, Typespecs}

    require Packet
    require Response
    require PullStatus

    defstruct [
      :next_begin_offset,
      :min_offset,
      :max_offset,
      :status,
      :suggest_which_broker_id,
      :body
    ]

    @resp_success Response.resp_success()
    @resp_pull_not_found Response.resp_pull_not_found()
    @resp_pull_retry_immediately Response.resp_pull_retry_immediately()
    @resp_pull_offset_moved Response.resp_pull_offset_moved()

    @pull_found PullStatus.pull_found()
    @pull_no_new_msg PullStatus.pull_no_new_msg()
    @pull_no_matched_msg PullStatus.pull_no_matched_msg()
    @pull_offset_illegal PullStatus.pull_offset_illegal()

    @type t :: %__MODULE__{
            next_begin_offset: non_neg_integer(),
            min_offset: non_neg_integer(),
            max_offset: non_neg_integer(),
            status: non_neg_integer(),
            suggest_which_broker_id: non_neg_integer(),
            body: binary()
          }

    @spec from_pkt(Packet.t()) :: t()
    def from_pkt(pkt) do
      status =
        case Packet.packet(pkt, :code) do
          @resp_success -> @pull_found
          @resp_pull_not_found -> @pull_no_new_msg
          @resp_pull_retry_immediately -> @pull_no_matched_msg
          @resp_pull_offset_moved -> @pull_offset_illegal
          other_code -> raise "unknown response code: #{other_code}"
        end

      ext_fields = Packet.packet(pkt, :ext_fields)

      %__MODULE__{
        next_begin_offset: get_int_ext_field(ext_fields, "nextBeginOffset"),
        min_offset: get_int_ext_field(ext_fields, "minOffset"),
        max_offset: get_int_ext_field(ext_fields, "maxOffset"),
        status: status,
        suggest_which_broker_id: get_int_ext_field(ext_fields, "suggestWhichBrokerId"),
        body: Packet.packet(pkt, :body)
      }
    end

    @spec get_int_ext_field(Typespecs.ext_fields(), String.t()) :: non_neg_integer()
    defp get_int_ext_field(ext_fields, key) do
      case ext_fields do
        %{^key => value} -> String.to_integer(value)
        _ -> 0
      end
    end
  end
end
