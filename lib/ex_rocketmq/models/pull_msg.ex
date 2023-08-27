defmodule ExRocketmq.Models.PullMsg do
  @moduledoc """
  request/response header model for pull message
  """

  defmodule Request do
    @moduledoc false

    alias ExRocketmq.{Remote.ExtFields}

    @behaviour ExtFields

    defstruct consumer_group: "",
              topic: "",
              queue_id: 0,
              queue_offset: 0,
              max_msg_nums: 0,
              sys_flag: 0,
              commit_offset: 0,
              suspend_timeout_millis: 0,
              sub_expression: "",
              sub_version: 0,
              expression_type: ""

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

    alias ExRocketmq.{
      Remote.Packet,
      Protocol.Response,
      Protocol.PullStatus,
      Typespecs,
      Models.MessageExt
    }

    require Packet
    require Response
    require PullStatus

    defstruct next_begin_offset: 0,
              min_offset: 0,
              max_offset: 0,
              status: 0,
              suggest_which_broker_id: 0,
              body: "",
              messages: []

    @type t :: %__MODULE__{
            next_begin_offset: non_neg_integer(),
            min_offset: non_neg_integer(),
            max_offset: non_neg_integer(),
            status: non_neg_integer(),
            suggest_which_broker_id: non_neg_integer(),
            body: binary(),
            messages: [MessageExt.t()]
          }
    @status_code_map %{
      Response.resp_success() => PullStatus.pull_found(),
      Response.resp_pull_not_found() => PullStatus.pull_no_new_msg(),
      Response.resp_pull_retry_immediately() => PullStatus.pull_no_matched_msg(),
      Response.resp_pull_offset_moved() => PullStatus.pull_offset_illegal()
    }

    @spec from_pkt(Packet.t()) :: {:ok, t()} | Typespecs.error_t()
    def from_pkt(pkt) do
      with {:ok, status} <- get_status(pkt) do
        ext_fields = Packet.packet(pkt, :ext_fields)

        {:ok,
         %__MODULE__{
           next_begin_offset: get_int_ext_field(ext_fields, "nextBeginOffset"),
           min_offset: get_int_ext_field(ext_fields, "minOffset"),
           max_offset: get_int_ext_field(ext_fields, "maxOffset"),
           status: status,
           suggest_which_broker_id: get_int_ext_field(ext_fields, "suggestWhichBrokerId"),
           body: Packet.packet(pkt, :body),
           messages: MessageExt.decode_from_binary(Packet.packet(pkt, :body))
         }}
      end
    end

    @spec get_status(Packet.t()) :: {:ok, non_neg_integer()} | Typespecs.error_t()
    defp get_status(pkt) do
      @status_code_map
      |> Map.get(Packet.packet(pkt, :code))
      |> case do
        nil ->
          {:error, %{code: Packet.packet(pkt, :code), remark: Packet.packet(pkt, :remark)}}

        status ->
          {:ok, status}
      end
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
