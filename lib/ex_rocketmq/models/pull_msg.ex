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

    defstruct [
      :next_begin_offset,
      :min_offset,
      :max_offset,
      :status,
      :suggest_which_broker_id,
      :body
    ]
  end
end
