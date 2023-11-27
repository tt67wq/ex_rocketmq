defmodule ExRocketmq.Models.RunningInfo do
  @moduledoc false

  alias ExRocketmq.Models.MessageQueue
  alias ExRocketmq.Models.ProcessInfo
  alias ExRocketmq.Models.Stat
  alias ExRocketmq.Models.Subscription
  alias ExRocketmq.Typespecs

  defstruct properties: %{},
            subscriptions: [],
            mq_table: %{},
            status_table: %{}

  @type t :: %__MODULE__{
          properties: Typespecs.str_dict(),
          subscriptions: [Subscription.t()],
          mq_table: %{MessageQueue.t() => ProcessInfo.t()},
          status_table: %{Typespecs.topic() => Stat.t()}
        }

  @spec encode(t()) :: binary()
  def encode(%__MODULE__{
        properties: properties,
        subscriptions: subscriptions,
        mq_table: mq_table,
        status_table: status_table
      }) do
    properties = "\"properties\":#{Jason.encode!(properties)}"

    status_table =
      Enum.map_join(status_table, ",", fn {topic, stat} -> "\"#{topic}\":#{Jason.encode!(Stat.to_map(stat))}" end)

    status_table = "\"statusTable\":{#{status_table}}"

    subs = Enum.map_join(subscriptions, ",", fn sub -> Jason.encode!(Subscription.to_map(sub)) end)
    subs = "\"subscriptionSet\":[#{subs}]"

    mq_table =
      Enum.map_join(mq_table, ",", fn {mq, process_info} ->
        "#{Jason.encode!(MessageQueue.to_map(mq))}:#{Jason.encode!(ProcessInfo.to_map(process_info))}"
      end)

    mq_table = "\"mqTable\":{#{mq_table}}"

    "{#{properties},#{status_table},#{subs},#{mq_table}}"
  end
end
