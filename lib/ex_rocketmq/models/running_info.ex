defmodule ExRocketmq.Models.RunningInfo do
  @moduledoc false

  alias ExRocketmq.{Typespecs}
  alias ExRocketmq.Models.{Stat, Subscription, MessageQueue, ProcessInfo}

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
end
