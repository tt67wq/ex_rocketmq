defmodule ExRocketmq.Models.RunningInfo do
  @moduledoc false

  alias ExRocketmq.{Typespecs, Models}

  defstruct properties: %{},
            subscription_data: %{},
            mq_table: %{},
            status_table: %{}

  @type t :: %__MODULE__{
          properties: Typespecs.str_dict(),
          subscription_data: %{Models.Subscription.t() => boolean()},
          mq_table: %{Models.MessageQueue.t() => Models.ProcessInfo.t()},
          status_table: %{String.t() => Models.Stat.t()}
        }
end
