defmodule ExRocketmq.Models.RunningInfo do
  @moduledoc false

  alias ExRocketmq.{Typespecs, Models}

  defstruct properties: %{},
            subscriptions: [],
            process_queue_table: %{},
            status_table: %{}

  @type t :: %__MODULE__{
          properties: Typespecs.str_dict(),
          subscriptions: [Subscription.t()],
          process_queue_table: %{MessageQueue.t() => ProcessInfo.t()},
          status_table: %{Typespecs.topic() => Stat.t()}
        }
end
