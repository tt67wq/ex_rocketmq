defmodule ExRocketmq.Models.ProcessInfo do
  @moduledoc false

  defstruct commit_offset: 0,
            cached_msg_min_offset: 0,
            cached_msg_max_offset: 0,
            cached_msg_count: 0,
            cached_msg_size_in_mib: 0,
            transaction_msg_min_offset: 0,
            transaction_msg_max_offset: 0,
            transaction_msg_count: 0,
            locked: false,
            try_unlock_times: 0,
            last_lock_timestamp: 0,
            dropped: false,
            last_pull_timestamp: 0,
            last_consumed_timestamp: 0

  @type t :: %__MODULE__{
          commit_offset: non_neg_integer(),
          cached_msg_min_offset: non_neg_integer(),
          cached_msg_max_offset: non_neg_integer(),
          cached_msg_count: non_neg_integer(),
          cached_msg_size_in_mib: non_neg_integer(),
          transaction_msg_min_offset: non_neg_integer(),
          transaction_msg_max_offset: non_neg_integer(),
          transaction_msg_count: non_neg_integer(),
          locked: boolean,
          try_unlock_times: non_neg_integer(),
          last_lock_timestamp: non_neg_integer(),
          dropped: boolean,
          last_pull_timestamp: non_neg_integer(),
          last_consumed_timestamp: non_neg_integer()
        }
end
