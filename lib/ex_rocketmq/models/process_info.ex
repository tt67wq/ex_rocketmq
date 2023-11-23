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

  def to_map(%__MODULE__{
        commit_offset: commit_offset,
        cached_msg_min_offset: cached_msg_min_offset,
        cached_msg_max_offset: cached_msg_max_offset,
        cached_msg_count: cached_msg_count,
        cached_msg_size_in_mib: cached_msg_size_in_mib,
        transaction_msg_min_offset: transaction_msg_min_offset,
        transaction_msg_max_offset: transaction_msg_max_offset,
        transaction_msg_count: transaction_msg_count,
        locked: locked,
        try_unlock_times: try_unlock_times,
        last_lock_timestamp: last_lock_timestamp,
        dropped: dropped,
        last_pull_timestamp: last_pull_timestamp,
        last_consumed_timestamp: last_consumed_timestamp
      }) do
    %{
      "commitOffset" => commit_offset,
      "cachedMsgMinOffset" => cached_msg_min_offset,
      "cachedMsgMaxOffset" => cached_msg_max_offset,
      "cachedMsgCount" => cached_msg_count,
      "cachedMsgSizeInMiB" => cached_msg_size_in_mib,
      "transactionMsgMinOffset" => transaction_msg_min_offset,
      "transactionMsgMaxOffset" => transaction_msg_max_offset,
      "transactionMsgCount" => transaction_msg_count,
      "locked" => locked,
      "tryUnlockTimes" => try_unlock_times,
      "lastLockTimestamp" => last_lock_timestamp,
      "dropped" => dropped,
      "lastPullTimestamp" => last_pull_timestamp,
      "lastConsumedTimestamp" => last_consumed_timestamp
    }
  end
end
