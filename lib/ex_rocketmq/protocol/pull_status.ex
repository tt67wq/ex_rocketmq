defmodule ExRocketmq.Protocol.PullStatus do
  @moduledoc """
  The pull status constants
  """
  import ExRocketmq.Util.Const

  const :pull_found, 0
  const :pull_no_new_msg, 1
  const :pull_no_matched_msg, 2
  const :pull_offset_illegal, 3
  const :pull_broker_timeout, 4
end
