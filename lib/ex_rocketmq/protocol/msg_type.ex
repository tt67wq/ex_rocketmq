defmodule ExRocketmq.Protocol.MsgType do
  @moduledoc false
  import ExRocketmq.Util.Const

  const :normal_msg, 0
  const :trans_msg_half, 1
  const :trans_msg_commit, 2
  const :delay_msg, 3
end
