defmodule ExRocketmq.Protocol.Transaction do
  @moduledoc false

  import ExRocketmq.Util.Const

  const :not_type, 0
  const :prepare_type, Bitwise.bsl(0x1, 2)
  const :commit_type, Bitwise.bsl(0x2, 2)
  const :rollback_type, Bitwise.bsl(0x3, 2)
end
