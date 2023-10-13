defmodule ExRocketmq.Protocol.ConsumeReturnType do
  @moduledoc """
  Consume return type
  """

  import ExRocketmq.Util.Const

  const :success, 0
  const :timeout, 1
  const :exception, 2
  const :null, 3
  const :failed, 4
end
