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

defmodule ExRocketmq.Protocol.ConsumeResult do
  @moduledoc false

  import ExRocketmq.Util.Const

  const :success, 0
  const :retry_later, 1
  const :commit, 2
  const :rollback, 3
  const :suspend_current_queue_a_moment, 4
end
