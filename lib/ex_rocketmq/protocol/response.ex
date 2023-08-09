defmodule ExRocketmq.Protocol.Response do
  @moduledoc """
  The response code constants
  """

  import ExRocketmq.Util.Const

  const :resp_success, 0
  const :resp_error, 1
  const :resp_topic_not_exist, 17
end
