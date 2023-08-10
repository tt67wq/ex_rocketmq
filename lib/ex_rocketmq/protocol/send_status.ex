defmodule ExRocketmq.Protocol.SendStatus do
  @moduledoc """
  The send status constants
  """

  import ExRocketmq.Util.Const

  const :send_ok, 0
  const :send_flush_disk_timeout, 1
  const :send_flush_slave_timeout, 2
  const :send_slave_not_available, 3
  const :send_unknown_error, 4
end
