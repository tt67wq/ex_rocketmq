defmodule ExRocketmq.Protocol.Flag do
  @moduledoc """
  sysflag constants
  """

  import ExRocketmq.Util.Const

  const :flag_compressed, 0x1
  const :flag_born_host_v6, Bitwise.bsl(0x1, 4)
end
