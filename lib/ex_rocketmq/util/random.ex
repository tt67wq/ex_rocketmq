defmodule ExRocketmq.Util.Random do
  @moduledoc """
  Random util
  """

  @spec random_uuid() :: String.t()
  def random_uuid(length \\ 12) do
    :crypto.strong_rand_bytes(length)
    |> Base.encode16()
    |> String.downcase()
  end
end
