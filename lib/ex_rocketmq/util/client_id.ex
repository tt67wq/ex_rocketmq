defmodule ExRocketmq.Util.ClientId do
  @moduledoc """
  generate client id
  """

  @spec get(String.t()) :: String.t()
  def get(name) do
    [ip_addr(), name, get_pid()]
    |> Enum.join("@")
  end

  @spec ip_addr() :: String.t()
  defp ip_addr() do
    {a, b, c, d} = ExRocketmq.Util.Network.get_local_ipv4_address()
    "#{a}.#{b}.#{c}.#{d}"
  end

  @spec get_pid() :: non_neg_integer()
  defp get_pid() do
    System.pid()
    |> String.to_integer()
  end
end
