defmodule ExRocketmq.Util.ClientId do
  @moduledoc """
  generate client id
  """

  @spec get() :: String.t()
  def get(), do: ExRocketmq.Util.Random.generate_id("P") |> get()

  @spec get(String.t()) :: String.t()
  def get(name) do
    [ip_addr(), "@", name]
    |> IO.iodata_to_binary()
  end

  @spec ip_addr() :: String.t()
  defp ip_addr() do
    {a, b, c, d} = ExRocketmq.Util.Network.get_local_ipv4_address()
    "#{a}.#{b}.#{c}.#{d}"
  end
end
