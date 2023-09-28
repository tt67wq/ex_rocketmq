defmodule ExRocketmq.Util.Network do
  @moduledoc """
  network tools
  """

  @spec parse_addr(String.t()) :: {String.t(), non_neg_integer()}
  def parse_addr(addr) do
    case String.split(addr, ":") do
      [host, port] -> {host, String.to_integer(port)}
      _ -> raise "invalid addr: #{inspect(addr)}"
    end
  end

  @spec get_local_ipv4_address() :: :socket.in_addr()
  def get_local_ipv4_address do
    # 获取本地 IP 地址列表
    {:ok, ifs} = :net.getifaddrs(:inet)

    ifs
    |> Enum.reject(fn %{name: name} -> name == ~c"lo0" end)
    |> Enum.take(1)
    |> case do
      [] -> raise "no network interface found"
      [%{addr: %{addr: addr}}] -> addr
    end
  end

  @spec binary_to_ipv6(<<_::128>>) :: String.t()
  def binary_to_ipv6(binary) do
    <<a::big-integer-size(16), b::big-integer-size(16), c::big-integer-size(16),
      d::big-integer-size(16), e::big-integer-size(16), f::big-integer-size(16),
      g::big-integer-size(16), h::big-integer-size(16)>> = binary

    [a, b, c, d, e, f, g, h]
    |> Enum.map_join(":", &Integer.to_string(&1, 16))
  end

  @spec binary_to_ipv4(<<_::32>>) :: String.t()
  def binary_to_ipv4(binary) do
    <<a::big-integer-size(8), b::big-integer-size(8), c::big-integer-size(8),
      d::big-integer-size(8)>> = binary

    [a, b, c, d]
    |> Enum.map_join(".", &Integer.to_string(&1))
  end
end
