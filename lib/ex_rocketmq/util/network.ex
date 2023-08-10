defmodule ExRocketmq.Util.Network do
  @moduledoc """
  network tools
  """

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
end
