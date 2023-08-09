defmodule ExRocketmq.Models.BrokerClusterInfo do
  @moduledoc """
  The broker cluster info model of rocketmq
  %{
    "brokerAddrTable" => %{
      "sts-broker-d2-0" => %{
        "brokerAddrs" => %{"0" => "10.88.4.57:20911", "1" => "10.88.4.189:20911"},
        "brokerName" => "sts-broker-d2-0",
        "cluster" => "d2"
      }
    },
    "clusterAddrTable" => %{"d2" => ["sts-broker-d2-0"]}
  }
  """

  alias ExRocketmq.Models.{BrokerData}

  @type t :: %__MODULE__{
          broker_addr_table: %{String.t() => BrokerData.t()},
          cluster_addr_table: map()
        }

  defstruct [:broker_addr_table, :cluster_addr_table]

  @spec from_json(String.t()) :: t()
  def from_json(json) do
    json
    |> Jason.decode!()
    |> from_map()
  end

  def from_map(%{"brokerAddrTable" => broker_addr_table, "clusterAddrTable" => cluster_addr_table}) do
    %__MODULE__{
      broker_addr_table:
        broker_addr_table |> Enum.into(%{}, fn {k, v} -> {k, BrokerData.from_map(v)} end),
      cluster_addr_table: cluster_addr_table
    }
  end
end
