defmodule MockTransaction do
  @moduledoc """
  mock transaction implementation for test
  """

  @behaviour ExRocketmq.Producer.Transaction

  require ExRocketmq.Protocol.Transaction
  require Logger

  defstruct []

  @type t :: %__MODULE__{}

  def new(), do: %__MODULE__{}

  def execute_local(_, msg) do
    Logger.info(
      "execute local transaction for #{inspect(msg)} in mock transaction implementation"
    )

    {:ok, :commit}
  end

  def check_local(_, msg) do
    Logger.info("check local transaction for #{inspect(msg)} in mock transaction implementation")
    {:ok, :commit}
  end
end
