defmodule ExRocketmq.Util.Queue do
  @moduledoc """
  a simple wrapper of queue
  """

  use Agent

  @spec start_link(opts :: Keyword.t()) :: Agent.on_start()
  def start_link(opts \\ []) do
    Agent.start_link(fn -> :queue.new() end, opts)
  end

  @spec push(pid(), any()) :: {:ok, any()}
  def push(agent, data) do
    Agent.get_and_update(agent, fn queue ->
      {:ok, :queue.in(data, queue)}
    end)
  end

  @spec pop(pid()) :: :empty | :any
  def pop(agent) do
    Agent.get_and_update(agent, fn queue ->
      case :queue.out(queue) do
        {:empty, _} = res -> res
        {{:value, data}, q} -> {data, q}
      end
    end)
  end

  @spec stop(pid()) :: :ok
  def stop(agent), do: Agent.stop(agent)
end
