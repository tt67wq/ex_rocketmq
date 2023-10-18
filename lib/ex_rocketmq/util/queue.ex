defmodule ExRocketmq.Util.Queue do
  @moduledoc """
  a simple wrapper of fifo-queue with agent, so you don't have to keep queue in Genserver state, just a agent pid
  """

  use Agent

  @doc """
  start the queue agent

  ## Examples

      iex> {:ok, agent} = ExRocketmq.Util.Queue.start_link()
      {:ok, #PID<0.123.0>}
  """
  @spec start_link(opts :: Keyword.t()) :: Agent.on_start()
  def start_link(opts \\ []) do
    Agent.start_link(fn -> :queue.new() end, opts)
  end

  @spec stop(pid()) :: :ok
  def stop(agent), do: Agent.stop(agent)

  @doc """
  push a item to queue-agent

  ## Examples

      iex> ExRocketmq.Util.Queue.push(agent, data)
      {:ok, _}
  """
  @spec push(pid(), any()) :: {:ok, any()}
  def push(agent, data) do
    Agent.get_and_update(agent, fn queue ->
      {:ok, :queue.in(data, queue)}
    end)
  end

  @doc """
  pop a item from queue-agent
  """
  @spec pop(pid()) :: :empty | :any
  def pop(agent) do
    Agent.get_and_update(agent, fn queue ->
      case :queue.out(queue) do
        {:empty, _} = res -> res
        {{:value, data}, q} -> {data, q}
      end
    end)
  end
end
