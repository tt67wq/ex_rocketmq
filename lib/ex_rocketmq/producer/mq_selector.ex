defmodule ExRocketmq.Producer.MqSelector do
  @moduledoc """
  The mq-selector behaviour of the producer
  """

  alias ExRocketmq.{Typespecs, Models}

  @type t :: struct()

  @callback new(Typespecs.opts()) :: t()
  @callback start(t()) :: t()
  @callback stop(t()) :: :ok

  @callback select(t(), Models.Message.t(), [Models.MessageQueue.t()]) :: Models.MessageQueue.t()

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @spec select(t(), Models.Message.t(), [Models.MessageQueue.t()]) :: Models.MessageQueue.t()
  def select(m, msg, queues), do: delegate(m, :select, [msg, queues])

  @spec start(t()) :: t()
  def start(m), do: delegate(m, :start, [])

  @spec stop(t()) :: :ok
  def stop(m), do: delegate(m, :stop, [])
end

defmodule ExRocketmq.Producer.MqSelector.Random do
  @moduledoc """
  select from queue list randomly
  """

  @behaviour ExRocketmq.Producer.MqSelector

  defstruct []

  @type t :: %__MODULE__{}

  def new(_opts), do: %__MODULE__{}

  def start(t), do: t
  def stop(_), do: :ok

  def select(_, _msg, mq_list) do
    mq_list
    |> Enum.random()
  end
end

defmodule ExRocketmq.Producer.Selector.RoundRobin do
  @moduledoc """
  select from queue list by round robin
  """

  @behaviour ExRocketmq.Producer.MqSelector

  defstruct [:pid]

  @type t :: %__MODULE__{
          pid: pid()
        }

  def new(opts \\ []) do
    struct(__MODULE__, opts)
  end

  def start(m) do
    {:ok, pid} = Agent.start_link(fn -> 0 end)
    %{m | pid: pid}
  end

  def stop(t) do
    Agent.stop(t.pid)
  end

  def select(m, _, mq_list) do
    Agent.get_and_update(m.pid, fn current ->
      {Enum.at(mq_list, current), (current + 1) |> rem(Enum.count(mq_list))}
    end)
  end
end
