defmodule ExRocketmq.Producer.Selector do
  @moduledoc """
  The selector behaviour of the producer
  """

  alias ExRocketmq.{Typespecs, Models}

  @type t :: struct()

  @callback new(Typespecs.opts()) :: t()
  @callback start_link(selector: t()) :: Typespecs.on_start()

  @callback select(t(), Models.Letter.t(), [Models.MessageQueue.t()]) :: Models.MessageQueue.t()

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @spec select(t(), Models.Letter.t(), [Models.MessageQueue.t()]) :: Models.MessageQueue.t()
  def select(m, msg, queues), do: delegate(m, :select, [msg, queues])

  @spec start_link(t()) :: Typespecs.on_start()
  def start_link(%module{} = m) do
    apply(module, :start_link, [[selector: m]])
  end
end

defmodule ExRocketmq.Producer.Selector.Random do
  @moduledoc """
  select from queue list randomly
  """

  alias ExRocketmq.{Typespecs}

  @behaviour ExRocketmq.Producer.Selector

  defstruct [:name]

  @type t :: %__MODULE__{
          name: Typespecs.name()
        }

  def new(opts \\ []) do
    opts = opts |> Keyword.put_new(:name, :random)
    struct(__MODULE__, opts)
  end

  def start_link(selector: m) do
    Agent.start_link(fn -> m end, name: m.name)
  end

  def select(_, _msg, mq_list) do
    mq_list
    |> Enum.random()
  end
end

defmodule ExRocketmq.Producer.Selector.RoundRobin do
  @moduledoc """
  select from queue list by round robin
  """

  alias ExRocketmq.{Typespecs}

  @behaviour ExRocketmq.Producer.Selector

  defstruct [:name]

  @type t :: %__MODULE__{
          name: Typespecs.name()
        }

  def new(opts \\ []) do
    opts = opts |> Keyword.put_new(:name, :round_robin)
    struct(__MODULE__, opts)
  end

  def start_link(selector: m) do
    Agent.start_link(fn -> 0 end, name: m.name)
  end

  def select(m, _, mq_list) do
    Agent.get_and_update(m.name, fn current ->
      {Enum.at(mq_list, current), (current + 1) |> rem(Enum.count(mq_list))}
    end)
  end
end
