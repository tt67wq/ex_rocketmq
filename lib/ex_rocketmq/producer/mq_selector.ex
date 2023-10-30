defmodule ExRocketmq.Producer.MqSelector do
  @moduledoc """
  Provides a behaviour for selecting MessageQueues in the ExRocketmq Producer.

  MQ selectors implementing this behaviour are responsible for selecting the
  appropriate MessageQueue when sending messages to RocketMQ. This allows plugging
  in different selection strategies like random, round-robin, etc.

  To use a custom MQ selector, implement the `new/1`, `start/1`, `stop/1` and
  `select/3` callbacks and pass the module as the `:mq_selector` option to the
  `ExRocketmq.Producer` constructor.
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
  Implements random MessageQueue selection for the ExRocketmq Producer.

  This module provides a random selection strategy by randomly choosing a
  MessageQueue from the given list on each `select/3` call.

  To use this random selector, pass the `ExRocketmq.Producer.MqSelector.Random`
  module as the `:selector` option when constructing a Producer:

    producer = ExRocketmq.Producer.start_link(
      selector: ExRocketmq.Producer.MqSelector.Random.new([])
    )

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
  Implements round-robin MessageQueue selection for the ExRocketmq Producer.

  It maintains an internal counter via an Agent to track the next queue index. On
  each `select/3` call, it returns the queue at the current index and increments
  the counter.

  To use this round-robin selector, pass the
  `ExRocketmq.Producer.Selector.RoundRobin` module as the `:selector` option
  when constructing a Producer:

    producer = ExRocketmq.Producer.start_link(
      selector: ExRocketmq.Producer.Selector.RoundRobin.new([])
    )

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

defmodule ExRocketmq.Producer.Selector.Hash do
  @moduledoc """
  Implements hash-based MessageQueue selection for the ExRocketmq Producer.

  It uses consistent hashing to map messages to queues based on the value of a
  sharding key property on the message. If no sharding key is present, it falls
  back to random selection.

  The sharding key used is the `:sharding_key` property defined in the RocketMQ
  protocol.

  To use this hash-based selector, pass the
  `ExRocketmq.Producer.Selector.Hash` module as the `:selector` option when
  constructing a Producer:

    producer = ExRocketmq.Producer.start_link(
      selector: ExRocketmq.Producer.Selector.Hash.new([])
    )

  """

  @behaviour ExRocketmq.Producer.MqSelector

  alias ExRocketmq.Models.Message
  alias ExRocketmq.Util.Fnv1a
  require ExRocketmq.Protocol.Properties

  @sharding_key ExRocketmq.Protocol.Properties.property_sharding_key()

  defstruct []

  @type t :: %__MODULE__{}

  def new(_opts), do: %__MODULE__{}

  def start(t), do: t
  def stop(_), do: :ok

  def select(_, msg, mq_list) do
    Message.get_property(msg, @sharding_key)
    |> case do
      nil ->
        mq_list
        |> Enum.random()

      key ->
        idx =
          key
          |> Fnv1a.hash()
          |> rem(Enum.count(mq_list))

        elem(mq_list, idx)
    end
  end
end
