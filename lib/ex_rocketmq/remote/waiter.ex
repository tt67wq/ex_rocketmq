defmodule ExRocketmq.Remote.Waiter do
  @moduledoc """
  The future-wait store of the remote, opaque => request pid
  """

  alias ExRocketmq.{Typespecs}

  use GenServer

  defstruct [:name, :interval]

  @type t :: %__MODULE__{
          name: atom(),
          interval: non_neg_integer()
        }

  @type key :: Typespecs.opaque()
  @type value :: {pid(), any()} | nil

  @spec new(Typespecs.opts()) :: t()
  def new(opts) do
    opts =
      opts
      |> Keyword.put_new(:interval, 5000)

    struct(__MODULE__, opts)
  end

  @spec pop(t(), Typespecs.opaque()) :: value()
  def pop(waiter, key) do
    value =
      case :ets.lookup(waiter.name, key) do
        [{^key, value, :infinity}] ->
          value

        [{^key, value, timeout}] when is_integer(timeout) ->
          if timeout < :os.system_time(:millisecond) do
            nil
          else
            value
          end

        _ ->
          nil
      end

    :ets.delete(waiter.name, key)
    value
  end

  @spec put(t(), key(), value(), ttl: non_neg_integer() | :infinity) :: any()
  def put(waiter, key, value, ttl: :infinity),
    do: :ets.insert(waiter.name, {key, value, :infinity})

  def put(waiter, key, value, ttl: ttl),
    do: :ets.insert(waiter.name, {key, value, :os.system_time(:millisecond) + ttl})

  def put(waiter, key, value, []), do: put(waiter, key, value, ttl: :infinity)

  @spec start_link(Typespecs.opts()) :: Typespecs.on_start()
  def start_link(opts) do
    {waiter, opts} = Keyword.pop!(opts, :waiter)
    GenServer.start_link(__MODULE__, waiter, opts)
  end

  @impl true
  def init(waiter) do
    :ets.new(waiter.name, [:named_table, :public, :set, {:read_concurrency, true}])
    {:ok, %{name: waiter.name, interval: waiter.interval}, {:continue, :begin}}
  end

  @impl true
  def handle_continue(:begin, %{interval: interval} = state) do
    Process.send_after(self(), :cleanup, interval)
    {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup, %{name: name, interval: interval} = state) do
    now = :os.system_time(:millisecond)

    :ets.foldl(
      fn {_key, _val, timeout} = item, deleted ->
        case timeout do
          :infinity ->
            deleted

          timeout when is_integer(timeout) and timeout < now ->
            :ets.delete(name, item)
            deleted + 1

          _ ->
            deleted
        end
      end,
      0,
      name
    )

    Process.send_after(self(), :cleanup, interval)
    {:noreply, state}
  end
end
