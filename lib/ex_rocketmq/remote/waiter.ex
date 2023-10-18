defmodule ExRocketmq.Remote.Waiter do
  @moduledoc """
  The future-wait store of the remote, opaque => request pid
  This module is a wrapper of ets and genserver, ets is responsible for storing the request,
  and GenServer is responsible for cleaning up the expired request
  """

  alias ExRocketmq.{Typespecs}

  use GenServer

  defstruct [:name, :pid, :interval]

  @type t :: %__MODULE__{
          name: atom(),
          pid: pid(),
          interval: non_neg_integer()
        }

  @type key :: Typespecs.opaque()
  @type value :: {pid(), any()} | nil

  @opts_schema [
    name: [
      type: :atom,
      required: true,
      doc: "The name of the waiter"
    ],
    interval: [
      type: :integer,
      default: 5000,
      doc: "The cleanup interval time in millisecond of the waiter"
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The other options of the waiter"
    ]
  ]

  @type opts_schema_t :: [unquote(NimbleOptions.option_typespec(@opts_schema))]

  @doc """
  pop the value of the key from the waiter

  ## Examples

      iex> ExRocketmq.Remote.Waiter.pop(waiter, 1)
      nil
  """
  @spec pop(t(), Typespecs.opaque()) :: value()
  def pop(waiter, key) do
    case :ets.lookup(waiter.name, key) do
      [{^key, value, :infinity}] ->
        {:ok, value}

      [{^key, value, timeout}] when is_integer(timeout) ->
        if timeout < :os.system_time(:millisecond) do
          {:ok, nil}
        else
          {:ok, value}
        end

      _ ->
        :empty
    end
    |> case do
      {:ok, val} ->
        :ets.delete(waiter.name, key)
        val

      :empty ->
        nil
    end
  end

  @doc """
  put key-value pair to the waiter

  ## Params
  - `waiter` - the waiter instance
  - `key` - the key of the value, must be an integer
  - `value` - the value of the key
  - `ttl` - the time to live(milliseconds) of the key-value pair

  ## Examples

      iex> ExRocketmq.Remote.Waiter.put(waiter, 1, return_value, ttl: 5000)
  """
  @spec put(t(), key(), value(), ttl: non_neg_integer() | :infinity) :: any()
  def put(waiter, key, value, ttl: :infinity),
    do: :ets.insert(waiter.name, {key, value, :infinity})

  def put(waiter, key, value, ttl: ttl),
    do: :ets.insert(waiter.name, {key, value, :os.system_time(:millisecond) + ttl})

  def put(waiter, key, value, []), do: put(waiter, key, value, ttl: :infinity)

  @doc """
  start the waiter

  ## Options
  #{NimbleOptions.docs(@opts_schema)}

  ## Examples

      iex> ExRocketmq.Remote.Waiter.start(name: :waiter_name)
      %ExRocketmq.Remote.Waiter{
        name: :waiter_name,
        pid: #PID<0.262.0>,
        interval: 5000
      }
  """
  @spec start(opts_schema_t()) :: t()
  def start(opts) do
    {opts, init} =
      opts
      |> NimbleOptions.validate!(@opts_schema)
      |> Keyword.pop(:opts)

    {:ok, pid} = GenServer.start_link(__MODULE__, init, opts)
    %__MODULE__{pid: pid, name: init[:name], interval: init[:interval]}
  end

  @impl true
  def terminate(_reason, %{name: name}) do
    :ets.delete_all_objects(name)
    :ets.delete(name)
  end

  @spec stop(t()) :: :ok
  def stop(%{pid: pid}) when not is_nil(pid), do: GenServer.stop(pid)

  @impl true
  def init(opts) do
    :ets.new(opts[:name], [:named_table, :public, :set, {:read_concurrency, true}])
    {:ok, %{name: opts[:name], interval: opts[:interval]}, {:continue, :begin}}
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
