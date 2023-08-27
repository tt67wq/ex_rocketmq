defmodule ExRocketmq.Remote.Waiter do
  @moduledoc """
  The future-wait store of the remote, opaque => request pid
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

  @spec start(opts_schema_t()) :: t()
  def start(opts) do
    {opts, init} =
      opts
      |> NimbleOptions.validate!(@opts_schema)
      |> Keyword.pop(:opts)

    {:ok, pid} = GenServer.start_link(__MODULE__, init, opts)
    %__MODULE__{pid: pid, name: init[:name], interval: init[:interval]}
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

  @impl true
  def terminate(_reason, %{name: name}) do
    :ets.delete_all_objects(name)
    :ets.delete(name)
  end
end
