defmodule ExRocketmq.Util.Buffer do
  @moduledoc """
  A simple buffer queue implementation using ETS tables.

  This module provides a way to create a buffer queue with a specified capacity
  and allows items to be put into and taken out of the queue. If the queue is
  full, `put` will return `{:error, :full}`. If the queue is empty, `take` will
  block until an item is available.

  ## Examples

      iex> {:ok, pid} = ExRocketmq.Util.Buffer.start_link(name: :my_queue, size: 10)
      iex> ExRocketmq.Util.Buffer.put(:my_queue, [1, 2, 3])
      :ok
      iex> ExRocketmq.Util.Buffer.take(:my_queue)
      [1, 2, 3]

  """

  use Agent

  defstruct buff: nil,
            buff_size: 0,
            capacity: 0,
            touch: 0,
            gc_freq: 0

  @type t :: %__MODULE__{
          buff: atom(),
          buff_size: non_neg_integer(),
          capacity: non_neg_integer(),
          touch: non_neg_integer(),
          gc_freq: non_neg_integer()
        }

  @doc """
  Starts a new buffer process.

  ## Options

  * `:name` - The name of the buffer process. Must be an atom
  * `:size` - The maximum number of items the buffer can hold


  ## Examples

      iex> {:ok, pid} = ExRocketmq.Util.Buffer.start_link(name: :my_queue, size: 10)
      iex> ExRocketmq.Util.Buffer.put(:my_queue, [1, 2, 3])

  """
  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop!(opts, :name)
    {size, opts} = Keyword.pop!(opts, :size)
    Agent.start_link(__MODULE__, :init, [name, size, opts], name: name)
  end

  @doc """
  Adds the given items to the buffer queue.

  If the buffer queue is full, this function will return `{:error, :full}`.

  ## Examples

      iex> {:ok, _pid} = ExRocketmq.Util.Buffer.start_link(:my_queue, 10)
      iex> ExRocketmq.Util.Buffer.put(:my_queue, [1, 2, 3])
      :ok
      iex> ExRocketmq.Util.Buffer.put(:my_queue, [4, 5, 6], 1000)
      :ok
      iex> ExRocketmq.Util.Buffer.put(:my_queue, [7, 8, 9, 10, 11])
      {:error, :full}

  """
  @spec put(atom(), [any()], non_neg_integer()) :: :ok | {:error, :full}
  def put(name, items \\ [], timeout \\ 5000)

  def put(_name, [], _timeout), do: :ok

  def put(name, items, timeout) do
    Agent.get_and_update(name, __MODULE__, :handle_put, [items], timeout)
  end

  @doc """
  Removes and returns items from the buffer queue.

  If the buffer queue is empty, this function will block until an item is available.

  ## Examples

      iex> {:ok, _pid} = ExRocketmq.Util.Buffer.start_link(:my_queue, 10)
      iex> ExRocketmq.Util.Buffer.put(:my_queue, [1, 2, 3])
      :ok
      iex> ExRocketmq.Util.Buffer.take(:my_queue)
      [1, 2, 3]
      iex> ExRocketmq.Util.Buffer.take(:my_queue)
      []

  """
  @spec take(atom(), non_neg_integer()) :: [any()]
  def take(name, timeout \\ 5000) do
    Agent.get_and_update(name, __MODULE__, :handle_take, [], timeout)
  end

  @spec stop(atom()) :: :ok
  def stop(name) do
    Agent.stop(name)
  end

  @doc """
  Returns the number of items in the buffer queue.

  ## Examples

      iex> {:ok, _pid} = ExRocketmq.Util.Buffer.start_link(:my_queue, 10)
      iex> ExRocketmq.Util.Buffer.put(:my_queue, [1, 2, 3])
      :ok
      iex> ExRocketmq.Util.Buffer.size(:my_queue)
      3

  """
  @spec size(atom(), non_neg_integer()) :: non_neg_integer()
  def size(name, timeout \\ 5000) do
    Agent.get(name, __MODULE__, :handle_size, [], timeout)
  end

  @doc false
  @spec init(atom(), non_neg_integer(), Keyword.t()) :: t()
  def init(name, size, opts) do
    :ets.new(:"#{name}_buff", [:named_table, :ordered_set])

    {gc_freq, _} = Keyword.pop(opts, :gc_freq, 1000)

    %__MODULE__{
      buff: :"#{name}_buff",
      buff_size: 0,
      capacity: size,
      touch: 0,
      gc_freq: gc_freq
    }
  end

  @doc false
  def handle_put(
        state = %__MODULE__{
          buff: buff,
          buff_size: buff_size,
          capacity: capacity,
          touch: touch,
          gc_freq: gc_freq
        },
        items
      ) do
    if Enum.count(items) + buff_size > capacity do
      {{:error, :full}, state}
    else
      items
      |> Enum.with_index(buff_size)
      |> Enum.each(fn {item, idx} ->
        :ets.insert(buff, {idx, item})
      end)

      if touch >= gc_freq do
        # gc
        :ets.select_delete(buff, [
          {{:"$1", :"$2"}, [{:>=, :"$1", buff_size + Enum.count(items)}], [true]}
        ])

        {:ok, %__MODULE__{state | buff_size: buff_size + Enum.count(items), touch: 0}}
      else
        {:ok, %__MODULE__{state | buff_size: buff_size + Enum.count(items), touch: touch + 1}}
      end
    end
  end

  @doc false
  def handle_take(state = %__MODULE__{buff_size: 0}), do: {[], state}

  def handle_take(
        state = %__MODULE__{
          buff: buff,
          buff_size: buff_size
        }
      ) do
    {:ets.select(buff, [{{:"$1", :"$2"}, [{:<, :"$1", buff_size}], [:"$2"]}]),
     %__MODULE__{state | buff_size: 0}}
  end

  @doc false
  def handle_size(%__MODULE__{buff_size: buff_size}), do: buff_size
end
