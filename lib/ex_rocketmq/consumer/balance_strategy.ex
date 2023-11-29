defmodule ExRocketmq.Consumer.BalanceStrategy do
  @moduledoc """
  The balance strategy behaviour of consumer
  """

  alias ExRocketmq.Models.MessageQueue
  alias ExRocketmq.Typespecs

  @type t :: struct()

  @callback allocate(
              m :: t(),
              client_id :: String.t(),
              mq_all :: list(MessageQueue.t()),
              cid_all :: list(String.t())
            ) :: {:ok, list(MessageQueue.t())}

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @doc """
  Implementing a strategy to allocate an appropriate message queue for the current client

  ## Examples

      iex> ExRocketmq.Consumer.BalanceStrategy.allocate(
             ExRocketmq.Consumer.BalanceStrategy.Average.new(),
             "c1",
             [mq1, mq2, mq3],
             [c1, c2, c3]
           )
      {:ok, [mq1]}
  """
  @spec allocate(
          m :: t(),
          client_id :: String.t(),
          mq_all :: list(MessageQueue.t()),
          cid_all :: list(String.t())
        ) :: {:ok, list(MessageQueue.t())}
  def allocate(m, client_id, mq_all, cid_all), do: delegate(m, :allocate, [client_id, mq_all, cid_all])
end

defmodule ExRocketmq.Consumer.BalanceStrategy.Average do
  @moduledoc """
  This module implements the balance strategy for RocketMQ consumers.

  This strategy is called 'Average', which allocates the message queues (mq) evenly among the clients (cid).
  The principle of this Average implementation is as follows:

  - Sort mqs and cid_all.
  - Divide mqs into len(cid_all) parts.
  - Allocate the corresponding chunk based on the position of cid in cid_all.

  ## Usage

  1. Create a new instance of the strategy using `new/0` function.
  2. Call the `allocate/4` function to allocate message queues to a specific client.

  ## Example

      strategy = Average.new()
      mq_all = [mq1, mq2, mq3, mq4]
      cid_all = ["client1", "client2", "client3"]
      {:ok, allocated_mqs} = Average.allocate(strategy, "client2", mq_all, cid_all)

  """

  @behaviour ExRocketmq.Consumer.BalanceStrategy

  import ExRocketmq.Util.Assertion

  alias ExRocketmq.Consumer.BalanceStrategy
  alias ExRocketmq.Models.MessageQueue
  alias ExRocketmq.Typespecs

  require Logger

  defstruct []

  @spec new() :: BalanceStrategy.t()
  def new, do: %__MODULE__{}

  @impl BalanceStrategy
  @spec allocate(
          m :: BalanceStrategy.t(),
          client_id :: String.t(),
          mq_all :: list(MessageQueue.t()),
          cid_all :: list(String.t())
        ) :: {:ok, list(MessageQueue.t())} | Typespecs.error_t()
  def allocate(_, client_id, mq_all, cid_all) do
    mq_all = Enum.sort_by(mq_all, & &1.queue_id)
    cid_all = Enum.sort(cid_all)
    idx = get_cid_index(client_id, cid_all)

    with :ok <- do_assert(fn -> idx >= 0 end, "client_id #{client_id} not in cid_all") do
      do_allocate(mq_all, cid_all, idx)
    end
  end

  @spec get_cid_index(String.t(), list(String.t())) :: integer()
  defp get_cid_index(client_id, cid_all) do
    cid_all
    |> Enum.find_index(&(&1 == client_id))
    |> case do
      nil -> -1
      index -> index
    end
  end

  @spec do_allocate(list(MessageQueue.t()), list(String.t()), non_neg_integer()) ::
          {:ok, list(MessageQueue.t())}
  defp do_allocate(mq_all, cid_all, cid_index) do
    mq_size = Enum.count(mq_all)
    cid_size = Enum.count(cid_all)

    mq_size
    |> div(cid_size)
    |> case do
      0 ->
        [mq_all]

      average_size ->
        mq_size
        |> rem(cid_size)
        |> case do
          0 -> Enum.chunk_every(mq_all, average_size)
          _ -> Enum.chunk_every(mq_all, average_size + 1)
        end
    end
    |> Enum.at(cid_index)
    |> case do
      nil -> {:ok, []}
      mqs -> {:ok, mqs}
    end
  end
end
