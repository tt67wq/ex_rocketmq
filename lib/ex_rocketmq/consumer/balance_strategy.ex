defmodule ExRocketmq.Consumer.BalanceStrategy do
  @moduledoc """
  The balance strategy behaviour of consumer
  """

  alias ExRocketmq.Models.{MessageQueue}
  alias ExRocketmq.Typespecs

  @type t :: struct()

  @callback allocate(
              m :: t(),
              client_id :: String.t(),
              mq_all :: list(MessageQueue.t()),
              cid_all :: list(String.t())
            ) :: {:ok, list(MessageQueue.t())} | Typespecs.error_t()

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @spec allocate(
          m :: t(),
          client_id :: String.t(),
          mq_all :: list(MessageQueue.t()),
          cid_all :: list(String.t())
        ) :: {:ok, list(MessageQueue.t())} | Typespecs.error_t()
  def allocate(m, client_id, mq_all, cid_all),
    do: delegate(m, :allocate, [client_id, mq_all, cid_all])
end

defmodule ExRocketmq.Consumer.BalanceStrategy.Average do
  @moduledoc false

  alias ExRocketmq.Consumer.BalanceStrategy
  alias ExRocketmq.Models.{MessageQueue}
  alias ExRocketmq.Typespecs

  import ExRocketmq.Util.Assertion

  require Logger

  @behaviour BalanceStrategy

  defstruct []

  @spec new() :: BalanceStrategy.t()
  def new(), do: %__MODULE__{}

  @impl BalanceStrategy
  @spec allocate(
          m :: BalanceStrategy.t(),
          client_id :: String.t(),
          mq_all :: list(MessageQueue.t()),
          cid_all :: list(String.t())
        ) :: {:ok, list(MessageQueue.t())} | Typespecs.error_t()
  def allocate(_, client_id, mq_all, cid_all) do
    with mq_all <- Enum.sort_by(mq_all, & &1.queue_id),
         cid_all <- Enum.sort(cid_all),
         idx <- get_cid_index(client_id, cid_all),
         :ok <- do_assert(fn -> idx >= 0 end, "client_id #{client_id} not in cid_all") do
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

    div(mq_size, cid_size)
    |> case do
      0 ->
        [mq_all]

      average_size ->
        rem(mq_size, cid_size)
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
