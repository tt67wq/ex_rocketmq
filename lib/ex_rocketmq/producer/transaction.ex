defmodule ExRocketmq.Producer.Transaction do
  @moduledoc """
  tranaction behavior
  """
  alias ExRocketmq.{Typespecs, Models}

  @type t :: struct()

  @callback execute_local(m :: t(), msg :: Models.Message.t()) ::
              {:ok, Typespecs.transaction_state()} | Typespecs.error_t()
  @callback check_local(m :: t(), msg :: Models.MessageExt.t()) ::
              {:ok, Typespecs.transaction_state()} | Typespecs.error_t()

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @doc """
  execute local transaction before commit to broker
  """
  @spec execute_local(t(), Models.Message.t()) ::
          {:ok, Typespecs.transaction_state()} | Typespecs.error_t()
  def execute_local(m, msg), do: delegate(m, :execute_local, [msg])

  @doc """
  check if local transaction is ok or not
  """
  @spec check_local(t(), Models.MessageExt.t()) ::
          {:ok, Typespecs.transaction_state()} | Typespecs.error_t()
  def check_local(m, msg), do: delegate(m, :check_local, [msg])
end

defmodule ExRocketmq.Producer.MockTransaction do
  @moduledoc """
  mock transaction implementation for test
  """

  @behaviour ExRocketmq.Producer.Transaction

  require ExRocketmq.Protocol.Transaction
  require Logger

  defstruct []

  @type t :: %__MODULE__{}

  @transaction_state %{
    "commit" => ExRocketmq.Protocol.Transaction.commit(),
    "rollback" => ExRocketmq.Protocol.Transaction.rollback(),
    "unknown" => ExRocketmq.Protocol.Transaction.unknown()
  }

  def new(), do: %__MODULE__{}

  def execute_local(_, msg) do
    Logger.info(
      "execute local transaction for #{inspect(msg)} in mock transaction implementation"
    )

    {:ok, @transaction_state["commit"]}
  end

  def check_local(_, msg) do
    Logger.info("check local transaction for #{inspect(msg)} in mock transaction implementation")
    {:ok, @transaction_state["commit"]}
  end
end
