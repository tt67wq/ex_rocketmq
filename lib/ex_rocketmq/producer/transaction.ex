defmodule ExRocketmq.Producer.Transaction do
  @moduledoc """
  The implementation of transactional messages in RocketMQ is based on a three-step process:

  - Producers send a prepare message:
    First, producers send a prepare message to the broker.
    This message gets persisted in the commit log but is not pushed to consumers.
    This ensures the message will not be consumed before the transaction is committed.

  - Producers execute transaction logic:
    Producers call their own transaction execution logic, such as database operations.
    This step is asynchronous and does not depend on RocketMQ.

  - Producers send a commit or rollback message:
    If the transaction executes successfully, producers send a commit message to the broker.
    The broker will then push the previously persisted prepare message to consumers, indicating it can now be consumed.
    If the transaction fails, producers send a rollback message. The broker will delete the previously persisted prepare message,
    and consumers will not receive that message.

  Through these three steps, it can be ensured that messages will only be consumed if the transaction commits successfully,
  thus achieving atomicity.

  This module provides a behaviour for implementing local transaction and check operations for RocketMQ producers.
  The `execute_local` callback is called before a message is committed to the broker.
  It accepts a `message` as an argument and should return `{:ok, state}` if the transaction was successful, or an error tuple otherwise.

  The `check_local` callback is called after a message is committed to the broker.
  It accepts a `message_ext` as an argument and should return `{:ok, state}` if the transaction was successful, or an error tuple otherwise.

  ## Examples

  Here's an example of how to implement the `ExRocketmq.Producer.Transaction` behaviour:

      ```Elixir
      defmodule MyTransaction do
        @behaviour ExRocketmq.Producer.Transaction

        @impl ExRocketmq.Producer.Transaction
        def execute_local(_state, message) do
          # perform local transaction
          {:ok, :committed}
        end

        @impl ExRocketmq.Producer.Transaction
        def check_local(_state, message_ext) do
          # check local transaction status
          {:ok, :committed}
        end
      end
      ```
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
