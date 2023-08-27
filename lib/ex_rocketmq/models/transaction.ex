defmodule ExRocketmq.Models.EndTransaction do
  @moduledoc """
  The end transaction header model
  """

  alias ExRocketmq.{Remote.ExtFields}

  @behaviour ExtFields

  defstruct [
    :producer_group,
    :tran_state_table_offset,
    :commit_log_offset,
    :commit_or_rollback,
    :from_transaction_check,
    :msg_id,
    :transaction_id
  ]

  @type t :: %__MODULE__{
          producer_group: String.t(),
          tran_state_table_offset: non_neg_integer(),
          commit_log_offset: non_neg_integer(),
          commit_or_rollback: non_neg_integer(),
          from_transaction_check: boolean(),
          msg_id: String.t(),
          transaction_id: String.t()
        }

  @impl ExtFields
  def to_map(t) do
    %{
      "producerGroup" => t.producer_group,
      "tranStateTableOffset" => "#{t.tran_state_table_offset}",
      "commitLogOffset" => "#{t.commit_log_offset}",
      "commitOrRollback" => "#{t.commit_or_rollback}",
      "fromTransactionCheck" => "#{t.from_transaction_check}",
      "msgId" => t.msg_id,
      "transactionId" => t.transaction_id
    }
  end
end
