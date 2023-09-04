defmodule ExRocketmq.Models.EndTransaction do
  @moduledoc """
  The end transaction header model
  """

  alias ExRocketmq.{Remote.ExtFields}

  @behaviour ExtFields

  defstruct producer_group: "",
            tran_state_table_offset: 0,
            commit_log_offset: 0,
            commit_or_rollback: 0,
            from_transaction_check: false,
            msg_id: "",
            transaction_id: ""

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

defmodule ExRocketmq.Models.CheckTransactionState do
  @moduledoc """
  The check transaction state header model
  """

  alias ExRocketmq.{Remote.ExtFields, Typespecs}

  @behaviour ExtFields

  defstruct tran_state_table_offset: 0,
            commit_log_offset: 0,
            msg_id: "",
            transaction_id: "",
            offset_msg_id: ""

  @type t :: %__MODULE__{
          tran_state_table_offset: non_neg_integer(),
          commit_log_offset: non_neg_integer(),
          msg_id: String.t(),
          transaction_id: String.t(),
          offset_msg_id: String.t()
        }

  @impl ExtFields
  def to_map(%__MODULE__{} = t) do
    %{
      "tranStateTableOffset" => "#{t.tran_state_table_offset}",
      "commitLogOffset" => "#{t.commit_log_offset}",
      "msgId" => t.msg_id,
      "transactionId" => t.transaction_id,
      "offsetMsgId" => t.offset_msg_id
    }
  end

  @spec decode(Typespecs.ext_fields()) :: {:ok, t()} | {:error, any()}
  def decode(ext_fields) do
    with tran_state_table_offset <- Map.get(ext_fields, "tranStateTableOffset", "0"),
         commit_log_offset <- Map.get(ext_fields, "commitLogOffset", "0"),
         msg_id <- Map.get(ext_fields, "msgId", ""),
         transaction_id <- Map.get(ext_fields, "transactionId", ""),
         offset_msg_id <- Map.get(ext_fields, "offsetMsgId", "") do
      {:ok,
       %__MODULE__{
         tran_state_table_offset: String.to_integer(tran_state_table_offset),
         commit_log_offset: String.to_integer(commit_log_offset),
         msg_id: msg_id,
         transaction_id: transaction_id,
         offset_msg_id: offset_msg_id
       }}
    end
  end
end
