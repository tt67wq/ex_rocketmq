defmodule ExRocketmq.Protocol.Properties do
  @moduledoc """
  RocketMQ properties
  """

  import ExRocketmq.Util.Const

  const :property_unique_client_msgid_key, "UNIQ_KEY"
  const :property_transaction_prepared, "TRAN_MSG"
  const :property_producer_group, "PGROUP"
  const :property_msg_type, "MSG_TYPE"
end
