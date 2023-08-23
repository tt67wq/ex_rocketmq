defmodule ExRocketmq.Protocol.Request do
  @moduledoc """
  The request code constants
  """
  import ExRocketmq.Util.Const

  const :req_send_message, 10
  const :req_pull_message, 11
  const :req_query_consumer_offset, 14
  const :req_update_consumer_offset, 15
  const :req_search_offset_by_timestamp, 29
  const :req_get_max_offset, 30
  const :req_heartbeat, 34
  const :req_get_routeinfo_by_topic, 105
  const :req_get_broker_cluster_info, 106
  const :req_send_batch_message, 320
  const :req_send_reply_message, 324
end
