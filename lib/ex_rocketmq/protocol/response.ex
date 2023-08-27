defmodule ExRocketmq.Protocol.Response do
  @moduledoc """
  The response code constants
  """

  import ExRocketmq.Util.Const

  const :resp_success, 0
  const :resp_error, 1
  const :res_flush_disk_timeout, 10
  const :res_slave_not_available, 11
  const :res_flush_slave_timeout, 12
  const :res_service_not_available, 14
  const :res_no_permission, 16
  const :resp_topic_not_exist, 17
  const :resp_pull_not_found, 19
  const :resp_pull_retry_immediately, 20
  const :resp_pull_offset_moved, 21
end
