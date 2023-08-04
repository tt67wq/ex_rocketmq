defmodule ExRocketmq.Protocol.Request do
  @moduledoc """
  The request code constants
  """
  import ExRocketmq.Util.Const

  const :req_get_routeinfo_by_topic, 105
  const :req_get_broker_cluster_info, 106
end
