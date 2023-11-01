defmodule ExRocketmq.Protocol.Request do
  @moduledoc """
  The request code constants
  """
  import ExRocketmq.Util.Const

  # 发送消息
  const :req_send_message, 10
  # 拉取消息
  const :req_pull_message, 11
  # 查询offset
  const :req_query_consumer_offset, 14
  # 更新offset
  const :req_update_consumer_offset, 15
  # 根据时间查询offset
  const :req_search_offset_by_timestamp, 29
  # 获取最大offset
  const :req_get_max_offset, 30
  # 发送心跳
  const :req_heartbeat, 34
  # 发送消息回退
  const :req_consumer_send_msg_back, 36
  # 结束事务
  const :req_end_transaction, 37
  # 获取消费者列表
  const :req_get_consumer_list_by_group, 38
  # 检查事务状态
  const :req_check_transaction_state, 39
  # 通知消费者id变更
  const :req_notify_consumer_ids_changed, 40
  # 锁定消息队列
  const :req_lock_batch_mq, 41
  # 解锁消息队列
  const :req_unlock_batch_mq, 42
  const :req_get_routeinfo_by_topic, 105
  const :req_get_broker_cluster_info, 106
  # 直接消费消息
  const :req_consume_message_directly, 309
  const :req_send_batch_message, 320
  const :req_send_reply_message, 324
end
