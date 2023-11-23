defmodule ExRocketmq.Puller.State do
  @moduledoc false

  alias ExRocketmq.Models.BrokerData
  alias ExRocketmq.Models.MessageExt
  alias ExRocketmq.Models.MessageQueue
  alias ExRocketmq.Models.Subscription
  alias ExRocketmq.Typespecs

  defstruct client_id: "",
            group_name: "",
            mq: nil,
            buff_manager: nil,
            broker_data: nil,
            consume_from_where: :last_offset,
            consume_timestamp: 0,
            next_offset: -1,
            pull_batch_size: 32,
            post_subscription_when_pull: false,
            subscription: nil,
            buff: nil,
            holding_msgs: [],
            lock_ttl: -1,
            round: 0,
            last_lock_timestamp: 0,
            rt: 0

  @type t :: %__MODULE__{
          client_id: String.t(),
          group_name: Typespecs.group_name(),
          mq: MessageQueue.t(),
          buff_manager: nil | atom(),
          broker_data: BrokerData.t(),
          consume_from_where: :last_offset | :first_offset | :timestamp,
          consume_timestamp: non_neg_integer(),
          next_offset: integer(),
          pull_batch_size: non_neg_integer(),
          post_subscription_when_pull: boolean(),
          subscription: Subscription.t(),
          buff: atom(),
          holding_msgs: [MessageExt.t()],
          lock_ttl: integer(),
          round: non_neg_integer(),
          last_lock_timestamp: non_neg_integer(),
          rt: non_neg_integer()
        }
end
