defmodule ExRocketmq.Models.ConsumeMessageDirectly do
  @moduledoc false

  alias ExRocketmq.Typespecs

  defstruct consumer_group: "",
            client_id: "",
            msg_id: "",
            broker_name: ""

  @type t :: %__MODULE__{
          consumer_group: Typespecs.group_name(),
          client_id: String.t(),
          msg_id: String.t(),
          broker_name: String.t()
        }

  @spec decode(Typespecs.properties()) :: t()
  def decode(properties) do
    %__MODULE__{
      consumer_group: Map.get(properties, "consumerGroup", ""),
      client_id: Map.get(properties, "clientId", ""),
      msg_id: Map.get(properties, "msgId", ""),
      broker_name: Map.get(properties, "brokerName", "")
    }
  end
end

defmodule ExRocketmq.Models.ConsumeMessageDirectlyResult do
  @moduledoc false

  alias ExRocketmq.{Remote.ExtFields}

  @behaviour ExtFields

  defstruct order: false, auto_commit: false, consume_result: 0, remark: "", spend_time_millis: 0

  @type t :: %__MODULE__{
          order: boolean(),
          auto_commit: boolean(),
          consume_result: non_neg_integer(),
          remark: String.t(),
          spend_time_millis: non_neg_integer()
        }

  @impl ExtFields
  @spec to_map(t()) :: Typespecs.str_dict()
  def to_map(m) do
    %{
      "order" => m.order,
      "autoCommit" => m.auto_commit,
      "consumeResult" => m.consume_result,
      "remark" => m.remark,
      "spendTimeMillis" => m.spend_time_millis
    }
  end
end
