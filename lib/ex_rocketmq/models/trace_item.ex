defmodule ExRocketmq.Models.TraceItem do
  @moduledoc """
  trace item data model
  """

  defstruct topic: "",
            msg_id: "",
            offset_msg_id: "",
            tags: "",
            keys: "",
            store_host: "",
            client_host: "",
            store_time: 0,
            retry_times: 0,
            body_length: 0,
            msg_type: 0

  @type t :: %__MODULE__{
          topic: String.t(),
          msg_id: String.t(),
          offset_msg_id: String.t(),
          tags: String.t(),
          keys: String.t(),
          store_host: String.t(),
          client_host: String.t(),
          store_time: non_neg_integer(),
          retry_times: non_neg_integer(),
          body_length: non_neg_integer(),
          msg_type: non_neg_integer()
        }
end
