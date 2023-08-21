defmodule ExRocketmq.Models.Message do
  @moduledoc """
  The model of message sent by producer or consumed by consumer
  """
  alias ExRocketmq.{Typespecs}

  defstruct [
    :topic,
    :body,
    :flag,
    :transaction_id,
    :batch,
    :compress,
    :properties
  ]

  @type t :: %__MODULE__{
          topic: String.t(),
          body: binary(),
          flag: Typespecs.flag(),
          transaction_id: Typespecs.transaction_id(),
          batch: boolean(),
          compress: boolean(),
          properties: Typespecs.properties()
        }

  @spec encode(t()) :: binary()
  def encode(msg) do
    encoded_properties = encode_properties(msg)

    total_size = 20 + byte_size(msg.body) + byte_size(encoded_properties)

    <<
      total_size::big-integer-size(32),
      0::big-integer-size(32),
      0::big-integer-size(32),
      msg.flag::big-integer-size(32),
      byte_size(msg.body)::big-integer-size(32),
      msg.body::binary,
      byte_size(encoded_properties)::big-integer-size(16),
      encoded_properties::binary
    >>
  end

  @spec encode_properties(t()) :: binary()
  def encode_properties(msg) do
    msg.properties
    |> Enum.map(fn {k, v} -> [k, <<1>>, v, <<2>>] |> IO.iodata_to_binary() end)
    |> IO.iodata_to_binary()
  end
end
