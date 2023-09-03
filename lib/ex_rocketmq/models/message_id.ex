defmodule ExRocketmq.Models.MessageId do
  @moduledoc false

  alias ExRocketmq.Util.Network

  defstruct addr: "", port: 0, offset: 0

  @type t :: %__MODULE__{
          addr: String.t(),
          port: non_neg_integer(),
          offset: non_neg_integer()
        }

  @spec decode(binary()) :: {:ok, t()} | {:error, :invalid_msg_id}
  def decode(msg_id_bin) do
    with <<encoded_addr::binary-size(8), encoded_port::binary-size(8),
           encoded_offset::binary-size(16)>> <- msg_id_bin,
         {:ok, addr_bytes} <- Base.decode16(encoded_addr),
         {:ok, <<port::big-integer-size(32)>>} <- Base.decode16(encoded_port),
         {:ok, <<offset::big-integer-size(64)>>} <- Base.decode16(encoded_offset) do
      {:ok,
       %__MODULE__{
         addr: Network.binary_to_ipv4(addr_bytes),
         port: port,
         offset: offset
       }}
    else
      _ -> {:error, :invalid_msg_id}
    end
  end
end
