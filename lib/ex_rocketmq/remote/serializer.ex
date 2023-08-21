defmodule ExRocketmq.Remote.Serializer do
  @moduledoc """
  encode/decode the packet to be sent or received
  """
  alias ExRocketmq.{Typespecs, Remote.Packet}

  @type t :: struct()

  @callback new(Typespecs.opts()) :: t()
  @callback encode(t(), Packet.t()) :: {:ok, binary()}
  @callback decode(t(), binary()) :: {:ok, Packet.t()} | {:error, any()}

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @spec encode(t(), Packet.t()) :: {:ok, binary()}
  def encode(m, msg), do: delegate(m, :encode, [msg])

  @spec decode(t(), binary()) :: {:ok, Packet.t()} | {:error, any()}
  def decode(m, bin), do: delegate(m, :decode, [bin])
end

defmodule ExRocketmq.Remote.Serializer.Json do
  @moduledoc """
  encode/decode the packet to be sent or received

  Frame format:
  ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  | frame_size | header_length |         header_body        |     body     |
  ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  |   4bytes   |     4bytes    | (21 + r_len + e_len) bytes | remain bytes |
  ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  """
  alias ExRocketmq.Remote.Packet

  require Packet

  defstruct [:name]

  @type t :: %__MODULE__{
          name: atom()
        }

  def new(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:name, :json)

    struct(__MODULE__, opts)
  end

  @spec encode(t(), Packet.t()) :: {:ok, binary()}
  def encode(_, pkt) do
    {:ok, header} =
      %{
        "code" => Packet.packet(pkt, :code),
        "language" => Packet.packet(pkt, :language),
        "version" => Packet.packet(pkt, :version),
        "opaque" => Packet.packet(pkt, :opaque),
        "flag" => Packet.packet(pkt, :flag),
        "remark" => Packet.packet(pkt, :remark),
        "extFields" => Packet.packet(pkt, :ext_fields)
      }
      |> Jason.encode()

    frame_size = 4 + byte_size(header) + byte_size(Packet.packet(pkt, :body))

    ret =
      [
        <<frame_size::big-integer-size(32), byte_size(header)::big-integer-size(32)>>,
        header,
        Packet.packet(pkt, :body)
      ]
      |> IO.iodata_to_binary()

    {:ok, ret}
  end

  @spec decode(t(), binary()) :: {:ok, Packet.t()} | {:error, any()}
  def decode(_, bin) do
    <<header_size::big-integer-size(32), rest::binary>> = bin
    <<header_body::bytes-size(header_size), body::binary>> = rest

    case Jason.decode(header_body) do
      {:ok, map} ->
        {:ok,
         Packet.packet(
           code: map["code"],
           language: map["language"],
           version: map["version"],
           opaque: map["opaque"],
           flag: map["flag"],
           remark: map["remark"],
           ext_fields: map["extFields"],
           body: body
         )}

      {:error, _} ->
        {:error, "invalid header"}
    end
  end
end
