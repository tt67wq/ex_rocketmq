defmodule ExRocketmq.Serializer do
  @moduledoc """
  encode/decode the message to be sent or received
  """
  alias ExRocketmq.{Typespecs, Message}

  @type t :: struct()

  @callback new(Typespecs.opts()) :: t()
  @callback encode(t(), Message.t()) :: {:ok, binary()}
  @callback decode(t(), binary()) :: {:ok, Message.t()} | {:error, any()}

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @spec encode(t(), Message.t()) :: {:ok, binary()}
  def encode(m, msg), do: delegate(m, :encode, [msg])

  @spec decode(t(), binary()) :: {:ok, Message.t()} | {:error, any()}
  def decode(m, bin), do: delegate(m, :decode, [bin])
end

defmodule ExRocketmq.Serializer.Json do
  @moduledoc """
  encode/decode the message to be sent or received

  Frame format:
  ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  | frame_size | header_length |         header_body        |     body     |
  ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  |   4bytes   |     4bytes    | (21 + r_len + e_len) bytes | remain bytes |
  ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  """
  alias ExRocketmq.{Message}

  require Message

  defstruct [:name]

  @type t :: %__MODULE__{
          name: Typespecs.name()
        }

  def new(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:name, :json)

    struct(__MODULE__, opts)
  end

  @spec encode(t(), Message.t()) :: {:ok, binary()}
  def encode(_, msg) do
    {:ok, header} =
      %{
        "code" => Message.message(msg, :code),
        "language" => Message.message(msg, :language),
        "version" => Message.message(msg, :version),
        "opaque" => Message.message(msg, :opaque),
        "flag" => Message.message(msg, :flag),
        "remark" => Message.message(msg, :remark),
        "extFields" => Message.message(msg, :ext_fields)
      }
      |> Jason.encode()

    frame_size = 4 + byte_size(header) + byte_size(Message.message(msg, :body))

    {:ok,
     <<frame_size::big-integer-size(32), byte_size(header)::big-integer-size(32)>> <>
       header <> Message.message(msg, :body)}
  end

  @spec decode(t(), binary()) :: {:ok, Message.t()} | {:error, any()}
  def decode(_, bin) do
    <<header_size::big-integer-size(32), rest::binary>> = bin
    <<header_body::bytes-size(header_size), body::binary>> = rest

    case Jason.decode(header_body) do
      {:ok, map} ->
        {:ok,
         Message.message(
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
