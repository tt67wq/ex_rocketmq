defmodule ExRocketmq.Models.Message do
  @moduledoc """
  The model of message sent by producer or consumed by consumer
  """
  alias ExRocketmq.{Typespecs}

  defstruct topic: "",
            body: "",
            flag: 0,
            transaction_id: "",
            batch: false,
            compress: false,
            properties: %{},
            queue_id: 0

  @type t :: %__MODULE__{
          topic: String.t(),
          body: binary(),
          flag: Typespecs.flag(),
          transaction_id: Typespecs.transaction_id(),
          batch: boolean(),
          compress: boolean(),
          properties: Typespecs.properties(),
          queue_id: non_neg_integer()
        }

  @property_kv_sep <<1>>
  @property_sep <<2>>

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
  def encode_properties(%__MODULE__{properties: nil}), do: ""

  def encode_properties(%__MODULE__{properties: properties}) do
    properties
    |> Enum.map(fn {k, v} -> [k, @property_kv_sep, v, @property_sep] |> IO.iodata_to_binary() end)
    |> IO.iodata_to_binary()
  end

  @spec decode_properties(binary()) :: Typespecs.properties()
  def decode_properties(""), do: %{}

  def decode_properties(data) do
    data
    |> :binary.split(@property_sep)
    |> Enum.reduce(
      %{},
      fn
        "", acc ->
          acc

        bin, acc ->
          [k, v] = :binary.split(bin, @property_kv_sep)
          Map.put(acc, k, v)
      end
    )
  end

  @spec get_property(t(), String.t(), String.t() | nil) :: String.t() | nil
  def get_property(msg, key, default \\ nil)

  def get_property(%__MODULE__{properties: nil}, _key, _default), do: nil

  def get_property(%__MODULE__{properties: properties}, key, default),
    do: Map.get(properties, key, default)

  @spec with_property(t(), String.t(), String.t()) :: t()
  def with_property(%__MODULE__{properties: nil} = msg, key, value) do
    %{msg | properties: %{key => value}}
  end

  def with_property(%__MODULE__{properties: properties} = msg, key, value) do
    %{msg | properties: Map.put(properties, key, value)}
  end

  @spec with_properties(t(), Typespecs.properties()) :: t()
  def with_properties(%__MODULE__{properties: nil} = msg, properties) do
    %{msg | properties: properties}
  end

  def with_properties(%__MODULE__{properties: properties} = msg, append_properties) do
    %{msg | properties: Map.merge(properties, append_properties)}
  end
end

defmodule ExRocketmq.Models.MessageExt do
  @moduledoc """
  extended message model
  """

  alias ExRocketmq.{
    Typespecs,
    Util.Network,
    Compressor,
    Compress.Zlib,
    Models.Message,
    Protocol.Flag,
    Protocol.Properties
  }

  require Flag
  require Properties

  @flag_born_host_v6 Flag.flag_born_host_v6()
  @flag_compressed Flag.flag_compressed()
  @property_unique_client_msgid_key Properties.property_unique_client_msgid_key()

  @type t :: %__MODULE__{
          message: Message.t(),
          msg_id: String.t(),
          offset_msg_id: String.t(),
          store_size: non_neg_integer(),
          queue_offset: non_neg_integer(),
          sys_flag: non_neg_integer(),
          born_timestamp: non_neg_integer(),
          born_host: String.t(),
          store_timestamp: non_neg_integer(),
          store_host: String.t(),
          commit_log_offset: non_neg_integer(),
          body_crc: non_neg_integer(),
          reconsume_times: non_neg_integer(),
          prepared_transaction_offset: non_neg_integer(),
          delay_level: Typespecs.delay_level()
        }

  defstruct message: %Message{},
            msg_id: "",
            offset_msg_id: "",
            store_size: 0,
            queue_offset: 0,
            sys_flag: 0,
            born_timestamp: 0,
            born_host: "",
            store_timestamp: 0,
            store_host: "",
            commit_log_offset: 0,
            body_crc: 0,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
            delay_level: 0

  @spec decode_from_binary(binary()) :: [t()]
  def decode_from_binary(body), do: decode_one(body, [])

  defp decode_one(<<>>, acc), do: acc

  defp decode_one(body, acc) do
    <<
      store_size::big-integer-size(32),
      _magic::size(32),
      body_crc::big-integer-size(32),
      queue_id::big-integer-size(32),
      flag::big-integer-size(32),
      queue_offset::big-integer-size(64),
      commit_log_offset::big-integer-size(64),
      sys_flag::big-integer-size(32),
      born_timestamp::big-integer-size(64),
      rest::binary
    >> = body

    {_, born_host, _port, rest} = parse_host_and_port(sys_flag, rest)
    <<store_timestamp::big-integer-size(64), rest::binary>> = rest
    {host_bin, store_host, port, rest} = parse_host_and_port(sys_flag, rest)
    offset_message_id = get_offset_message_id(host_bin, port, commit_log_offset)

    <<reconsume_times::big-integer-size(32), prepared_transaction_offset::big-integer-size(64),
      body_length::big-integer-size(32), rest::binary>> = rest

    <<body::bytes-size(body_length), rest::binary>> = rest
    body = uncompress(sys_flag, body)
    <<topic_length::big-integer-size(8), rest::binary>> = rest
    <<topic::bytes-size(topic_length), rest::binary>> = rest
    <<properties_length::big-integer-size(16), rest::binary>> = rest
    <<properties_binary::bytes-size(properties_length), rest::binary>> = rest
    properties = Message.decode_properties(properties_binary)

    ext = %__MODULE__{
      message: %Message{
        topic: topic,
        body: body,
        flag: flag,
        queue_id: queue_id,
        properties: properties
      },
      msg_id: Map.get(properties, @property_unique_client_msgid_key, offset_message_id),
      offset_msg_id: offset_message_id,
      store_size: store_size,
      queue_offset: queue_offset,
      sys_flag: sys_flag,
      born_timestamp: born_timestamp,
      born_host: born_host,
      store_timestamp: store_timestamp,
      store_host: store_host,
      commit_log_offset: commit_log_offset,
      body_crc: body_crc,
      reconsume_times: reconsume_times,
      prepared_transaction_offset: prepared_transaction_offset
    }

    decode_one(rest, [ext | acc])
  end

  @spec parse_host_and_port(non_neg_integer(), binary()) ::
          {binary(), String.t(), non_neg_integer(), binary()}
  defp parse_host_and_port(sys_flag, binary) do
    sys_flag
    |> ipv6?()
    |> if do
      <<host::bytes-size(16), port::big-integer-size(32), rest::binary>> = binary
      {host, Network.binary_to_ipv6(host), port, rest}
    else
      <<host::bytes-size(4), port::big-integer-size(32), rest::binary>> = binary
      {host, Network.binary_to_ipv4(host), port, rest}
    end
  end

  defp ipv6?(sys_flag) do
    sys_flag
    |> Bitwise.band(@flag_born_host_v6)
    |> Kernel.==(@flag_born_host_v6)
  end

  @spec uncompress(non_neg_integer(), binary()) :: binary()
  defp uncompress(sys_flag, body) do
    sys_flag
    |> Bitwise.band(@flag_compressed)
    |> Kernel.==(@flag_compressed)
    |> if do
      Compressor.uncompress(Zlib, body)
    else
      body
    end
  end

  @spec get_offset_message_id(binary(), non_neg_integer(), non_neg_integer()) :: String.t()
  def get_offset_message_id(host, port, offset) do
    [
      host,
      <<port::big-integer-size(32)>>,
      <<offset::big-integer-size(64)>>
    ]
    |> IO.iodata_to_binary()
    |> Base.encode16(case: :upper)
  end
end
