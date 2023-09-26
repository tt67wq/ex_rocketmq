defmodule ExRocketmq.Typespecs do
  @moduledoc """
  all kinds of typespecs
  """

  @type opts() :: keyword()
  @type ok_t() :: {:ok, any()}
  @type ok_t(t) :: {:ok, t}
  @type error_t() :: {:error, any()}
  @type name() :: atom() | {:global, term()} | {:via, module(), term()}
  @type str_dict() :: %{String.t() => String.t()}
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  # rocketmq related types
  @type opaque :: non_neg_integer()
  @type req_code :: non_neg_integer()
  @type resp_code :: non_neg_integer()
  @type topic :: String.t()
  @type broker_name :: String.t()
  @type flag :: non_neg_integer()
  @type sysflag :: non_neg_integer()
  @type group_name :: String.t()
  @type namespace :: String.t()
  @type compress_level :: :zlib.zlevel()
  @type transaction_id :: String.t()
  @type properties :: str_dict()
  @type ext_fields :: str_dict()
  @type transaction_state :: :commit | :rollback | :unknown
  @type transaction_type :: non_neg_integer()
  @type consumer_model :: :cluster | :broadcast
  @type consume_from_where :: :last_offset | :first_offset | :timestamp
  @type msg_id :: String.t()
  @type delay_level :: non_neg_integer()
end
