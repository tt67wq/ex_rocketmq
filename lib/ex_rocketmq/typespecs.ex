defmodule ExRocketmq.Typespecs do
  @moduledoc """
  all kinds of typespecs
  """

  @type opts() :: keyword()
  @type ok_t() :: {:ok, any()}
  @type ok_t(t) :: {:ok, t}
  @type error_t() :: {:error, any()}
  @type name() :: atom()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @type opaque :: non_neg_integer()
  @type req_code :: non_neg_integer()
end
