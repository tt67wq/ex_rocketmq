defmodule ExRocketmq.Remote.Message do
  @moduledoc """
  The message to be sent or received in transport layer
  """

  alias ExRocketmq.{Typespecs, Remote.ExtFields}
  require Record

  @response_type 1

  Record.defrecord(:message,
    code: 0,
    # 5 represents erlang
    language: 5,
    version: 317,
    opaque: 0,
    flag: 0,
    remark: "",
    ext_fields: %{},
    body: <<>>
  )

  @type t ::
          record(:message,
            code: non_neg_integer(),
            language: non_neg_integer(),
            version: non_neg_integer(),
            opaque: non_neg_integer(),
            flag: non_neg_integer(),
            remark: String.t(),
            ext_fields: map(),
            body: binary()
          )

  @spec response_type?(t()) :: boolean()
  def response_type?(m) do
    message(m, :flag)
    |> Bitwise.band(@response_type)
    |> Kernel.==(@response_type)
  end

  @spec new_command_message(Typespecs.req_code(), ExtFields.t(), binary()) :: t()
  def new_command_message(code, header, body) do
    message(
      code: code,
      body: body,
      ext_fields: ExtFields.to_map(header)
    )
  end
end