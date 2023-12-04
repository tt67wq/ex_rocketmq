defmodule ExRocketmq.Remote.Packet do
  @moduledoc """
  The communication package of RocketMQ is divided into four parts:

  ```text
  |-- Length --|-- Header Length --|-- Header --|-- Body --|
  ```

  1. Header: This is the header of the protocol, and the data is serialized in JSON format.
    Each key field in the JSON is fixed, but the fields for different communication requests may vary.
  2. Body: This contains the actual binary data of the request.
    For example, in a network request to send a message, the body would contain the actual message content.
  3. Length: This represents the overall length of the package, which is a four-byte integer.
  4. Header length: This indicates the length of the header, also represented as a four-byte integer.


  ## Header explanation:

  - code: Request/Response code. see `ExRocketmq.Protocol.Request` and `ExRocketmq.Protocol.Response` for details.

  - language: Since support for multiple languages is required, this field can inform both
    communicating parties about the programming language used in the communication layer.

  - version: Informs the communication layer about the version number of the other party.
    The responding party can use this information to perform special operations for compatibility with older versions,
    among other things.

  - opaque: Request identification code. this is simply an incrementing integer used to match the corresponding request with its response.

  - flag: Bit interpretation.
      The 0th bit indicates whether the communication is a request or a response. 0 represents a request, while 1 represents a response.
      The 1st bit indicates whether the request is a one-way request. 1 represents a one-way request.
      When handling a one-way request, the responding party does not provide a response,
      and the requesting party does not need to wait for a response from the responding party.

  - remark: Additional text information. Commonly used to store exception messages returned by brokers/nameservers,
      facilitating problem localization for developers.

  - extFields: This field is a string-dict, and it differs for each request/response and is completely customizable.
  """

  alias ExRocketmq.Typespecs

  require Record

  @response_type 1

  Record.defrecord(:packet,
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
          record(:packet,
            code: non_neg_integer(),
            language: non_neg_integer(),
            version: non_neg_integer(),
            opaque: non_neg_integer(),
            flag: non_neg_integer(),
            remark: String.t(),
            ext_fields: Typespecs.ext_fields(),
            body: binary()
          )

  @spec response_type?(t()) :: boolean()
  def response_type?(m) do
    m
    |> packet(:flag)
    |> Bitwise.band(@response_type)
    |> Kernel.==(@response_type)
  end
end
