defmodule ExRocketmq.Namesrvs do
  @moduledoc """
  The namesrvs layer of the rocketmq: how client communicates with the namesrvs
  """

  use GenServer

  alias ExRocketmq.{Remote, Typespecs, Message, Protocol.Request}

  require Message

  import ExRocketmq.Util.Debug

  @namesrvs_opts_schema [
    name: [
      type: :atom,
      default: __MODULE__,
      doc: "The name of the namesrvs"
    ],
    remote: [
      type: :any,
      required: true,
      doc: "The remote instance of the namesrvs"
    ],
    json_module: [
      type: :atom,
      default: Jason,
      doc: "The json module of the namesrvs"
    ]
  ]

  defstruct [:name, :remote, :json_module]

  @type t :: %__MODULE__{
          name: Typespecs.name(),
          remote: Remote.t(),
          json_module: atom()
        }

  @type namesrvs_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@namesrvs_opts_schema))]

  @spec new(namesrvs_opts_schema_t()) :: t()
  def new(opts) do
    opts =
      opts
      |> NimbleOptions.validate!(@namesrvs_opts_schema)

    struct(__MODULE__, opts)
  end

  @spec start_link(namesrvs: t()) :: Typespecs.on_start()
  def start_link(namesrvs: namesrvs) do
    GenServer.start_link(__MODULE__, namesrvs, name: namesrvs.name)
  end

  def query_topic_route_info(namesrvs, topic) do
    with {:ok, msg} <-
           GenServer.call(
             namesrvs.name,
             {:rpc, Request.req_get_routeinfo_by_topic(), <<>>, %{"topic" => topic}}
           ) do
      msg
      |> Message.message(:body)
      |> fix_invalid_json()
      |> namesrvs.json_module.decode()
      |> debug()
    end
  end

  # because of fastjson, brokerAddrs is a integer-keyed map, which is invalid json
  # we replace integer-keyed map with string-keyed map here
  defp fix_invalid_json(input) do
    pattern = ~r/"brokerAddrs":{(\d+:"[^"]+",*)+}/
    matches = Regex.run(pattern, input)

    case matches do
      [] ->
        input

      [brokerAddrs, _] ->
        # "\"brokerAddrs\":{0:\"10.88.4.78:20911\",1:\"10.88.4.79:20911\"}"
        fixedBrokerAddrs =
          brokerAddrs
          |> String.slice(15..-2)
          |> String.split(",")
          |> Enum.map(fn x ->
            [index, addr] = String.split(x, ":", parts: 2)
            "\"#{index}\":#{addr}"
          end)
          |> Enum.join(",")
          |> then(&"\"brokerAddrs\":{#{&1}}")

        String.replace(input, brokerAddrs, fixedBrokerAddrs)
    end
  end

  # -------- server -------

  def init(namesrvs) do
    {:ok, _} = Remote.start_link(remote: namesrvs.remote)
    {:ok, %{namesrvs: namesrvs, opaque: 0}}
  end

  def handle_call(
        {:rpc, code, body, ext_fields},
        _from,
        %{namesrvs: namesrvs, opaque: opaque} = state
      ) do
    msg =
      Message.message(
        code: code,
        opaque: opaque,
        ext_fields: ext_fields,
        body: body
      )

    {:reply, Remote.rpc(namesrvs.remote, msg), %{state | opaque: opaque + 1}}
  end
end
