defmodule ExRocketmq.Broker do
  @moduledoc """
  RocketMQ Broker Client
  """
  @broker_opts_schema [
    name: [
      type: :atom,
      default: __MODULE__,
      doc: "The name of the broker"
    ],
    remotes: [
      type: {:list, :any},
      required: true,
      doc: "The remote instances of the broker"
    ]
  ]

  defstruct [:name, :remotes]

  @type t :: %__MODULE__{
          name: Typespecs.name(),
          remotes: [Remote.t()]
        }

  @type namesrvs_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@broker_opts_schema))]
end
