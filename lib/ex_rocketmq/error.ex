defmodule ExRocketmq.Error do
  @moduledoc """
  error of rocketmq
  """
  defexception [:message]

  @type t :: %__MODULE__{
          message: String.t()
        }

  @spec new(String.t()) :: %__MODULE__{}
  def new(message), do: %__MODULE__{message: message}
end

defmodule ExRocketmq.NamesrvsError do
  @moduledoc """
  error of rocketmq.Namesrvs
  """
  defexception [:code, :message]

  @type t :: %__MODULE__{
          code: non_neg_integer(),
          message: String.t()
        }

  @spec new(non_neg_integer(), String.t()) :: t()
  def new(code, message), do: %__MODULE__{code: code, message: message}
end
