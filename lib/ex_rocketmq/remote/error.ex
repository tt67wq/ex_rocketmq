defmodule ExRocketmq.Remote.Error do
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
