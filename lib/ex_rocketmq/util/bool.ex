defmodule ExRocketmq.Util.Bool do
  @moduledoc """
  bool util
  """

  @spec boolean?(String.t()) :: boolean()
  def boolean?(value) do
    case value do
      "1" -> true
      "t" -> true
      "T" -> true
      "TRUE" -> true
      "True" -> true
      "true" -> true
      _ -> false
    end
  end
end
