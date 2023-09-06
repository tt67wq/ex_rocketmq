defmodule ExRocketmq.Util.Hasher do
  @moduledoc """
  Hash util
  """

  @spec hash_string(String.t()) :: non_neg_integer()
  def hash_string(s) do
    s
    |> String.to_charlist()
    |> Enum.reduce(0, fn c, acc ->
      acc * 31 + c
    end)
  end
end
