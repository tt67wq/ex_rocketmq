defmodule ExRocketmq.Util.BitHelper do
  @moduledoc """
  bit tool for bit operation
  """

  @spec set_bit(non_neg_integer(), non_neg_integer(), boolean()) :: non_neg_integer()
  def set_bit(flag, offset, condition) do
    if condition do
      flag
      |> Bitwise.band(Bitwise.bsl(1, offset))
    else
      flag
    end
  end

  def unset_bit(flag, offset, condition) do
    if condition do
      flag
      |> Bitwise.band(Bitwise.bsl(-2, offset))
    else
      flag
    end
  end
end
