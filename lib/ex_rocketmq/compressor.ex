defmodule ExRocketmq.Compressor do
  @moduledoc """
  compress/uncompress behavior
  """

  @callback compress(binary(), keyword()) :: binary()
  @callback uncompress(binary()) :: binary()

  defp delegate(module, func, args),
    do: apply(module, func, [args])

  @spec compress(module(), binary(), keyword()) :: binary()
  def compress(m, data, opts \\ []), do: delegate(m, :compress, [data, opts])

  @spec uncompress(module(), binary()) :: binary()
  def uncompress(m, data), do: delegate(m, :uncompress, [data])
end
