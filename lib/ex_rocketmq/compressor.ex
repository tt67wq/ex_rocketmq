defmodule ExRocketmq.Compressor do
  @moduledoc """
  This module provides compression and decompression functionality for messages sent to and received from RocketMQ.

  It defines a behavior `ExRocketmq.Compressor` that can be implemented by custom compression modules.

  ## Examples

      iex> ExRocketmq.Compressor.compress(MyCompressor, "Hello World")
      "compressed data"

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
