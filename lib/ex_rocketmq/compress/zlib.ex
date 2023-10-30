defmodule ExRocketmq.Compress.Zlib do
  @moduledoc """
  his module provides an implementation of the `ExRocketmq.Compressor` behaviour using the zlib library.

  The `compress` function takes a binary `data` and compresses it using the zlib algorithm.
  It accepts an optional `opts` keyword list that can be used to specify the compression level. The default level is `:best_compression`.

  The `uncompress` function takes a binary `data` and decompresses it using the zlib algorithm.

  Both functions return the compressed or uncompressed binary data, respectively.

  ## Examples

      iex> compressed = ExRocketmq.Compress.Zlib.compress("hello world")
      iex> ExRocketmq.Compress.Zlib.uncompress(compressed)
      "hello world"
  """
  alias ExRocketmq.{Compressor}

  @behaviour Compressor

  @impl Compressor
  def compress(data, opts \\ [level: :best_compression]) do
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, Keyword.get(opts, :level, :best_compression))
    [compressed] = :zlib.deflate(z, data, :finish)
    :ok = :zlib.deflateEnd(z)
    :ok = :zlib.close(z)
    compressed
  end

  @impl Compressor
  def uncompress(data) do
    z = :zlib.open()
    :ok = :zlib.inflateInit(z)
    [uncompressed] = :zlib.inflate(z, data)
    :ok = :zlib.inflateEnd(z)
    :ok = :zlib.close(z)
    uncompressed
  end
end
