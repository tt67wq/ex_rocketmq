defmodule ExRocketmq.Compress.Zlib do
  @moduledoc """
  impl of ExRocketmq.Compressor via zlib
  """
  alias ExRocketmq.{Compressor}

  @behaviour Compressor

  @impl Compressor
  def compress(data, opts \\ [level: :best_compression]) do
    with z <- :zlib.open(),
         :ok <- :zlib.deflateInit(z, Keyword.get(opts, :level, :best_compression)),
         [compressed] = :zlib.deflate(z, data, :finish),
         :ok <- :zlib.deflateEnd(z),
         :ok <- :zlib.close(z) do
      compressed
    end
  end

  @impl Compressor
  def uncompress(data) do
    with z <- :zlib.open(),
         :ok <- :zlib.inflateInit(z),
         [uncompressed] = :zlib.inflate(z, data),
         :ok <- :zlib.inflateEnd(z),
         :ok <- :zlib.close(z) do
      uncompressed
    end
  end
end
