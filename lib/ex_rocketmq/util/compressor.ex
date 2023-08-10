defmodule ExRocketmq.Util.Compressor do
  @moduledoc """
  compress job via zlib
  """

  use Agent

  @type zstream :: :zlib.zstream()

  def start_link(opts \\ []) do
    {level, opts} = Keyword.pop(opts, :level, :best_compression)
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, level)
    Agent.start_link(fn -> z end, opts)
  end

  @spec compress(pid(), iodata()) :: binary()
  def compress(agent, data) do
    Agent.get_and_update(agent, fn z ->
      {do_compress(z, data), :zlib.deflateReset(z)}
    end)
  end

  @spec do_compress(zstream(), iodata()) :: binary()
  defp do_compress(z, data) do
    [compressed] = :zlib.deflate(z, data, :finish)
    compressed
  end
end
