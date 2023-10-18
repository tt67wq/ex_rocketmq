defmodule CompressTest do
  @moduledoc false
  use ExUnit.Case

  alias ExRocketmq.{Compressor, Compress.Zlib, Util.Debug}

  setup_all do
    [compressor: Zlib]
  end

  test "compress and uncompress", %{compressor: c} do
    raw = "Hello Elixir"
    compressed = Compressor.compress(c, raw)
    uncompressed = Compressor.uncompress(c, compressed)
    assert raw == uncompressed
  end

  test "compress multi time", %{compressor: c} do
    a = Compressor.compress(c, "Hello Elixir")
    b = Compressor.compress(c, "Hello Elixir")
    assert a == b
  end
end
