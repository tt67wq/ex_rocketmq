defmodule ExRocketmq.Util.Fnv1a do
  @moduledoc ~S"""
  Hashes terms using the FNV-1a 32-bit algorithm.

  ## Examples

      iex> ExRocketmq.Util.Fnv1a.hash({1, 2})
      2761922969
      iex> ExRocketmq.Util.Fnv1a.hash("this thing")
      891900385
  """

  @offset 2_166_136_261
  @prime 16_777_619

  def hash(term) when is_binary(term) do
    _hash(@offset, 0, term)
  end

  def hash(term) do
    term
    |> :erlang.term_to_binary()
    |> hash
  end

  defp _hash(hash, byte_offset, bin) when byte_size(bin) == byte_offset do
    hash
  end

  defp _hash(hash, byte_offset, bin) do
    <<_::size(byte_offset)-binary, octet::size(8), _::binary>> = bin
    # xord = hash ^^^ octet
    xord = Bitwise.bxor(hash, octet)
    # hash = rem(xord * @prime, 2 <<< 31)
    hash = rem(xord * @prime, Bitwise.bsl(2, 31))
    _hash(hash, byte_offset + 1, bin)
  end
end
