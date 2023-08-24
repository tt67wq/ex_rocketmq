defmodule ExRocketmq.Util.Random do
  @moduledoc """
  random kits
  """

  @doc """
  generate short random id

  ## Examples

  iex> generate_id("H")
  "H68203790HX446F"
  """
  @spec generate_id(String.t()) :: String.t()
  def generate_id(prefix) do
    mid =
      1..8
      |> Enum.map(fn _ -> Enum.random(0..9) end)
      |> Enum.join()

    "#{prefix}#{mid}#{gen_reference()}"
  end

  defp gen_reference() do
    min = String.to_integer("100000", 36)
    max = String.to_integer("ZZZZZZ", 36)

    max
    |> Kernel.-(min)
    |> :rand.uniform()
    |> Kernel.+(min)
    |> Integer.to_string(36)
  end
end
