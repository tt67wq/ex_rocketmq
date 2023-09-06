defmodule ExRocketmq.Models.MsgSelector do
  @moduledoc """
  consumer message selector
  """

  defstruct type: :tag,
            expression: "*"

  @type t :: %__MODULE__{
          type: :tag | :sql92,
          expression: String.t()
        }

  @spec tags(t()) :: list(String.t())
  def tags(%__MODULE__{type: :tag, expression: expression}) do
    case expression do
      "*" ->
        []

      _ ->
        expression
        |> String.split("||")
        |> Enum.map(&String.trim/1)
        |> Enum.uniq()
    end
  end

  def tags(_), do: []

  @spec codes(t()) :: list(String.t())
  def codes(%__MODULE__{} = m) do
    m
    |> tags()
    |> Enum.map(&ExRocketmq.Util.Hasher.hash_string/1)
    |> Enum.map(&Integer.to_string/1)
    |> Enum.uniq()
  end
end
