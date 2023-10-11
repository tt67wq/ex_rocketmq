defmodule ExRocketmq.Exception do
  @moduledoc """
  General exception with an optional string, map, or Keyword list stored
  in exception details
  """
  defexception [:message, :details]

  @spec new(String.t() | nil, any()) :: %__MODULE__{}
  def new(message, details \\ nil) do
    %__MODULE__{message: message, details: details}
  end

  @spec message(%__MODULE__{}) :: String.t()
  def message(%__MODULE__{message: message, details: details}) do
    pfx = "** (ExRocketmq.Exception) "

    case message do
      nil -> pfx <> details(details)
      val -> pfx <> val <> details(details)
    end
  end

  defp details(e) when is_map(e), do: ": " <> (Map.to_list(e) |> inspect())
  defp details(e) when is_binary(e), do: ": " <> e
  defp details(nil), do: ""
  defp details(e), do: ": " <> inspect(e)
end
