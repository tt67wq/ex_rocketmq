defmodule ExRocketmq.Exception do
  @moduledoc """
  General exception with an optional string, map, or Keyword list stored
  in exception details
  """
  defexception [:message, :details]

  def message(%__MODULE__{} = exception) do
    pfx = "** (Exception) "

    case exception.message do
      nil -> pfx <> details(exception.details)
      val -> pfx <> val <> details(exception.details)
    end
  end

  defp details(e) when is_map(e), do: ": " <> (Map.to_list(e) |> inspect())
  defp details(e) when is_binary(e), do: ": " <> e
  defp details(nil), do: ""
  defp details(e), do: ": " <> inspect(e)
end
