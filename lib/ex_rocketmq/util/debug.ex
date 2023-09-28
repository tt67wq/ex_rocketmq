defmodule ExRocketmq.Util.Debug do
  @moduledoc """
  The debug tools
  """
  require Logger

  def debug(msg), do: tap(msg, fn msg -> Logger.debug("[DEBUGING!!!!] => #{inspect(msg)}") end)
end
