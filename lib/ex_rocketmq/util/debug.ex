defmodule ExRocketmq.Util.Debug do
  @moduledoc """
  Debug tools
  """
  require Logger

  def debug(msg), do: tap(msg, fn msg -> Logger.debug("[DEBUGING!!!!] => #{inspect(msg)}") end)
end
