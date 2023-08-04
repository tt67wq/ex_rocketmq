defmodule ExRocketmq.Util.Debug do
  @moduledoc """
  The debug tools
  """
  def debug(msg), do: tap(msg, &IO.inspect(&1))
end
