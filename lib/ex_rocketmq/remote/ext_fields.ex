defmodule ExRocketmq.Remote.ExtFields do
  @moduledoc """
  behavior of ext_fields
  """
  alias ExRocketmq.{Typespecs}

  @type t :: struct()

  @callback to_map(t()) :: Typespecs.str_dict()

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @doc """
  convert a struct to a jsonable string->string dict
  """
  @spec to_map(t()) :: Typespecs.str_dict()
  def to_map(m), do: delegate(m, :to_map, [])
end
