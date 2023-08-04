defmodule ExRocketmq.Transport do
  @moduledoc """
  The transport layer of the rocketmq protocol
  """
  alias ExRocketmq.{Typespecs}

  @type t :: struct()
  @type error_t :: {:error, :timeout | any()}

  @callback new(Typespecs.opts()) :: t()
  @callback start_link(transport: t()) :: Typespecs.on_start()
  @callback output(transport :: t(), msg :: binary()) ::
              :ok | error_t()
  @callback recv(transport :: t()) ::
              {:ok, binary()} | error_t()

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @spec start_link(t()) :: Typespecs.on_start()
  def start_link(%module{} = m) do
    apply(module, :start_link, [[transport: m]])
  end

  @spec output(t(), binary()) :: :ok | error_t()
  def output(transport, msg), do: delegate(transport, :output, [msg])

  @spec recv(t()) :: {:ok, binary()} | error_t()
  def recv(transport), do: delegate(transport, :recv, [])
end
