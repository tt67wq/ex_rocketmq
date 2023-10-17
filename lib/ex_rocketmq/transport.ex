defmodule ExRocketmq.Transport do
  @moduledoc """
  The transport layer of the rocketmq protocol, default implementation is ExRocketmq.Transport.TCP
  """
  alias ExRocketmq.{Typespecs}

  @type t :: struct()
  @type error_t :: {:error, :timeout | any()}

  @callback new(Typespecs.opts()) :: t()
  @callback start(t()) :: {:ok, t()} | error_t()
  @callback stop(t()) :: :ok
  @callback output(transport :: t(), msg :: binary()) ::
              :ok | error_t()
  @callback recv(transport :: t()) ::
              {:ok, binary()} | error_t()

  @callback info(t()) :: {:ok, map()} | error_t()

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @doc """
  start transport module
  """
  @spec start(t()) :: {:ok, t()} | error_t()
  def start(%module{} = m), do: apply(module, :start, [m])

  @doc """
  stop transport module
  """
  @spec stop(t()) :: :ok
  def stop(%module{} = m), do: apply(module, :stop, [m])

  @doc """
  output a pkt by transport layer
  """
  @spec output(t(), binary()) :: :ok | error_t()
  def output(transport, msg), do: delegate(transport, :output, [msg])

  @doc """
  recv a packet from transport layer
  """
  @spec recv(t()) :: {:ok, binary()} | error_t()
  def recv(transport), do: delegate(transport, :recv, [])

  @doc """
  get the info of transport
  """
  @spec info(t()) :: {:ok, map()} | error_t()
  def info(transport), do: delegate(transport, :info, [])
end
