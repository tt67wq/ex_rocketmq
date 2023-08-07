defmodule ExRocketmq.Remote do
  @moduledoc """
  The remote layer of the rocketmq: how client communicates with the nameserver
  """
  alias ExRocketmq.{Transport, Typespecs, Serializer, Message, Remote.Waiter}

  import ExRocketmq.Util.Debug

  require Logger
  require Message

  use GenServer

  @remote_opts_schema [
    name: [
      type: :atom,
      default: __MODULE__,
      doc: "The name of the remote"
    ],
    transport: [
      type: :any,
      required: true,
      doc: "The transport instance of the remote, default tcp"
    ],
    serializer: [
      type: :any,
      default: Serializer.Json.new(),
      doc: "The serializer of the remote"
    ]
  ]

  defstruct [:name, :transport, :serializer]

  @type t :: %__MODULE__{
          name: Typespecs.name(),
          serializer: Serializer.t(),
          transport: Transport.t()
        }

  @type remote_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@remote_opts_schema))]

  @spec new(remote_opts_schema_t()) :: t()
  def new(opts) do
    opts =
      opts
      |> NimbleOptions.validate!(@remote_opts_schema)

    struct(__MODULE__, opts)
  end

  @spec rpc(t(), Message.t()) :: {:ok, Message.t()} | {:error, any()}
  def rpc(remote, msg) when is_atom(remote) do
    GenServer.call(remote, {:rpc, msg})
  end

  def rpc(%__MODULE__{name: name}, msg) do
    GenServer.call(name, {:rpc, msg})
    |> debug()
  end

  @spec start_link(remote: t()) :: Typespecs.on_start()
  def start_link(remote: remote) do
    GenServer.start_link(__MODULE__, remote, name: remote.name)
  end

  def init(remote) do
    waiter = Waiter.new(name: :"#{remote.name}_waiter")
    {:ok, _} = Waiter.start_link(waiter: waiter)

    {:ok,
     %{
       transport: remote.transport,
       serializer: remote.serializer,
       # opaque => from
       waiter: waiter
     }, {:continue, :connect}}
  end

  def handle_continue(:connect, %{transport: transport} = state) do
    Transport.start_link(transport)
    |> case do
      {:ok, _pid} ->
        Logger.debug(%{"msg" => "connected", "host" => transport.host, "port" => transport.port})
        Process.send_after(self(), :recv, 0)
        {:noreply, state}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_call(
        {:rpc, msg},
        from,
        %{transport: transport, serializer: serializer, waiter: waiter} = state
      ) do
    {:ok, data} = Serializer.encode(serializer, msg)
    Transport.output(transport, data)
    Waiter.put(waiter, Message.message(msg, :opaque), from, ttl: 5000)
    {:noreply, state}
  end

  def handle_info(
        :recv,
        %{transport: transport, serializer: serializer, waiter: waiter} = state
      ) do
    Logger.debug("recv: waiting")

    Transport.recv(transport)
    |> case do
      {:ok, data} ->
        Logger.debug("recv: #{inspect(data)}")

        Serializer.decode(serializer, data)
        |> case do
          {:ok, msg} ->
            process_msg(msg, waiter)

          {:error, reason} ->
            Logger.error(%{"msg" => "decode error", "reason" => reason})
        end

        Process.send_after(self(), :recv, 0)

      {:error, :timeout} ->
        Process.send_after(self(), :recv, 1000)

      {:error, reason} ->
        # maybe reconnecting
        Logger.warning(%{"msg" => "recv error", "reason" => inspect(reason)})
        Process.send_after(self(), :recv, 2000)
    end

    {:noreply, state}
  end

  defp process_msg(msg, waiter) do
    if Message.response_type?(msg) do
      process_response(msg, waiter)
    else
      process_notify(msg)
    end
  end

  defp process_response(msg, waiter) do
    opaque = Message.message(msg, :opaque)

    Waiter.pop(waiter, opaque)
    |> case do
      nil ->
        Logger.warning(%{"msg" => "no request found", "opaque" => opaque})

      pid ->
        GenServer.reply(pid, {:ok, msg})
    end
  end

  defp process_notify(_msg) do
    # TODO
  end

  def terminate(reason, state) do
    Logger.warning(%{"msg" => "terminated", "reason" => inspect(reason)})
    {:ok, state}
  end
end
