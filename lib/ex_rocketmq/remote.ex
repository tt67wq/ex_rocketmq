defmodule ExRocketmq.Remote do
  @moduledoc """
  The remote layer of the rocketmq: how client communicates with the nameserver
  """
  alias ExRocketmq.{Transport, Typespecs, Remote.Serializer, Remote.Message, Remote.Waiter}

  # import ExRocketmq.Util.Debug

  require Logger
  require Message

  use GenServer

  @remote_opts_schema [
    name: [
      type: {:or, [:atom, :any]},
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

  @doc """
  create a new remote instance

  ## Options
  #{NimbleOptions.docs(@remote_opts_schema)}

  ## Examples

      iex> t = Transport.Tcp.new(host: "example.com", port: 9876)
      iex> ExRocketmq.Remote.new(name: :test_remote, transport: t)
  """
  @spec new(remote_opts_schema_t()) :: t()
  def new(opts) do
    opts =
      opts
      |> NimbleOptions.validate!(@remote_opts_schema)

    struct(__MODULE__, opts)
  end

  @doc """
  make a rpc call to the remote server, and return the response

  ## Examples

      iex> {:ok, _res} = ExRocketmq.Remote.rpc(remote, msg)
  """
  @spec rpc(t() | atom() | pid(), Message.t()) :: {:ok, Message.t()} | {:error, any()}
  def rpc(remote, msg) when is_pid(remote), do: GenServer.call(remote, {:rpc, msg})
  def rpc(remote, msg) when is_atom(remote), do: GenServer.call(remote, {:rpc, msg})
  def rpc(%__MODULE__{name: name}, msg), do: GenServer.call(name, {:rpc, msg})

  @doc """
  send msg to the remote server, and don't wait for the response

  ## Examples

      iex> :ok = ExRocketmq.Remote.one_way(remote, msg)
  """
  @spec one_way(t() | atom() | pid(), Message.t()) :: :ok
  def one_way(remote, msg) when is_pid(remote), do: GenServer.cast(remote, {:one_way, msg})
  def one_way(remote, msg) when is_atom(remote), do: GenServer.cast(remote, {:one_way, msg})
  def one_way(%__MODULE__{name: name}, msg), do: GenServer.cast(name, {:one_way, msg})

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

  # 启动后立即连接传输层，并且启动接收消息的定时器
  def handle_continue(:connect, %{transport: transport} = state) do
    Transport.start(transport)
    |> case do
      {:ok, t} ->
        Logger.debug(%{"msg" => "connected", "host" => transport.host, "port" => transport.port})
        Process.send_after(self(), :recv, 0)
        {:noreply, %{state | transport: t}}

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

  def handle_cast({:one_way, msg}, %{transport: transport, serializer: serializer} = state) do
    {:ok, data} = Serializer.encode(serializer, msg)
    Transport.output(transport, data)
    {:noreply, state}
  end

  def handle_info(
        :recv,
        %{transport: transport, serializer: serializer, waiter: waiter} = state
      ) do
    Logger.debug("recv: waiting")

    with {:ok, data} <- Transport.recv(transport),
         {:ok, msg} <- Serializer.decode(serializer, data) do
      process_msg(msg, waiter)
      Process.send_after(self(), :recv, 0)
    else
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
