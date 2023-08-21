defmodule ExRocketmq.Remote do
  @moduledoc """
  The remote layer of the rocketmq: how client communicates with the nameserver
  """

  alias ExRocketmq.{
    Transport,
    Typespecs,
    Remote.Serializer,
    Remote.Message,
    Remote.Waiter,
    Util.Queue,
    Util.Random
  }

  require Logger
  require Message

  use GenServer

  @remote_opts_schema [
    transport: [
      type: :any,
      required: true,
      doc: "The transport instance of the remote, default tcp"
    ],
    serializer: [
      type: :any,
      default: Serializer.Json.new(),
      doc: "The serializer of the remote"
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The other options of the remote"
    ]
  ]

  @type remote_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@remote_opts_schema))]

  @doc """
  make a rpc call to the remote server, and return the response

  ## Examples

      iex> {:ok, _res} = ExRocketmq.Remote.rpc(remote, msg)
  """
  @spec rpc(pid(), Message.t()) :: {:ok, Message.t()} | {:error, any()}
  def rpc(remote, msg), do: GenServer.call(remote, {:rpc, msg})

  @doc """
  send msg to the remote server, and don't wait for the response

  ## Examples

      iex> :ok = ExRocketmq.Remote.one_way(remote, msg)
  """
  @spec one_way(pid(), Message.t()) :: :ok
  def one_way(remote, msg), do: GenServer.cast(remote, {:one_way, msg})

  @spec pop_notify(pid()) :: Message.t() | :empty
  def pop_notify(remote), do: GenServer.call(remote, :pop_notify)

  @spec start_link(remote_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    {opts, args} =
      opts
      |> NimbleOptions.validate!(@remote_opts_schema)
      |> Keyword.pop(:opts)

    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(opts) do
    waiter = Waiter.new(name: :"#{Random.random_uuid(12)}")
    {:ok, _} = Waiter.start_link(waiter: waiter)
    {:ok, notify} = Queue.start_link()

    {:ok,
     %{
       transport: opts[:transport],
       serializer: opts[:serializer],
       waiter: waiter,
       notify: notify
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

  def handle_call(:pop_notify, _from, %{notify: queue} = state),
    do: {:reply, Queue.pop(queue), state}

  def handle_cast({:one_way, msg}, %{transport: transport, serializer: serializer} = state) do
    {:ok, data} = Serializer.encode(serializer, msg)
    Transport.output(transport, data)
    {:noreply, state}
  end

  def handle_info(
        :recv,
        %{transport: transport, serializer: serializer, waiter: waiter, notify: queue} = state
      ) do
    Logger.debug("recv: waiting")

    with {:ok, data} <- Transport.recv(transport),
         {:ok, msg} <- Serializer.decode(serializer, data) do
      if Message.response_type?(msg) do
        process_response(msg, waiter)
      else
        process_notify(msg, queue)
      end

      Process.send_after(self(), :recv, 0)
    else
      {:error, :timeout} ->
        Logger.warning(%{"msg" => "recv timeout"})
        Process.send_after(self(), :recv, 0)

      {:error, reason} ->
        # maybe reconnecting
        Logger.warning(%{"msg" => "recv error", "reason" => inspect(reason)})
        Process.send_after(self(), :recv, 2000)
    end

    {:noreply, state}
  end

  defp process_response(msg, waiter) do
    opaque = Message.message(msg, :opaque)

    Waiter.pop(waiter, opaque)
    |> case do
      nil ->
        # maybe one-way request
        :ok

      pid ->
        GenServer.reply(pid, {:ok, msg})
    end
  end

  defp process_notify(msg, queue), do: Queue.push(queue, msg)

  def terminate(reason, state) do
    Logger.warning(%{"msg" => "terminated", "reason" => inspect(reason)})
    {:ok, state}
  end
end
