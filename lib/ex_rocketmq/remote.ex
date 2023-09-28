defmodule ExRocketmq.Remote do
  @moduledoc """
  The remote layer of the rocketmq: how client communicates with the nameserver
  """

  alias ExRocketmq.{
    Transport,
    Typespecs,
    Remote.Serializer,
    Remote.Packet,
    Remote.Waiter,
    Util.Queue,
    Util.Random
  }

  require Logger
  require Packet

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
  @spec rpc(pid(), Packet.t(), non_neg_integer()) :: {:ok, Packet.t()} | {:error, any()}
  def rpc(remote, pkt, timeout \\ 30_000) do
    try do
      GenServer.call(remote, {:rpc, pkt}, timeout)
    catch
      :exit, {:timeout, _} ->
        {:error, :timeout}

      :exit, reason ->
        Logger.error("remote rpc error: #{inspect(reason)}")
        exit(reason)
    end
  end

  @doc """
  send msg to the remote server, and don't wait for the response

  ## Examples

      iex> :ok = ExRocketmq.Remote.one_way(remote, msg)
  """
  @spec one_way(pid(), Packet.t()) :: :ok
  def one_way(remote, pkt), do: GenServer.cast(remote, {:one_way, pkt})

  @spec pop_notify(pid()) :: Packet.t() | :empty
  def pop_notify(remote), do: GenServer.call(remote, :pop_notify)

  @doc """
  get transport running info

  ## Examples

      iex> ExRocketmq.Remote.transport_info(remote)
      {:ok, %{host: "some host", port: 1234, pid: #PID<0.123.0>}}
  """
  @spec transport_info(pid()) :: {:ok, map()} | {:error, any()}
  def transport_info(remote), do: GenServer.call(remote, :transport_info)

  @spec start_link(remote_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    {opts, args} =
      opts
      |> NimbleOptions.validate!(@remote_opts_schema)
      |> Keyword.pop(:opts)

    GenServer.start_link(__MODULE__, args, opts)
  end

  @spec stop(pid()) :: :ok
  def stop(remote), do: GenServer.stop(remote)

  def init(opts) do
    {:ok, notify} = Queue.start_link()

    {:ok,
     %{
       transport: opts[:transport],
       serializer: opts[:serializer],
       waiter: Waiter.start(name: :"#{Random.generate_id("W")}"),
       notify: notify
     }, {:continue, :connect}}
  end

  # 启动后立即连接传输层，并且启动接收消息的定时器
  def handle_continue(:connect, %{transport: transport} = state) do
    Transport.start(transport)
    |> case do
      {:ok, t} ->
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
    Waiter.put(waiter, Packet.packet(msg, :opaque), from, ttl: 60_000)
    {:noreply, state}
  end

  def handle_call(:transport_info, _, %{transport: transport} = state),
    do: {:reply, Transport.info(transport), state}

  def handle_call(:pop_notify, _from, %{notify: queue} = state),
    do: {:reply, Queue.pop(queue), state}

  def handle_cast({:one_way, msg}, %{transport: transport, serializer: serializer} = state) do
    {:ok, data} = Serializer.encode(serializer, msg)
    Transport.output(transport, data)
    {:noreply, state}
  end

  def handle_info(
        :recv,
        %{
          transport: transport,
          serializer: serializer,
          waiter: waiter,
          notify: queue
        } = state
      ) do
    with {:ok, data} <- Transport.recv(transport),
         {:ok, pkt} <- Serializer.decode(serializer, data) do
      if Packet.response_type?(pkt) do
        process_response(pkt, waiter)
      else
        process_notify(pkt, queue)
      end

      Process.send_after(self(), :recv, 0)
    else
      {:error, :timeout} ->
        Process.send_after(self(), :recv, 0)

      {:error, reason} ->
        # maybe reconnecting
        Logger.warning("recv error: #{inspect(reason)}")
        Process.send_after(self(), :recv, 2000)
    end

    {:noreply, state}
  end

  defp process_response(pkt, waiter) do
    opaque = Packet.packet(pkt, :opaque)

    Waiter.pop(waiter, opaque)
    |> case do
      nil ->
        # maybe one-way request
        :ok

      pid ->
        GenServer.reply(pid, {:ok, pkt})
    end
  end

  defp process_notify(pkt, queue), do: Queue.push(queue, pkt)

  def terminate(reason, %{transport: transport, waiter: waiter, notify: notify}) do
    Logger.info("remote terminated with reason: #{inspect(reason)}")

    # stop the transport connection
    transport
    |> is_nil()
    |> unless do
      Transport.stop(transport)
    end

    # stop the waiter
    Waiter.stop(waiter)

    # stop the notify queue
    Queue.stop(notify)
  end
end
