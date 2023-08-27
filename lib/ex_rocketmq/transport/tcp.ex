defmodule ExRocketmq.Transport.Tcp do
  @moduledoc """
  Implement the transport layer of the rocketmq protocol via tcp
  """

  alias ExRocketmq.{Transport, Typespecs}

  use Connection

  require Logger

  @behaviour Transport

  @tcp_opts_schema [
    host: [
      type: :string,
      required: true,
      doc: "The host of the nameserver"
    ],
    port: [
      type: :integer,
      required: true,
      doc: "The port of the nameserver"
    ],
    timeout: [
      type: :integer,
      default: 5000,
      doc: "The timeout of the transport"
    ],
    sockopts: [
      type: {:list, :any},
      default: [],
      doc: "The socket options of the transport"
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The other options of the transport"
    ]
  ]

  defstruct [:pid, :host, :port, :timeout, :sockopts, :opts]

  @type t :: %__MODULE__{
          pid: pid(),
          host: String.t(),
          port: non_neg_integer(),
          timeout: non_neg_integer(),
          sockopts: Typespecs.opts(),
          opts: Typespecs.opts()
        }

  @type tcp_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@tcp_opts_schema))]

  @impl Transport
  def new(opts) do
    opts =
      opts
      |> NimbleOptions.validate!(@tcp_opts_schema)

    struct(__MODULE__, opts)
  end

  @impl Transport
  def output(transport, data) do
    Connection.call(transport.pid, {:send, data})
  end

  @impl Transport
  def recv(transport) do
    Connection.call(transport.pid, {:recv, 2000})
  end

  @impl Transport
  def start(
        %{
          host: host,
          port: port,
          timeout: timeout,
          sockopts: sockopts,
          opts: opts
        } = transport
      ) do
    {:ok, pid} = Connection.start_link(__MODULE__, {host, port, timeout, sockopts}, opts)
    {:ok, %{transport | pid: pid}}
  end

  @impl Transport
  def stop(%{pid: pid}) do
    GenServer.stop(pid, :normal)
  end

  @impl Connection
  def init({host, port, timeout, sockopts}) do
    {:connect, :init,
     %{
       host: String.to_charlist(host),
       port: port,
       timeout: timeout,
       sockopts: sockopts,
       sock: nil,
       retry: 0
     }}
  end

  @impl Connection
  def connect(
        :backoff,
        %{sock: nil, host: host, port: port, retry: 3} =
          s
      ) do
    Logger.error("retrying to connect to #{host}:#{port} after 3 attempts")
    {:stop, :connect_failed, %{s | sock: nil}}
  end

  def connect(
        :backoff,
        %{
          sock: nil,
          host: host,
          port: port,
          timeout: timeout,
          sockopts: sockopts,
          retry: retry
        } =
          s
      ) do
    Logger.info("retrying to connect to #{host}:#{port} after #{retry} attempts")

    do_connect(host, port, sockopts, timeout)
    |> case do
      {:ok, sock} ->
        {:ok, %{s | sock: sock, retry: 0}}

      {:error, reason} ->
        reason = :inet.format_error(reason)
        Logger.error("connect error: #{reason}, host: #{host}, port: #{port}")
        {:backoff, 2 ** retry * 1000, %{s | retry: retry + 1}}
    end
  end

  def connect(
        _,
        %{sock: nil, host: host, port: port, timeout: timeout, sockopts: sockopts} = s
      ) do
    do_connect(host, port, sockopts, timeout)
    |> case do
      {:ok, sock} ->
        {:ok, %{s | sock: sock}}

      {:error, reason} ->
        reason = :inet.format_error(reason)
        Logger.error("connect error: #{reason}, host: #{host}, port: #{port}")
        {:backoff, 1000, s}
    end
  end

  defp do_connect(host, port, sockopts, timeout) do
    :gen_tcp.connect(
      host,
      port,
      [:binary, {:active, false}, {:packet, :raw} | sockopts],
      timeout
    )
  end

  @impl Connection
  def disconnect(info, %{sock: sock, host: host, port: port} = s) do
    :ok = :gen_tcp.close(sock)

    case info do
      {:close, from} ->
        Connection.reply(from, :ok)

      {:error, :closed} ->
        Logger.warning("socket closed, host: #{host}, port: #{port}")

      {:error, reason} ->
        reason = :inet.format_error(reason)
        Logger.error("socket closed, host: #{host}, port: #{port}, reason: #{reason}")
    end

    {:connect, :reconnect, %{s | sock: nil}}
  end

  @impl Connection
  def handle_call(_, _, %{sock: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  def handle_call({:recv, timeout}, _, %{sock: sock} = s) do
    with {:ok, <<frame_size::size(32)>>} <- :gen_tcp.recv(sock, 4, timeout),
         {:ok, data} <- :gen_tcp.recv(sock, frame_size, timeout) do
      {:reply, {:ok, data}, s}
    else
      {:error, :timeout} = error ->
        {:reply, error, s}

      {:error, reason} = error ->
        reason = :inet.format_error(reason)
        Logger.error("recv error: #{reason}, host: #{s.host}, port: #{s.port}")
        {:disconnect, error, error, s}
    end
  end

  def handle_call({:send, data}, _, s) do
    case send_with_retry(s, data, 2) do
      :ok ->
        {:reply, :ok, s}

      error ->
        {:disconnect, error, error, s}
    end
  end

  def handle_call(:close, from, s) do
    {:disconnect, {:close, from}, s}
  end

  @impl Connection
  def handle_info(:recving, %{sock: nil} = s) do
    {:noreply, s}
  end

  @impl Connection
  def terminate(reason, %{sock: sock}) do
    Logger.warning("tcp terminated with reason: #{inspect(reason)}")

    sock
    |> is_nil()
    |> unless do
      :gen_tcp.close(sock)
    end
  end

  @spec send_with_retry(any(), binary(), non_neg_integer()) :: :ok | {:error, any()}
  defp send_with_retry(_s, _data, 0), do: {:error, :retry_exceeded}

  defp send_with_retry(%{sock: sock} = s, data, retry) do
    case :gen_tcp.send(sock, data) do
      :ok ->
        :ok

      {:error, reason} ->
        reason = :inet.format_error(reason)
        Logger.error("send error: #{reason}, host: #{s.host}, port: #{s.port}, retrying")
        send_with_retry(s, data, retry - 1)
    end
  end
end
