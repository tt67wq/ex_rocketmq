defmodule ExRocketmq.Transport.Tcp do
  @moduledoc """
  Implement the transport layer of the rocketmq protocol via tcp
  """

  alias ExRocketmq.{Transport, Typespecs}

  use Connection

  require Logger

  @behaviour Transport

  @tcp_opts_schema [
    name: [
      type: :atom,
      default: __MODULE__,
      doc: "The name of the transport"
    ],
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
    ]
  ]

  defstruct [:name, :host, :port, :timeout, :sockopts]

  @type t :: %__MODULE__{
          name: Typespecs.name(),
          host: String.t(),
          port: non_neg_integer(),
          timeout: non_neg_integer(),
          sockopts: Typespecs.opts()
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
    Connection.call(transport.name, {:send, data})
  end

  @impl Transport
  def recv(transport) do
    Connection.call(transport.name, {:recv, 2000})
  end

  def child_spec(opts) do
    m = Keyword.fetch!(opts, :transport)
    %{id: {__MODULE__, m.name}, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl Transport
  def start_link(
        transport: %{
          name: name,
          host: host,
          port: port,
          timeout: timeout,
          sockopts: sockopts
        }
      ) do
    Connection.start_link(__MODULE__, {host, port, timeout, sockopts}, name: name)
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
        %{sock: nil, host: host, port: port, timeout: timeout, sockopts: sockopts, retry: 3} =
          s
      ) do
    Logger.info(%{"msg" => "backoff", "host" => host, "port" => port})

    case :gen_tcp.connect(host, port, [{:active, false} | sockopts], timeout) do
      {:ok, sock} ->
        {:ok, %{s | sock: sock, retry: 0}}

      {:error, reason} ->
        Logger.error(%{
          "reason" => reason,
          "host" => host,
          "port" => port,
          "msg" => "connect error"
        })

        {:stop, reason, s}
    end
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
    Logger.info(%{"msg" => "backoff", "host" => host, "port" => port})

    case :gen_tcp.connect(host, port, [{:active, false} | sockopts], timeout) do
      {:ok, sock} ->
        {:ok, %{s | sock: sock, retry: 0}}

      {:error, reason} ->
        Logger.error(%{
          "reason" => reason,
          "host" => host,
          "port" => port,
          "msg" => "connect error"
        })

        {:backoff, 2 ** retry * 1000, %{s | retry: retry + 1}}
    end
  end

  def connect(
        _,
        %{sock: nil, host: host, port: port, timeout: timeout, sockopts: sockopts} = s
      ) do
    Logger.info(%{"msg" => "connect", "host" => host, "port" => port})

    case :gen_tcp.connect(host, port, [{:active, false} | sockopts], timeout) do
      {:ok, sock} ->
        {:ok, %{s | sock: sock}}

      {:error, reason} ->
        Logger.error(%{
          "reason" => reason,
          "host" => host,
          "port" => port,
          "msg" => "connect error"
        })

        {:backoff, 1000, s}
    end
  end

  @impl Connection
  def disconnect(info, %{sock: sock} = s) do
    :ok = :gen_tcp.close(sock)

    case info do
      {:close, from} ->
        Connection.reply(from, :ok)

      {:error, :closed} ->
        Logger.warning(%{
          "host" => s.host,
          "port" => s.port,
          "reason" => "closed",
          "msg" => "disconnect"
        })

      {:error, reason} ->
        reason = :inet.format_error(reason)

        Logger.error(%{
          "host" => s.host,
          "port" => s.port,
          "reason" => reason,
          "msg" => "disconnect"
        })
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
      {:reply, data, s}
    else
      {:error, :timeout} = error ->
        {:reply, error, s}

      {:error, reason} ->
        Logger.error(%{
          "reason" => reason,
          "host" => s.host,
          "port" => s.port,
          "msg" => "recv error"
        })

        {:disconnect, reason, reason, s}
    end
  end

  def handle_call({:send, data}, _, %{sock: sock} = s) do
    case :gen_tcp.send(sock, data) do
      :ok ->
        {:reply, :ok, s}

      {:error, _} = error ->
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
  def terminate(reason, s) do
    Logger.warning(%{"msg" => "terminate", "host" => s.host, "port" => s.port, "reason" => reason})
  end
end
