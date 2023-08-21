defmodule ExRocketmq.Broker do
  @moduledoc """
  RocketMQ Broker Client
  """

  defmodule State do
    @moduledoc false

    defstruct [
      :broker_name,
      :remote,
      :opaque,
      :version
    ]

    @type t :: %__MODULE__{
            broker_name: String.t(),
            remote: pid(),
            opaque: non_neg_integer(),
            version: non_neg_integer()
          }
  end

  alias ExRocketmq.{
    Typespecs,
    Remote,
    Remote.Packet,
    Remote.Error,
    Remote.ExtFields,
    Models.Heartbeat,
    Models.SendMsg,
    Protocol.Request,
    Protocol.Response
  }

  require Packet
  require Request
  require Response
  require Logger

  use GenServer

  @req_send_message Request.req_send_message()
  @req_hearbeat Request.req_heartbeat()
  @resp_success Response.resp_success()

  @broker_opts_schema [
    broker_name: [
      type: :string,
      required: true,
      doc: "The name of the broker"
    ],
    remote: [
      type: :any,
      required: true,
      doc: "The remote instances of the broker"
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "The options for the broker"
    ]
  ]

  @type namesrvs_opts_schema_t :: [unquote(NimbleOptions.option_typespec(@broker_opts_schema))]

  @spec start_link(namesrvs_opts_schema_t()) :: Typespecs.on_start()
  def start_link(opts) do
    {opts, init} =
      opts
      |> NimbleOptions.validate!(@broker_opts_schema)
      |> Keyword.pop(:opts)

    GenServer.start_link(__MODULE__, init, opts)
  end

  @spec heartbeat(pid(), Heartbeat.t()) :: :ok | Typespecs.error_t()
  def heartbeat(broker, heartbeat) do
    with {:ok, body} <- Heartbeat.encode(heartbeat),
         {:ok, resp_msg} <- GenServer.call(broker, {:rpc, @req_hearbeat, body, %{}}) do
      resp_msg
      |> Packet.packet(:code)
      |> case do
        @resp_success ->
          GenServer.cast(broker, {:set_version, resp_msg |> Packet.packet(:version)})
          :ok

        code ->
          remark = resp_msg |> Packet.packet(:remark)
          {:error, Error.new(code, remark)}
      end
    end
  end

  @spec sync_send_message(pid(), SendMsg.Request.t(), binary()) ::
          {:ok, SendMsg.Response.t()} | Typespecs.error_t()
  def sync_send_message(broker, header, body) do
    with ext_fields <- ExtFields.to_map(header),
         {:ok, pkt} <- GenServer.call(broker, {:rpc, @req_send_message, body, ext_fields}),
         resp <- SendMsg.Response.from_pkt(pkt) do
      q = %{resp.queue | topic: header.topic, broker_name: GenServer.call(broker, :broker_name)}
      {:ok, %{resp | queue: q}}
    end
  end

  @spec async_send_message(pid(), SendMsg.Request.t(), binary()) :: Task.t()
  def async_send_message(broker, header, body) do
    Task.async(fn ->
      sync_send_message(broker, header, body)
    end)
  end

  @spec one_way_send_message(pid(), SendMsg.Request.t(), binary()) ::
          :ok
  def one_way_send_message(broker, header, body) do
    with ext_fields <- ExtFields.to_map(header) do
      GenServer.cast(broker, {:one_way, @req_send_message, body, ext_fields})
    end
  end

  def init(opts) do
    {:ok, remote_pid} = Remote.start_link(opts[:remote])

    {:ok, %State{broker_name: opts[:broker_name], remote: remote_pid, opaque: 0, version: 0}}
  end

  def handle_call(
        {:rpc, code, body, ext_fields},
        _from,
        %{remote: remote, opaque: opaque} = state
      ) do
    pkt =
      Packet.packet(
        code: code,
        opaque: opaque,
        ext_fields: ext_fields,
        body: body
      )

    reply =
      remote
      |> Remote.rpc(pkt)

    {:reply, reply, %{state | opaque: opaque + 1}}
  end

  def handle_call(:broker_name, _, %{broker_name: broker_name} = state),
    do: {:reply, broker_name, state}

  def handle_cast({:set_version, version}, %{version: old_version} = state) do
    if version != old_version do
      Logger.info("Broker version changed from #{old_version} to #{version}")
      {:noreply, %{state | version: version}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:one_way, code, body, ext_fields}, %{remote: remote} = state) do
    pkt =
      Packet.packet(
        code: code,
        ext_fields: ext_fields,
        body: body
      )

    remote
    |> Remote.one_way(pkt)

    {:noreply, state}
  end
end
