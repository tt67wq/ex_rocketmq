defmodule ExRocketmq.Trace.Supervisor do
  @moduledoc false

  use Supervisor

  alias ExRocketmq.Util.{UniqId, Buffer}

  def start_link(opts) do
    {init, opts} =
      opts
      |> Keyword.pop(:opts)

    Supervisor.start_link(__MODULE__, init, opts)
  end

  def init(opts) do
    {cid, _opts} = Keyword.pop!(opts, :cid)

    children = [
      {Registry, keys: :unique, name: :"Registry.#{cid}"},
      {Task.Supervisor, name: :"Task.Supervisor.#{cid}"},
      {DynamicSupervisor, name: :"DynamicSupervisor.#{cid}"},
      {UniqId, name: :"UniqId.#{cid}"},
      {Buffer, name: :"Buffer.#{cid}", size: 4096}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
