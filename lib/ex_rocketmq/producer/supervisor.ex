defmodule ExRocketmq.Producer.Supervisor do
  @moduledoc false
  alias ExRocketmq.{
    Util.UniqId,
    Tracer
  }

  use Supervisor

  def start_link(opts) do
    {init, opts} =
      opts
      |> Keyword.pop(:opts)

    Supervisor.start_link(__MODULE__, init, opts)
  end

  def init(opts) do
    {cid, opts} = Keyword.pop!(opts, :cid)

    children = [
      {Registry, keys: :unique, name: :"Registry.#{cid}"},
      {UniqId, name: :"UniqId.#{cid}"},
      {Task.Supervisor, name: :"Task.Supervisor.#{cid}"},
      {DynamicSupervisor, name: :"DynamicSupervisor.#{cid}"}
    ]

    children =
      if opts[:trace_enable] do
        [
          {Tracer, namesrvs: opts[:namesrvs], opts: [name: :"Tracer.#{cid}"]}
          | children
        ]
      else
        children
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
