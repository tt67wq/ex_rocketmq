defmodule ExRocketmq.Util.SupervisorHelper do
  @moduledoc false

  @spec all_pids_under_supervisor(pid()) :: list(pid())
  def all_pids_under_supervisor(broker_dynamic_supervisor) do
    broker_dynamic_supervisor
    |> DynamicSupervisor.which_children()
    |> Enum.map(fn {_, pid, _, _} -> pid end)
  end
end
