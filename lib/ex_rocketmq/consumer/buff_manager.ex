defmodule ExRocketmq.Consumer.BuffManager do
  @moduledoc """
  BuffManager is responsible for managing buffers for RocketMQ consumers.

  ## Usage

  To use BuffManager, you need to start it using `start_link/1` function and provide a name:

      {:ok, pid} = ExRocketmq.Consumer.BuffManager.start_link(name: :buff_manager)

  Once started, you can use the `get_or_new/3` function to retrieve or create a buffer for a specific topic and queue ID.

  ## Example

      {:buff, offset, commit} = ExRocketmq.Consumer.BuffManager.get_or_new(:buff_manager, %MessageQueue{topic: "my_topic", queue_id: 1})

  """

  use Agent

  alias ExRocketmq.{
    Util
  }

  alias ExRocketmq.Models.{
    MessageQueue
  }

  defstruct []

  @type t :: %__MODULE__{}

  @spec start_link(name: atom()) :: Agent.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Agent.start_link(__MODULE__, :init, [name], name: name)
  end

  defp supervisor(name), do: :"ds_#{name}"
  defp table(name), do: :"buff_store_#{name}"

  @spec get_or_new(name :: atom(), MessageQueue.t()) ::
          {buff :: atom(), offset :: non_neg_integer(), commit? :: boolean()}
  def get_or_new(name, %MessageQueue{topic: topic, queue_id: queue_id}) do
    tb = table(name)

    tb
    |> :ets.lookup({topic, queue_id})
    |> case do
      [] ->
        name
        |> supervisor()
        |> DynamicSupervisor.start_child(
          {Util.Buffer, [name: :"#{topic}_#{queue_id}", size: 4096]}
        )

        :ets.insert(tb, {{topic, queue_id}, :"#{topic}_#{queue_id}", 0, false})
        {:"#{topic}_#{queue_id}", 0, false}

      [{_, buff, offset, commit?}] ->
        {buff, offset, commit?}
    end
  end

  @spec delete_buff(name :: atom(), MessageQueue.t()) ::
          boolean()
  def delete_buff(name, %MessageQueue{topic: topic, queue_id: queue_id}) do
    tb = table(name)

    tb
    |> :ets.lookup({topic, queue_id})
    |> case do
      [] ->
        true

      [{_, buff, _, _}] ->
        Util.Buffer.stop(buff)
        :ets.delete(tb, {topic, queue_id})
    end
  end

  @spec update_offset(
          name :: atom(),
          mq :: MessageQueue.t(),
          offset :: non_neg_integer()
        ) ::
          boolean()
  def update_offset(name, %MessageQueue{topic: topic, queue_id: queue_id}, offset) do
    name
    |> table()
    |> :ets.update_element({topic, queue_id}, [{3, offset}, {4, true}])
  end

  def stop(name) do
    ds = supervisor(name)

    ds
    |> Util.SupervisorHelper.all_pids_under_supervisor()
    |> Enum.each(fn pid ->
      DynamicSupervisor.terminate_child(ds, pid)
    end)

    DynamicSupervisor.stop(ds)

    :ets.delete(table(name))
    Agent.stop(name)
  end

  def init(name) do
    DynamicSupervisor.start_link(name: supervisor(name), strategy: :one_for_one)
    :ets.new(table(name), [:set, :public, :named_table])
    %__MODULE__{}
  end
end
