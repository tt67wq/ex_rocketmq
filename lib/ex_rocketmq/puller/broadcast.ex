defmodule ExRocketmq.Puller.Broadcast do
  @moduledoc false

  alias ExRocketmq.{
    Util,
    Broker,
    Puller.State,
    Puller.Common,
    Consumer.BuffManager
  }

  alias ExRocketmq.Models.{
    BrokerData
  }

  require Logger

  def run(%State{next_offset: -1} = state) do
    # get remote offset
    {:ok, offset} = Common.get_next_offset(state)
    run(%{state | next_offset: offset})
  end

  def run(
        %State{
          client_id: cid,
          broker_data: bd,
          holding_msgs: []
        } = state
      ) do
    # no need to commit offset for broadcast puller
    req = Common.new_pull_request(state, 0, false)

    broker =
      bd
      |> BrokerData.slave_addr()
      |> then(fn addr ->
        Broker.get_or_new_broker(
          bd.broker_name,
          addr,
          :"Registry.#{cid}",
          :"DynamicSupervisor.#{cid}"
        )
      end)

    Common.pull_from_broker(broker, req, state)
    |> case do
      {[], _} ->
        # pull failed or no new msgs, suspend for a while
        Process.sleep(5000)
        run(state)

      {msgs, next_offset} ->
        run(%State{state | holding_msgs: msgs, next_offset: next_offset})
    end
  end

  def run(
        %State{
          topic: topic,
          queue_id: queue_id,
          holding_msgs: msgs,
          buff_manager: buff_manager
        } = state
      ) do
    {buff, _, _} = BuffManager.get_or_new(buff_manager, topic, queue_id)

    Util.Buffer.put(buff, msgs)
    |> case do
      :ok ->
        run(%State{state | holding_msgs: []})

      _ ->
        # buffer is full, suspend for a while
        Logger.warning("process queue is busy, buffer is full, suspend for a while")
        Process.sleep(1000)
        run(state)
    end
  end
end
