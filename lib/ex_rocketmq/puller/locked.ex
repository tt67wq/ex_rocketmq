defmodule ExRocketmq.Puller.Locked do
  @moduledoc """
  Similar to the normal puller, but it will first try to acquire a lock on the queue
  """

  alias ExRocketmq.Broker
  alias ExRocketmq.Consumer.BuffManager
  alias ExRocketmq.Models.BrokerData
  alias ExRocketmq.Models.Lock
  alias ExRocketmq.Puller.Common
  alias ExRocketmq.Puller.State
  alias ExRocketmq.Stats
  alias ExRocketmq.Util

  require Logger

  def run(%State{client_id: cid, group_name: group_name, broker_data: bd, mq: mq, lock_ttl: ttl} = state) when ttl <= 0 do
    # require lock first
    broker =
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.master_addr(bd),
        :"Registry.#{cid}",
        :"DynamicSupervisor.#{cid}"
      )

    req = %Lock.Req{
      consumer_group: group_name,
      client_id: cid,
      mq: [mq]
    }

    broker
    |> Broker.lock_batch_mq(req)
    |> case do
      {:ok, _} ->
        run(%State{state | lock_ttl: 30_000, last_lock_timestamp: System.system_time(:millisecond)})

      {:error, reason} ->
        Logger.error("lock mq failed, reason: #{inspect(reason)}, retry later")
        Process.sleep(5000)
        run(state)
    end
  end

  def run(%State{next_offset: -1, lock_ttl: ttl} = state) do
    # get remote offset
    now = System.system_time(:millisecond)
    {:ok, offset} = Common.get_next_offset(state)
    run(%{state | next_offset: offset, lock_ttl: new_ttl(ttl, now)})
  end

  def run(
        %State{
          client_id: cid,
          mq: mq,
          buff_manager: buff_manager,
          broker_data: bd,
          holding_msgs: [],
          lock_ttl: ttl,
          pull_cnt: pull_cnt,
          rt: rt,
          last_lock_timestamp: last_lock_timestamp
        } = state
      ) do
    # report
    Stats.puller_report(:"Stats.#{cid}", mq, true, last_lock_timestamp, pull_cnt, rt)

    now = System.system_time(:millisecond)
    {buff, commit_offset, commit?} = BuffManager.get_or_new(buff_manager, mq)
    req = Common.new_pull_request(state, commit_offset, commit?)

    if_result =
      if commit? do
        BrokerData.master_addr(bd)
      else
        BrokerData.slave_addr(bd)
      end

    broker =
      then(if_result, fn addr ->
        Broker.get_or_new_broker(bd.broker_name, addr, :"Registry.#{cid}", :"DynamicSupervisor.#{cid}")
      end)

    broker
    |> Common.pull_from_broker(req, state)
    |> case do
      {[], _, cost} ->
        # pull failed or no new msgs, suspend for a while
        Process.sleep(5000)
        run(%State{state | lock_ttl: new_ttl(ttl, now), rt: rt + cost})

      {msgs, next_offset, cost} ->
        run(%State{
          state
          | holding_msgs: msgs,
            next_offset: next_offset,
            lock_ttl: new_ttl(ttl, now),
            buff: buff,
            pull_cnt: pull_cnt + Enum.count(msgs),
            rt: rt + cost
        })
    end
  end

  def run(%State{mq: mq, holding_msgs: msgs, buff_manager: buff_manager, lock_ttl: ttl, buff: buff} = state) do
    now = System.system_time(:millisecond)

    buff =
      buff
      |> is_nil()
      |> if do
        {buff, _, _} = BuffManager.get_or_new(buff_manager, mq)
        buff
      else
        buff
      end

    buff
    |> Util.Buffer.put(msgs)
    |> case do
      :ok ->
        run(%State{state | holding_msgs: [], lock_ttl: new_ttl(ttl, now)})

      _ ->
        # buffer is full, suspend for a while
        Logger.warning("process queue is busy, buffer is full, suspend for a while")
        Process.sleep(1000)
        run(%State{state | lock_ttl: new_ttl(ttl, now)})
    end
  end

  defp new_ttl(old_ttl, since) do
    diff = System.system_time(:millisecond) - since
    old_ttl - diff
  end
end
