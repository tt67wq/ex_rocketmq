defmodule ExRocketmq.ProcessQueue.Common do
  @moduledoc false
  alias ExRocketmq.Broker
  alias ExRocketmq.Consumer.Processor
  alias ExRocketmq.Models.BrokerData
  alias ExRocketmq.Models.ConsumerSendMsgBack
  alias ExRocketmq.Models.Message
  alias ExRocketmq.Models.MessageExt
  alias ExRocketmq.Models.Trace
  alias ExRocketmq.Models.TraceItem
  alias ExRocketmq.ProcessQueue.State
  alias ExRocketmq.Protocol.ConsumeReturnType
  alias ExRocketmq.Tracer
  alias ExRocketmq.Typespecs
  alias ExRocketmq.Util

  require ConsumeReturnType
  require Logger

  @success_return ConsumeReturnType.success()
  @failed_return ConsumeReturnType.failed()
  @exception_return ConsumeReturnType.exception()

  @spec process_with_trace(
          atom() | pid() | nil,
          Processor.t(),
          Typespecs.group_name(),
          Typespecs.topic(),
          list(MessageExt.t())
        ) :: Processor.consume_result() | {:error, term()}
  def process_with_trace(nil, processor, _group, topic, msgs) do
    Processor.process(processor, topic, msgs)
  end

  def process_with_trace(tracer, processor, group, topic, msgs) do
    items =
      Enum.flat_map(msgs, fn %MessageExt{
                               message: m,
                               msg_id: msg_id,
                               store_timestamp: store_timestamp,
                               store_size: store_size,
                               reconsume_times: reconsume_times,
                               store_host: store_host
                             } ->
        m
        |> Message.get_property("TRACE_ON")
        |> case do
          "false" ->
            []

          _ ->
            [
              %TraceItem{
                topic: topic,
                msg_id: msg_id,
                tags: Message.get_property(m, "TAGS", ""),
                keys: Message.get_property(m, "KEYS", ""),
                store_time: store_timestamp,
                body_length: store_size,
                retry_times: reconsume_times,
                client_host: Util.Network.local_ip_addr(),
                store_host: store_host
              }
            ]
        end
      end)

    before_trace = %Trace{
      type: :sub_before,
      timestamp: System.system_time(:millisecond),
      group_name: group,
      success?: true,
      items: items
    }

    begin_at = System.system_time(:millisecond)

    ret = Processor.process(processor, topic, msgs)

    after_trace = %Trace{
      type: :sub_after,
      timestamp: System.system_time(:millisecond),
      group_name: group,
      success?: ret == :success,
      cost_time: System.system_time(:millisecond) - begin_at,
      code: process_code(ret),
      items: items
    }

    Tracer.send_trace(tracer, [before_trace, after_trace])

    ret
  end

  defp process_code({:error, _}), do: @exception_return
  defp process_code(:success), do: @success_return
  defp process_code(_), do: @failed_return

  @spec send_msgs_back(list(MessageExt.t()), State.t()) :: :ok
  def send_msgs_back([], _), do: :ok

  def send_msgs_back(
        msgs,
        %State{client_id: cid, group_name: group_name, broker_data: bd, max_reconsume_times: max_reconsume_times} = state
      ) do
    broker =
      Broker.get_or_new_broker(
        bd.broker_name,
        BrokerData.master_addr(bd),
        :"Registry.#{cid}",
        :"DynamicSupervisor.#{cid}"
      )

    msgs
    |> Task.async_stream(fn msg ->
      Logger.debug("send msg back: #{msg.message.topic}-#{msg.commit_log_offset}")

      broker
      |> Broker.consumer_send_msg_back(%ConsumerSendMsgBack{
        group: group_name,
        offset: msg.commit_log_offset,
        delay_level: msg.delay_level,
        origin_msg_id: msg.msg_id,
        origin_topic: msg.message.topic,
        unit_mode: false,
        max_reconsume_times: max_reconsume_times
      })
      |> case do
        :ok ->
          nil

        _ ->
          msg
      end
    end)
    |> Enum.reduce([], fn
      {:ok, nil}, acc -> acc
      {:ok, msg}, acc -> [msg | acc]
    end)
    |> case do
      [] ->
        :ok

      msgs ->
        Logger.warning("send msgs back failed: #{inspect(msgs)}")
        Process.sleep(5000)

        send_msgs_back(msgs, state)
    end
  end
end
