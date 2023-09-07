defmodule ExRocketmq.OffsetStore do
  @moduledoc """
  Rocketmq offset store, manage offset for consumer
  """

  alias ExRocketmq.Typespecs
  alias ExRocketmq.Models.{MessageQueue}

  @type t :: struct()

  @callback persist_offset(store :: t(), mqs :: [MessageQueue.t()]) :: :ok
  @callback remove(store :: t(), mq :: MessageQueue.t()) :: :ok
  @callback read(store :: t(), mq :: MessageQueue.t()) :: {:ok, integer()} | {:error, any()}
  @callback update(store :: t(), mq :: MessageQueue.t(), offset :: integer()) :: :ok
  @callback get_offset(store :: t(), topic :: Typespecs.topic()) ::
              list({
                MessageQueue.t(),
                integer()
              })
end
