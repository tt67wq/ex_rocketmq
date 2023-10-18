defmodule QueueTest do
  @moduledoc false
  use ExUnit.Case

  alias ExRocketmq.Util.Queue

  test "push adds an item to the queue" do
    {:ok, agent} = Queue.start_link()
    Queue.push(agent, "item")
    assert Queue.pop(agent) == "item"
  end

  test "pop returns :empty when the queue is empty" do
    {:ok, agent} = Queue.start_link()
    assert Queue.pop(agent) == :empty
  end

  test "pop returns the oldest item in the queue" do
    {:ok, agent} = Queue.start_link()
    Queue.push(agent, "item1")
    Queue.push(agent, "item2")
    assert Queue.pop(agent) == "item1"
    assert Queue.pop(agent) == "item2"
  end
end
