defmodule BufferTest do
  @moduledoc false
  use ExUnit.Case

  alias ExRocketmq.Util.Buffer

  setup do
    start_supervised!({Buffer, name: :my_queue, size: 10})
    [name: :my_queue]
  end

  describe "put/3" do
    test "adds items to the buffer queue" do
      assert Buffer.put(:my_queue, [1, 2, 3]) == :ok
    end

    test "returns :error if the buffer queue is full" do
      assert Buffer.put(:my_queue, Enum.to_list(1..10)) == :ok
      assert Buffer.put(:my_queue, [11]) == {:error, :full}
    end
  end

  describe "take/2" do
    test "removes and returns items from the buffer queue" do
      assert Buffer.put(:my_queue, [1, 2, 3, 4]) == :ok
      assert Buffer.put(:my_queue, [5, 6, 7]) == :ok
      assert Buffer.take(:my_queue) == [1, 2, 3, 4, 5, 6, 7]
    end
  end

  describe "size/2" do
    test "returns the size of the buffer queue" do
      assert Buffer.put(:my_queue, [1, 2, 3]) == :ok
      assert Buffer.size(:my_queue) == 3
    end
  end
end
