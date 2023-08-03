defmodule ExRocketmqTest do
  use ExUnit.Case
  doctest ExRocketmq

  test "greets the world" do
    assert ExRocketmq.hello() == :world
  end
end
