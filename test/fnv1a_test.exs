defmodule Fnv1aTest do
  @moduledoc false
  use ExUnit.Case

  test "hashing a binary" do
    assert ExRocketmq.Util.Fnv1a.hash("this thing") == 891_900_385
  end

  test "hashing a tuple" do
    assert ExRocketmq.Util.Fnv1a.hash({1, 2}) == 2_761_922_969
  end
end
