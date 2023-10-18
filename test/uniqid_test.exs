defmodule UniqIdTest do
  @moduledoc false
  use ExUnit.Case

  setup_all do
    {:ok, pid} = ExRocketmq.Util.UniqId.start_link()
    [agent: pid]
  end

  test "get_uniq_id returns a unique ID", %{agent: agent} do
    id1 = ExRocketmq.Util.UniqId.get_uniq_id(agent)
    id2 = ExRocketmq.Util.UniqId.get_uniq_id(agent)
    assert id1 != id2
  end

  test "get_uniq_id returns a 32-character string", %{agent: agent} do
    id = ExRocketmq.Util.UniqId.get_uniq_id(agent)
    assert String.length(id) == 32
  end
end
