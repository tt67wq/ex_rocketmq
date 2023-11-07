defmodule ExRocketmq.Models.Stat do
  @moduledoc false

  alias ExRocketmq.Typespecs

  defstruct pull_rt: 0,
            pull_tps: 0,
            consume_rt: 0,
            consume_ok_tps: 0,
            consume_failed_tps: 0,
            consume_failed_msgs: 0

  @type t :: %__MODULE__{
          pull_rt: float(),
          pull_tps: float(),
          consume_rt: float(),
          consume_ok_tps: float(),
          consume_failed_tps: float(),
          consume_failed_msgs: integer
        }

  @spec to_map(t()) :: Typespecs.str_dict()
  def to_map(%__MODULE__{
        pull_rt: pull_rt,
        pull_tps: pull_tps,
        consume_rt: consume_rt,
        consume_ok_tps: consume_ok_tps,
        consume_failed_tps: consume_failed_tps,
        consume_failed_msgs: consume_failed_msgs
      }) do
    %{
      "pullRT" => pull_rt,
      "pullTPS" => pull_tps,
      "consumeRT" => consume_rt,
      "consumeOKTPS" => consume_ok_tps,
      "consumeFailedTPS" => consume_failed_tps,
      "consumeFailedMsgs" => consume_failed_msgs
    }
  end
end
