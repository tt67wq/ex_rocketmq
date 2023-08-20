defmodule ExRocketmq.Util.UniqId do
  @moduledoc """
  generate uniq id for rocketmq msg
  """

  defmodule State do
    @moduledoc false

    defstruct [
      :counter,
      :begin_ts,
      :next_ts,
      :prefix
    ]
  end

  use Agent

  @spec get_uniq_id(pid()) :: binary()
  def get_uniq_id(name) do
    :ok =
      Agent.update(name, fn %{next_ts: next, counter: counter} = state ->
        if :os.system_time(:second) > next do
          {begin, next} = get_time_range()
          %{state | begin_ts: begin, next_ts: next, counter: counter + 1}
        else
          %{state | counter: counter + 1}
        end
      end)

    Agent.get(
      name,
      fn %{prefix: prefix, counter: counter, begin_ts: begin} ->
        gap = :os.system_time(:second) - begin

        (prefix <> <<gap * 1000::big-integer-size(32), counter::big-integer-size(16)>>)
        |> Base.encode16(case: :upper)
      end
    )
  end

  def start_link(opts \\ []) do
    with pid <- get_pid(),
         {ip1, ip2, ip3, ip4} <- ExRocketmq.Util.Network.get_local_ipv4_address(),
         buf <- <<ip1, ip2, ip3, ip4, pid::big-integer-size(16), 0::size(32)>>,
         {begin, next} <- get_time_range(),
         do:
           Agent.start_link(
             fn ->
               %State{
                 counter: 0,
                 begin_ts: begin,
                 next_ts: next,
                 prefix: Base.encode16(buf, case: :upper)
               }
             end,
             opts
           )
  end

  @spec get_pid() :: non_neg_integer()
  defp get_pid() do
    System.pid()
    |> String.to_integer()
  end

  defp get_time_range() do
    %{year: y, month: m} = Date.utc_today()
    time = Time.new!(0, 0, 0)
    begin = DateTime.new!(Date.new!(y, m, 1), time) |> DateTime.to_unix()
    next = DateTime.new!(Date.new!(y, m + 1, 1), time) |> DateTime.to_unix()
    {begin, next}
  end
end
