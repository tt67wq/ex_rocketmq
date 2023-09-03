defmodule ExRocketmq.Util.Assertion do
  @moduledoc """
  断言工具
  """
  defmacro do_assert(assert_fn, error_msg) do
    quote do
      unquote(assert_fn).()
      |> case do
        true -> :ok
        false -> {:error, unquote(error_msg)}
      end
    end
  end

  defmacro assert_exists(obj, error_msg) do
    quote do
      (fn -> not is_nil(unquote(obj)) end).()
      |> case do
        true -> :ok
        false -> {:error, unquote(error_msg)}
      end
    end
  end

  defmacro assert_non_exists(obj, error_msg) do
    quote do
      (fn -> is_nil(unquote(obj)) end).()
      |> case do
        true -> :ok
        false -> {:error, unquote(error_msg)}
      end
    end
  end
end
