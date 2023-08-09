defmodule ExRocketmq.Util.Const do
  @moduledoc """
  defmodule MyApp.Constant do
  import Const

    const :facebook_url, "http://facebook.com/rohanpujaris"

  end

  require MyApp.Constant
  @facebook_url MyApp.Constant.facebook_url  # You can use this line anywhere to get the facebook url.
  """
  defmacro const(const_name, const_value) do
    quote do
      defmacro unquote(const_name)(), do: unquote(const_value)
    end
  end
end
