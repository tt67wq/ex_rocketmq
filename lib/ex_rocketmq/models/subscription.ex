defmodule ExRocketmq.Models.Subscription do
  @moduledoc """
  subscription data model
  """

  defstruct class_filter_mode: false,
            topic: "",
            sub_string: "*",
            tags_set: [],
            code_set: [],
            sub_version: 0,
            expression_type: "TAG"

  @type t :: %__MODULE__{
          class_filter_mode: boolean(),
          topic: String.t(),
          sub_string: String.t(),
          tags_set: list(String.t()),
          code_set: list(String.t()),
          sub_version: non_neg_integer(),
          expression_type: String.t()
        }

  @spec to_map(t()) :: %{String.t() => any()}
  def to_map(t) do
    %{
      "classFilterMode" => t.class_filter_mode,
      "topic" => t.topic,
      "subString" => t.sub_string,
      "tagsSet" => t.tags_set,
      "codeSet" => t.code_set,
      "subVersion" => t.sub_version,
      "expressionType" => t.expression_type
    }
  end
end
