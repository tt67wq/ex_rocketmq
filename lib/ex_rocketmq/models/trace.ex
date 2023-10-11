defmodule ExRocketmq.Models.Trace do
  @moduledoc """
  trace data model
  """

  alias ExRocketmq.Models.TraceItem

  @content_spliter <<1>>
  @field_spliter <<2>>

  defstruct type: :pub,
            timestamp: 0,
            region_id: "",
            region_name: "",
            group_name: "",
            cost_time: 0,
            success?: true,
            request_id: "",
            code: 0,
            items: []

  @type t :: %__MODULE__{
          type: :pub | :sub_before | :sub_after,
          timestamp: non_neg_integer(),
          region_id: String.t(),
          region_name: String.t(),
          group_name: String.t(),
          cost_time: non_neg_integer(),
          success?: boolean(),
          request_id: String.t(),
          code: non_neg_integer(),
          items: list(TraceItem.t())
        }

  @spec encode(t()) :: String.t()
  def encode(%__MODULE__{
        type: :pub,
        timestamp: ts,
        region_id: region_id,
        group_name: group_name,
        cost_time: cost_time,
        success?: success?,
        items: [
          %TraceItem{
            topic: topic,
            msg_id: msg_id,
            tags: tags,
            keys: keys,
            store_host: store_host,
            body_length: body_length,
            msg_type: msg_type,
            offset_msg_id: offset_msg_id,
            client_host: client_host
          }
          | _
        ]
      }) do
    [
      "Pub",
      "#{ts}",
      region_id,
      group_name!(group_name),
      topic!(topic),
      msg_id,
      tags,
      keys,
      store_host,
      "#{body_length}",
      "#{cost_time}",
      "#{msg_type}",
      offset_msg_id,
      (success? && "true") || "false",
      client_host
    ]
    |> Enum.join(@content_spliter)
    |> then(fn x -> x <> @field_spliter end)
  end

  def encode(%__MODULE__{
        type: :sub_before,
        timestamp: ts,
        region_id: region_id,
        group_name: group_name,
        request_id: request_id,
        items: items
      }) do
    items
    |> Enum.map_join(@content_spliter, fn x ->
      [
        "SubBefore",
        "#{ts}",
        region_id,
        group_name!(group_name),
        request_id,
        x.msg_id,
        x.retry_times,
        nullwrap(x.keys),
        x.client_host
      ]
    end)
    |> Enum.join(@field_spliter)
  end

  def encode(%__MODULE__{
        type: :sub_after,
        timestamp: ts,
        group_name: group_name,
        code: code,
        request_id: request_id,
        cost_time: cost_time,
        success?: success?,
        items: items
      }) do
    items
    |> Enum.map_join(@content_spliter, fn x ->
      [
        "SubAfter",
        request_id,
        x.msg_id,
        "#{cost_time}",
        (success? && "true") || "false",
        nullwrap(x.keys),
        "#{code}",
        "#{ts}",
        group_name!(group_name)
      ]
    end)
    |> Enum.join(@field_spliter)
  end

  defp group_name!(group_name) do
    group_name
    |> String.split("%", parts: 2)
    |> case do
      [group_name, _] -> group_name
      [group_name] -> group_name
      [] -> ""
    end
  end

  defp topic!(topic) do
    topic
    |> String.split("%", parts: 2)
    |> case do
      [topic, _] -> topic
      [topic] -> topic
      [] -> ""
    end
  end

  defp nullwrap(""), do: "null"
  defp nullwrap(s), do: s
end
