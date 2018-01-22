defmodule Concentrate.Merge.Table do
  @moduledoc """
  Maintains a table of merged values from different sources.

  We can be slightly clever to save ourselves some work.

  * When updating data, we can map over the items (to get the Mergeable implementation) without reversing at the end
  * Then, when building indexes, we know that the items are in reversed order and so we don't need to reverse again
  """
  defstruct data: %{}
  alias Concentrate.Mergeable

  def new do
    %__MODULE__{}
  end

  def add(%{data: data} = table, source_name) do
    %{table | data: Map.put_new(data, source_name, [])}
  end

  def remove(table, source_name) do
    %{table | data: Map.delete(table.data, source_name)}
  end

  def update(%{data: data} = table, source_name, items) do
    %{table | data: %{data | source_name => item_list(items)}}
  end

  def items(%{data: empty}) when empty == %{} do
    []
  end

  def items(%{data: data}) do
    merged =
      data
      |> fold_map
      |> Map.values()

    for {_, item} <- :lists.sort(merged) do
      item
    end
  end

  defp item_list(items) do
    for item <- items do
      module = Mergeable.impl_for!(item)
      key = module.key(item)
      {{module, key}, item}
    end
  end

  defp fold_map(map) do
    :maps.fold(fn _key, items, acc -> merge_list(items, acc) end, %{}, map)
  end

  defp merge_list(items, acc) do
    Enum.reduce(items, acc, fn {key, item}, acc ->
      {module, _} = key

      case acc do
        %{^key => {index, existing}} ->
          %{acc | key => {index, module.merge(existing, item)}}

        acc ->
          Map.put(acc, key, {module.sort_key(item), item})
      end
    end)
  end
end
