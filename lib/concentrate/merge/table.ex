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
    %{table | data: Map.put_new(data, source_name, %{})}
  end

  def remove(table, source_name) do
    %{table | data: Map.delete(table.data, source_name)}
  end

  def update(%{data: data} = table, source_name, items) do
    existing_source = Map.get(data, source_name, %{})

    item_list =
      Map.new(items, fn item ->
        module = Mergeable.impl_for!(item)
        key = {module, module.key(item)}
        # merge an existing item from the previous table
        item =
          case existing_source do
            %{^key => existing_item} -> module.merge(existing_item, item)
            _ -> item
          end

        {key, item}
      end)

    %{table | data: %{data | source_name => item_list}}
  end

  def items(%{data: empty}) when empty == %{} do
    []
  end

  def items(%{data: data}) do
    data
    |> fold_map
    |> Map.values()
  end

  defp fold_map(map) do
    :maps.fold(fn _key, items, acc -> merge_list(items, acc) end, %{}, map)
  end

  defp merge_list(items, acc) when acc == %{} do
    # if there's no acc, we don't need to merge at all
    items
  end

  defp merge_list(items, acc) do
    Map.merge(items, acc, fn {module, _}, item, existing ->
      module.merge(existing, item)
    end)
  end
end
