defmodule Concentrate.Merge.Diff do
  @moduledoc """
  Returns an ongoing diff of lists of Mergeables.
  """
  alias Concentrate.Mergeable

  @opaque t :: %__MODULE__{
            keys: [Mergeable.key()],
            items: %{Mergeable.key() => Mergeable.mergeable()}
          }
  @type change :: {:added | :changed | :removed, Mergeable.key(), Mergeable.mergeable()}

  defstruct items: %{}, keys: []

  @spec new :: t()
  def new do
    %__MODULE__{}
  end

  @doc """
  Returns the list of changes between the current state and the new items.

  Each change is either:

  * {:added, key, new_item}
  * {:changed, key, changed_item}
  * {:removed, key, old_item}
  """
  @spec diff(t(), [Mergeable.mergeable()]) :: {t(), [change()]}
  def diff(diff, items) do
    old_map = diff.items
    old_keys = diff.keys
    new_map = Map.new(items, &map_pair/1)
    new_keys = Map.keys(new_map)

    added_map = Map.drop(new_map, old_keys)
    added_stream = change_stream(added_map, :added)

    removed_map = Map.drop(old_map, new_keys)
    removed_stream = change_stream(removed_map, :removed)

    overlap_map = Map.take(new_map, old_keys)

    changed_stream =
      change_stream(overlap_map, :changed, fn {key, item} -> Map.fetch!(old_map, key) != item end)

    changes = Enum.concat([added_stream, changed_stream, removed_stream])

    {%__MODULE__{items: new_map, keys: new_keys}, changes}
  end

  defp map_pair(item) do
    module = Mergeable.impl_for!(item)
    {{module, module.key(item)}, item}
  end

  defp change_stream(map, change_type) do
    Stream.map(map, fn {{_module, key}, item} -> {change_type, key, item} end)
  end

  defp change_stream(map, key, filter_fn) do
    map
    |> Stream.filter(filter_fn)
    |> change_stream(key)
  end
end
