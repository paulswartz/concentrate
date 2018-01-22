defprotocol Concentrate.Mergeable do
  @moduledoc """
  Protocol for structures which can be merged together.
  """

  @doc """
  Returns the key used to group items for merging.
  """
  @spec key(mergeable) :: term when mergeable: struct
  def key(mergeable)

  @doc """
  Returns a key used to sort the items after merging.
  """
  @spec sort_key(mergeable) :: term when mergeable: struct
  def sort_key(mergeable)

  @doc """
  Merges two items into a list of items.
  """
  @spec merge(mergeable, mergeable) :: mergeable
        when mergeable: struct
  def merge(first, second)
end
