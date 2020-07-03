defprotocol Concentrate.Mergeable do
  @moduledoc """
  Protocol for structures which can be merged together.
  """

  @type mergeable :: struct
  @type key :: term

  @doc """
  Returns the key used to group items for merging.
  """
  @spec key(mergeable) :: key
  def key(mergeable)

  @doc """
  Merges two items into a list of items.
  """
  @spec merge(mergeable, mergeable) :: mergeable
  def merge(first, second)

  @spec group_key(mergeable) :: key
  def group_key(mergeable)
end
