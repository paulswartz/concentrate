defmodule Concentrate.Merge.DiffTest do
  @moduledoc false
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias Concentrate.Merge.Diff
  alias Concentrate.TestMergeable

  describe "diff/2" do
    test "no data == no changes" do
      diff = Diff.new()
      assert {_, []} = Diff.diff(diff, [])
    end

    test "reports a change in an item" do
      diff = Diff.new()
      one = TestMergeable.new(1, 1)
      two = TestMergeable.new(1, 2)
      assert {diff, [{:added, 1, ^one}]} = Diff.diff(diff, [one])
      assert {diff, [{:changed, 1, ^two}]} = Diff.diff(diff, [two])
    end

    test "multiple item changes only report the last one" do
      items = [
        TestMergeable.new(1, 1),
        expected = TestMergeable.new(1, 2)
      ]

      diff = Diff.new()
      assert {diff, [{:added, 1, ^expected}]} = Diff.diff(diff, items)
      assert {diff, []} = Diff.diff(diff, items)
    end

    property "initially reports all items as :added" do
      check all(items <- TestMergeable.mergeables()) do
        expected = for item <- items, do: {:added, item.key, item}

        diff = Diff.new()
        {_, actual} = Diff.diff(diff, items)

        assert Enum.sort(expected) == Enum.sort(actual)
      end
    end

    property "removing all items returns them as removed" do
      check all(items <- TestMergeable.mergeables()) do
        expected = for item <- items, do: {:removed, item.key, item}

        diff = Diff.new()
        {diff, _} = Diff.diff(diff, items)
        {_, actual} = Diff.diff(diff, [])

        assert Enum.sort(actual) == Enum.sort(expected)
      end
    end

    property "applying a diff returns the latest set of items" do
      check all(
              first <- TestMergeable.mergeables(),
              second <- TestMergeable.mergeables()
            ) do
        diff = Diff.new()
        {diff, first_changes} = Diff.diff(diff, first)
        {_diff, second_changes} = Diff.diff(diff, second)
        expected = Enum.sort(second)

        actual =
          [first_changes, second_changes]
          |> Enum.concat()
          |> Enum.reduce(%{}, fn change, acc ->
            case change do
              {:added, key, item} ->
                :error = Map.fetch(acc, key)
                # ensure that :added only appears for new items
                Map.put(acc, key, item)

              {:changed, key, item} ->
                # ensure that the key already exists
                %{acc | key => item}

              {:removed, key, item} ->
                # ensure that the key existed before deleting
                %{^key => ^item} = acc
                Map.delete(acc, key)
            end
          end)
          |> Map.values()
          |> Enum.sort()

        assert expected == actual
      end
    end
  end
end
