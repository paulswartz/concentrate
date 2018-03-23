defmodule Concentrate.Merge.TableTest do
  @moduledoc false
  use ExUnit.Case, async: true
  use ExUnitProperties
  import Concentrate.Merge.Table
  alias Concentrate.{Merge, Mergeable, TestMergeable}

  describe "items/2" do
    property "with one source, returns the data" do
      check all mergeables <- TestMergeable.mergeables() do
        from = :from
        table = new()
        table = add(table, from)
        table = update(table, from, mergeables)
        assert Enum.sort(items(table)) == Enum.sort(Merge.merge(mergeables))
      end
    end

    property "with multiple sources, returns the merged data" do
      check all multi_source_mergeables <- sourced_mergeables() do
        # reverse so we get the latest data
        # get the uniq sources
        expected =
          multi_source_mergeables
          |> Enum.reverse()
          |> Enum.uniq_by(&elem(&1, 0))
          |> Enum.flat_map(&elem(&1, 1))
          |> Merge.merge()

        table =
          Enum.reduce(multi_source_mergeables, new(), fn {source, mergeables}, table ->
            table
            |> add(source)
            |> update(source, mergeables)
          end)

        actual = items(table)

        assert Enum.sort(actual) == Enum.sort(expected)
      end
    end

    property "updating a source returns the latest data for that source" do
      check all all_mergeables <- list_of_mergeables() do
        from = :from
        table = new()
        table = add(table, from)
        # we only want the keys from from last item, but we want to have
        # merged in all the data from the previously received mergeables. we
        # get the keys from the last mergeable, merge everything together,
        # then filter the merged data to only the original keys.
        last = List.last(all_mergeables)
        expected_keys = MapSet.new(last, &Mergeable.key/1)
        all_merged = Enum.reduce(all_mergeables, [], &Merge.merge(Enum.concat(&1, &2)))
        expected = Enum.filter(all_merged, &(Mergeable.key(&1) in expected_keys))

        table =
          Enum.reduce(all_mergeables, table, fn mergeables, table ->
            update(table, from, mergeables)
          end)

        assert Enum.sort(items(table)) == Enum.sort(expected)
      end
    end
  end

  defp sourced_mergeables do
    list_of(
      {StreamData.atom(:alphanumeric), TestMergeable.mergeables()},
      min_length: 1,
      max_length: 3
    )
  end

  defp list_of_mergeables do
    list_of(TestMergeable.mergeables(), min_length: 1, max_length: 3)
  end
end
