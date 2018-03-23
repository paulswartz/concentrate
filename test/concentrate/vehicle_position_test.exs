defmodule Concentrate.VehiclePositionTest do
  @moduledoc false
  use ExUnit.Case, async: true
  import Concentrate.VehiclePosition
  alias Concentrate.Mergeable

  describe "Concentrate.Mergeable.merge/2" do
    test "takes the latest of the two" do
      first = new(last_updated: 1, latitude: 1, longitude: 1)
      second = new(last_updated: 2, latitude: 2, longitude: 2)
      assert Mergeable.merge(first, second) == second
      assert Mergeable.merge(second, first) == second
    end

    test "keeps the highest stop sequence if the trip hasn't changed" do
      first =
        new(
          last_updated: 1,
          latitude: 1,
          longitude: 1,
          trip_id: "trip",
          stop_sequence: 5,
          status: :STOPPED_AT
        )

      second =
        new(
          last_updated: 2,
          latitude: 2,
          longitude: 2,
          trip_id: "trip",
          stop_sequence: 1,
          status: :IN_TRANSIT_TO
        )

      expected =
        new(
          last_updated: 2,
          latitude: 2,
          longitude: 2,
          trip_id: "trip",
          stop_sequence: 5,
          status: :STOPPED_AT
        )

      assert Mergeable.merge(first, second) == expected
      assert Mergeable.merge(second, first) == expected
    end

    test "doesn't keep old sequence when switching to a new trip" do
      first =
        new(
          last_updated: 1,
          latitude: 1,
          longitude: 1,
          trip_id: "trip",
          stop_sequence: 5,
          status: :STOPPED_AT
        )

      second =
        new(
          last_updated: 2,
          latitude: 2,
          longitude: 2,
          trip_id: "new trip",
          stop_sequence: 1,
          status: :IN_TRANSIT_TO
        )

      assert Mergeable.merge(first, second) == second
      assert Mergeable.merge(second, first) == second
    end
  end
end
