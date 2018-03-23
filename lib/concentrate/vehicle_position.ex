defmodule Concentrate.VehiclePosition do
  @moduledoc """
  Structure for representing a transit vehicle's position.
  """
  import Concentrate.StructHelpers

  defstruct_accessors([
    :id,
    :trip_id,
    :stop_id,
    :label,
    :license_plate,
    :latitude,
    :longitude,
    :bearing,
    :speed,
    :odometer,
    :stop_sequence,
    :last_updated,
    status: :IN_TRANSIT_TO
  ])

  def new(opts) do
    # required fields
    _ = Keyword.fetch!(opts, :latitude)
    _ = Keyword.fetch!(opts, :longitude)
    super(opts)
  end

  defimpl Concentrate.Mergeable do
    alias Concentrate.VehiclePosition
    def key(%{id: id}), do: id

    @doc """
    Merging VehiclePositions takes the latest position for a given vehicle.
    """
    def merge(first, %{last_updated: nil}) do
      first
    end

    def merge(%{last_updated: nil}, second) do
      second
    end

    def merge(first, second) do
      {first, second} = in_order(first, second)
      {stop_id, stop_sequence, status} = stop_sequence_status(first, second)

      VehiclePosition.update(second, %{
        stop_id: stop_id,
        stop_sequence: stop_sequence,
        status: status
      })
    end

    defp in_order(%{last_updated: flu} = first, %{last_updated: slu} = second) when flu <= slu do
      {first, second}
    end

    defp in_order(first, second) do
      {second, first}
    end

    defp stop_sequence_status(%{trip_id: trip_id} = first, %{trip_id: trip_id} = second) do
      if second.stop_sequence < first.stop_sequence do
        {first.stop_id, first.stop_sequence, first.status}
      else
        {second.stop_id, second.stop_sequence, second.status}
      end
    end

    defp stop_sequence_status(_first, second) do
      {second.stop_id, second.stop_sequence, second.status}
    end
  end
end
