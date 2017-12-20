defmodule Concentrate.StopTimeUpdate do
  @moduledoc """
  Structure for representing an update to a StopTime (e.g. a predicted arrival or departure)
  """
  defstruct [
    :trip_id,
    :stop_id,
    :arrival_time,
    :departure_time,
    :stop_sequence,
    :status,
    :track,
    schedule_relationship: :SCHEDULED
  ]

  @opaque t :: %__MODULE__{}

  @doc """
  Return a new StopTimeUpdate with the data from the arguments.
  """
  @spec new(Keyword.t()) :: t
  def new(opts) when is_list(opts) do
    struct!(__MODULE__, opts)
  end

  defimpl Concentrate.Mergeable do
    def key(%{trip_id: trip_id, stop_id: stop_id, stop_sequence: stop_sequence}) do
      {trip_id, stop_id, stop_sequence}
    end

    def merge(first, second) do
      @for.new(
        trip_id: first.trip_id,
        stop_id: first.stop_id,
        stop_sequence: first.stop_sequence,
        arrival_time: time(:min_by, first.arrival_time, second.arrival_time),
        departure_time: time(:max_by, first.departure_time, second.departure_time),
        status: first.status || second.status,
        track: first.track || second.track,
        schedule_relationship:
          if first.schedule_relationship == :SCHEDULED do
            second.schedule_relationship
          else
            first.schedule_relationship
          end
      )
    end

    defp time(_, nil, time), do: time
    defp time(_, time, nil), do: time

    defp time(fun, first, second) do
      times = [first, second]
      apply(Enum, fun, [times, &DateTime.to_unix/1])
    end
  end
end