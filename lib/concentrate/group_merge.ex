defmodule Concentrate.GroupMerge do
  @moduledoc """
  Experimental ProducerConsumer which processes incoming messages as diffs, instead of whole feeds.

  We manage the demand from producers manually.
  * On subscription, we ask for 1 event
  * Once we've received an event, schedule a timeout for 1s
  * When the timeout happens, merge and filter the current state
  * Request new events from producers who were part of the last merge
  """
  use GenStage
  require Logger
  alias Concentrate.Encoder.GTFSRealtimeHelpers
  alias Concentrate.{Filter, TripDescriptor}
  alias Concentrate.Merge.Diff
  alias Concentrate.Mergeable

  @start_link_opts [:name]
  # allow sources some time to load
  @initial_timeout 5_000

  defstruct timeout: 1_000,
            timer: nil,
            diffs: %{},
            demand: %{},
            key_feed_items: %{},
            merged_keys: %{},
            merged_groups: %{},
            group_keys: %{},
            updated_keys: MapSet.new(),
            updated_groups: MapSet.new(),
            filters: [],
            group_filters: []

  def start_link(opts \\ []) do
    start_link_opts = Keyword.take(opts, @start_link_opts)
    opts = Keyword.drop(opts, @start_link_opts)
    GenStage.start_link(__MODULE__, opts, start_link_opts)
  end

  @impl GenStage
  def init(opts) do
    filters = Keyword.get(opts, :filters, [])
    group_filters = build_group_filters(Keyword.get(opts, :group_filters, []))
    state = %__MODULE__{filters: filters, group_filters: group_filters}

    state =
      case Keyword.fetch(opts, :timeout) do
        {:ok, timeout} -> %{state | timeout: timeout}
        _ -> state
      end

    initial_timeout = Keyword.get(opts, :initial_timeout, @initial_timeout)
    opts = Keyword.take(opts, [:subscribe_to, :dispatcher])
    opts = Keyword.put_new(opts, :dispatcher, GenStage.BroadcastDispatcher)
    state = %{state | timer: Process.send_after(self(), :timeout, initial_timeout)}
    {:consumer, state, opts}
  end

  @impl GenStage
  def handle_subscribe(:producer, _options, from, state) do
    state = %{
      state
      | diffs: Map.put(state.diffs, from, Diff.new()),
        demand: Map.put(state.demand, from, 1)
    }

    :ok = GenStage.ask(from, 1)
    {:manual, state}
  end

  def handle_subscribe(_, _, _, state) do
    {:automatic, state}
  end

  @impl GenStage
  def handle_cancel(_reason, from, state) do
    state = %{
      state
      | diffs: Map.delete(state.diffs, from),
        demand: Map.delete(state.demand, from)
    }

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_events(events, from, state) do
    state = Enum.reduce(events, state, &handle_event(&1, from, &2))

    state = %{
      state
      | demand: Map.update!(state.demand, from, fn demand -> demand - length(events) end)
    }

    state =
      if state.timer do
        state
      else
        %{state | timer: Process.send_after(self(), :timeout, state.timeout)}
      end

    {:noreply, [], state}
  end

  def handle_event(event, from, state) do
    diff = Map.fetch!(state.diffs, from)
    {diff, changes} = Diff.diff(diff, event)
    diffs = Map.put(state.diffs, from, diff)
    state = %{state | diffs: diffs}
    Enum.reduce(changes, state, &apply_change_to_state(&2, from, &1))
  end

  @impl GenStage
  def handle_info(:timeout, state) do
    # start = System.monotonic_time()
    state = update_changed_keys(state)
    state = update_changed_groups(state)
    groups = Map.values(state.merged_groups)

    state = %{state | timer: nil, demand: ask_demand(state.demand)}
    # stop = System.monotonic_time()
    # IO.inspect({:time, __MODULE__, stop - start})
    {:noreply, [groups], state}
    # {:noreply, [], state}
  end

  def handle_info(msg, state) do
    _ =
      Logger.warn(fn ->
        "unknown message to #{__MODULE__} #{inspect(self())}: #{inspect(msg)}"
      end)

    {:noreply, [], state}
  end

  def update_changed_keys(state) do
    merged_keys =
      Enum.reduce(state.updated_keys, state.merged_keys, fn key, acc ->
        with {:ok, map} <- Map.fetch(state.key_feed_items, key),
             new_value when is_map(new_value) <-
               Enum.reduce(map, nil, fn
                 {_from, item}, acc when acc != nil -> Mergeable.merge(item, acc)
                 {_from, item}, _acc -> item
               end),
             [new_value] <- Filter.run([new_value], state.filters) do
          Map.put(acc, key, new_value)
        else
          _ -> Map.delete(acc, key)
        end
      end)

    %{state | merged_keys: merged_keys, updated_keys: MapSet.new()}
  end

  def update_changed_groups(state) do
    merged_groups =
      Enum.reduce(state.updated_groups, state.merged_groups, fn group_key, acc ->
        group_keys = Map.fetch!(state.group_keys, group_key)
        filtered = Enum.flat_map(group_keys, &List.wrap(Map.get(state.merged_keys, &1)))
        grouped = GTFSRealtimeHelpers.group(filtered)

        case group_filter(grouped, state.group_filters) do
          [group] ->
            Map.put(acc, group_key, group)

          [] ->
            Map.delete(acc, group_key)
        end
      end)

    %{state | merged_groups: merged_groups, updated_groups: MapSet.new()}
  end

  defp ask_demand(demand_map) do
    for {from, demand} <- demand_map, into: %{} do
      if demand <= 0 do
        GenStage.ask(from, 1)
        {from, 1}
      else
        {from, demand}
      end
    end
  end

  defp apply_change_to_state(state, from, {:changed, key, item}) do
    group_key = Mergeable.group_key(item)

    key_feed_items =
      Map.update!(state.key_feed_items, key, fn map ->
        Map.put(map, from, item)
      end)

    group_keys =
      Map.update!(state.group_keys, group_key, fn keys ->
        MapSet.put(keys, key)
      end)

    updated_keys = MapSet.put(state.updated_keys, key)
    updated_groups = MapSet.put(state.updated_groups, group_key)

    state = %{
      state
      | key_feed_items: key_feed_items,
        updated_keys: updated_keys,
        group_keys: group_keys,
        updated_groups: updated_groups
    }

    # update the old group if the key has changed
    state =
      with {:ok, old_item} <- Map.fetch(state.merged_keys, key),
           old_group_key when old_group_key != group_key <- Mergeable.group_key(old_item) do
        %{
          state
          | updated_groups: MapSet.put(state.updated_groups, old_group_key),
            group_keys:
              Map.update!(state.group_keys, old_group_key, fn keys ->
                MapSet.delete(keys, key)
              end)
        }
      else
        _ -> state
      end

    state
  end

  defp apply_change_to_state(state, from, {:added, key, item}) do
    group_key = Mergeable.group_key(item)

    key_feed_items =
      Map.update(state.key_feed_items, key, %{from => item}, fn map ->
        Map.put(map, from, item)
      end)

    group_keys =
      case state.group_keys do
        %{^group_key => keys} = group_keys ->
          %{group_keys | group_key => MapSet.put(keys, key)}

        %{} = group_keys ->
          Map.put(group_keys, group_key, MapSet.new([key]))
      end

    updated_keys = MapSet.put(state.updated_keys, key)
    updated_groups = MapSet.put(state.updated_groups, group_key)

    %{
      state
      | key_feed_items: key_feed_items,
        updated_keys: updated_keys,
        group_keys: group_keys,
        updated_groups: updated_groups
    }
  end

  defp apply_change_to_state(state, from, {:removed, key, item}) do
    group_key = Mergeable.group_key(item)

    key_feed_items =
      Map.update!(state.key_feed_items, key, fn map ->
        Map.delete(map, from)
      end)

    key_feed_items =
      if Map.fetch!(key_feed_items, key) == %{} do
        Map.delete(key_feed_items, key)
      else
        key_feed_items
      end

    group_keys =
      case state.group_keys do
        %{^group_key => keys} = group_keys ->
          %{group_keys | group_key => MapSet.delete(keys, key)}
      end

    updated_keys = MapSet.put(state.updated_keys, key)
    updated_groups = MapSet.put(state.updated_groups, group_key)

    %{
      state
      | key_feed_items: key_feed_items,
        updated_keys: updated_keys,
        group_keys: group_keys,
        updated_groups: updated_groups
    }
  end

  defp build_group_filters(filters) do
    for filter <- filters do
      fun =
        case filter do
          filter when is_atom(filter) ->
            &filter.filter/1

          filter when is_function(filter, 1) ->
            filter
        end

      flat_mapper(fun)
    end
  end

  defp flat_mapper(fun) do
    fn value ->
      case fun.(value) do
        {%TripDescriptor{} = td, [], []} ->
          flat_map_empty_trip_descriptor(td)

        {nil, _, _} ->
          []

        other ->
          [other]
      end
    end
  end

  defp flat_map_empty_trip_descriptor(td) do
    # allow CANCELED TripDescriptors to have no vehicle or stops
    if TripDescriptor.schedule_relationship(td) == :CANCELED do
      [{td, [], []}]
    else
      []
    end
  end

  defp group_filter(groups, filters) do
    Enum.reduce(filters, groups, fn filter, groups ->
      Enum.flat_map(groups, filter)
    end)
  end
end
