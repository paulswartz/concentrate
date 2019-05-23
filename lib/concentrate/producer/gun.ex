defmodule Concentrate.Producer.Gun do
  @moduledoc """
  HTTP producer implementation using Gun.
  """
  use GenStage
  require Logger
  @start_link_opts ~w(name)a
  @default_fetch_after 5_000
  @default_timeout 30_000
  @default_transport_opts [timeout: @default_timeout]
  @default_headers %{"accept-encoding" => "gzip"}

  defmodule State do
    @moduledoc false

    defstruct [
      :url,
      :parser,
      fetch_after: nil,
      opts: %{},
      transport_opts: [],
      headers: %{},
      conn: :not_connected,
      monitor: :not_connected,
      ref: :not_connected,
      demand: 0,
      response: nil,
      events: []
    ]
  end

  alias __MODULE__.State

  def start_link({url, opts}) when is_binary(url) and is_list(opts) do
    start_link_opts = Keyword.take(opts, @start_link_opts)
    opts = Keyword.drop(opts, @start_link_opts)
    GenStage.start_link(__MODULE__, {url, opts}, start_link_opts)
  end

  @impl GenStage
  def init({url, opts}) do
    opts = Map.new(opts)

    parser =
      case Map.fetch!(opts, :parser) do
        module when is_atom(module) ->
          &module.parse(&1, [])

        {module, opts} when is_atom(module) and is_list(opts) ->
          &module.parse(&1, opts)

        fun when is_function(fun, 1) ->
          fun
      end

    state = %State{
      url: url,
      parser: parser,
      fetch_after: Map.get(opts, :fetch_after, @default_fetch_after),
      transport_opts:
        Keyword.take(Map.get(opts, :get_opts, @default_transport_opts), ~w(timeout send_timeout)a),
      headers: Map.merge(@default_headers, Map.get(opts, :headers, %{})),
      opts: opts
    }

    {
      :producer,
      state,
      dispatcher: GenStage.BroadcastDispatcher
    }
  end

  @impl GenStage
  def handle_demand(new_demand, %{demand: existing_demand} = state) do
    demand = new_demand + existing_demand
    state = %{state | demand: demand}

    state =
      if existing_demand == 0 do
        make_request(state, state.url)
      else
        state
      end

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({:fetch, url}, state) do
    state =
      if state.demand > 0 do
        make_request(state, url)
      else
        state
      end

    {:noreply, [], state}
  end

  def handle_info({:gun_up, conn, _protocol}, %{conn: conn} = state) do
    {:noreply, [], state}
  end

  def handle_info(
        {:gun_response, conn, ref, fin, status, headers},
        %{ref: ref, conn: conn} = state
      ) do
    cache_headers =
      Enum.reduce(headers, state.headers, fn {header, value}, acc ->
        case String.downcase(header) do
          "last-modified" ->
            Map.put(acc, "if-modified-since", value)

          "etag" ->
            Map.put(acc, "if-none-match", value)

          _ ->
            acc
        end
      end)

    # don't use if-none-match if we already have if-modified-since
    cache_headers =
      case cache_headers do
        %{"if-modified-since" => _, "if-none-match" => _} ->
          Map.delete(cache_headers, "if-none-match")

        _ ->
          cache_headers
      end

    state = %{state | response: {status, headers, []}, headers: cache_headers}

    handle_http_response(state, fin)
  end

  def handle_info({:gun_data, conn, ref, fin, data}, %{ref: ref, conn: conn} = state) do
    {status, headers, body} = state.response

    state = %{state | response: {status, headers, [body | data]}}
    handle_http_response(state, fin)
  end

  def handle_info(
        {:gun_error, conn, ref, {type, instance, description}},
        %{conn: conn, ref: ref} = state
      ) do
    log_message(:warn, state, fn ->
      "error type=#{type} instance=#{instance} description=#{inspect(Atom.to_string(description))}"
    end)

    state = disconnect(state)
    state = fetch_again!(state)
    {:noreply, [], state}
  end

  def handle_info({:fetch_timeout, ref}, %{ref: ref} = state) do
    log_message(:warn, state, fn -> "fetch timed out, disconnecting" end)

    state = disconnect(state)
    state = fetch_again!(state)
    {:noreply, [], state}
  end

  def handle_info({:fetch_timeout, _}, state) do
    {:noreply, [], state}
  end

  def handle_info({:DOWN, monitor, _, conn, reason}, %{conn: conn, monitor: monitor} = state) do
    log_message(:warn, state, fn -> "process down reason=#{reason}" end)
    state = disconnect(state)
    state = fetch_again!(state)
    {:noreply, [], state}
  end

  def handle_info(message, state) do
    log_message(:warn, state, fn ->
      "unknown message message=#{inspect(message)} state=#{inspect(state)}"
    end)

    {:noreply, [], state}
  end

  def make_request(state, url) do
    state = connect(state, url)

    if state.conn != :not_connected do
      {_, _, _, path} = parse_url(url)
      ref = :gun.get(state.conn, path, Map.to_list(state.headers))

      Process.send_after(
        self(),
        {:fetch_timeout, ref},
        Keyword.get(state.transport_opts, :timeout)
      )

      %{state | ref: ref}
    else
      fetch_again!(state)
    end
  end

  defp connect(%{conn: :not_connected} = state, url) do
    {transport, host, port, _} = parse_url(url)

    case :gun.open(host, port, %{transport: transport}) do
      {:ok, conn} ->
        monitor = Process.monitor(conn)
        %{state | conn: conn, monitor: monitor}

      {:error, _} ->
        state
    end
  end

  defp connect(state, url) do
    state
    |> disconnect()
    |> connect(url)
  end

  defp disconnect(state) do
    if state.monitor != :not_connected do
      Process.demonitor(state.monitor)
    end

    if state.conn != :not_connected do
      :ok = :gun.close(state.conn)
    end

    %{state | conn: :not_connected, ref: :not_connected, monitor: :not_connected}
  end

  defp handle_http_response(state, :nofin) do
    {:noreply, [], state}
  end

  defp handle_http_response(%{response: {200, headers, body}} = state, _) do
    body = decode_body(body, find_header(headers, "content-encoding"))
    {time, parsed} = :timer.tc(state.parser, [body])

    log_message(:info, state, fn ->
      "updated: records=#{length(parsed)} time=#{time / 1000}"
    end)

    state = %{
      state
      | demand: max(state.demand - 1, 0),
        response: nil
    }

    state = fetch_again!(state)
    {:noreply, [parsed], state}
  rescue
    error ->
      state = log_parse_error(error, state, System.stacktrace())
      state = fetch_again!(state)
      {:noreply, [], state}
  catch
    error ->
      state = log_parse_error(error, state, System.stacktrace())
      state = fetch_again!(state)
      {:noreply, [], state}
  end

  defp handle_http_response(%{response: {redirect, headers, _body}} = state, _)
       when redirect in [301, 302] do
    {:ok, location} = find_header(headers, "location")
    state = disconnect(state)

    state =
      if redirect == 301 do
        state = %{state | url: location}
        fetch_again!(state, fetch_after: 0)
      else
        fetch_again!(state, url: location, fetch_after: 0)
      end

    {:noreply, [], state}
  end

  defp handle_http_response(%{response: {304, _headers, _body}} = state, _) do
    log_message(:info, state, fn -> "not modified status=304" end)
    state = fetch_again!(state)
    {:noreply, [], state}
  end

  defp handle_http_response(%{response: {404, _headers, _body}} = state, _) do
    log_message(:warn, state, fn -> "not found status=404" end)
    state = fetch_again!(state)
    {:noreply, [], state}
  end

  defp decode_body(body, {:ok, "gzip"}) do
    :zlib.gunzip(body)
  end

  defp decode_body(body, _) do
    IO.iodata_to_binary(body)
  end

  defp fetch_again!(state, opts \\ []) do
    _ =
      if state.demand > 0 do
        url = Keyword.get(opts, :url, state.url)
        fetch_after = Keyword.get(opts, :fetch_after, state.fetch_after)
        Process.send_after(self(), {:fetch, url}, fetch_after)
      end

    state
  end

  def find_header(headers, query) do
    value =
      Enum.find_value(headers, fn {header, value} ->
        if String.downcase(header) == query do
          value
        else
          nil
        end
      end)

    if value do
      {:ok, value}
    else
      :error
    end
  end

  defp log_parse_error(error, machine, trace) do
    _ =
      Logger.error(fn ->
        "#{__MODULE__}: parse error url=#{machine.url} error=#{inspect(error)}\n#{
          Exception.format_stacktrace(trace)
        }"
      end)

    machine
  end

  @doc """
  Parse URL into the pieces needed for connecting via Gun.

  iex> parse_url("https://mbta.com/developers")
  {:tls, 'mbta.com', 443, "/developers"}

  iex> parse_url("http://localhost:8080/path?query=string#fragement")
  {:tcp, 'localhost', 8080, "/path?query=string"}
  """
  def parse_url(url) when is_binary(url) do
    uri = URI.parse(url)

    transport =
      case uri.scheme do
        "https" -> :tls
        "http" -> :tcp
        nil -> :tcp
      end

    path =
      if uri.query do
        "#{uri.path}?#{uri.query}"
      else
        uri.path
      end

    {transport, String.to_charlist(uri.host), uri.port, path}
  end

  defp log_message(level, state, log_fn) do
    _ =
      Logger.log(level, fn ->
        "#{__MODULE__} #{log_fn.()} url=#{inspect(state.url)}"
      end)

    :ok
  end
end
