defmodule Concentrate.Producer.HTTPTest do
  @moduledoc false
  use ExUnit.Case, async: true
  import Concentrate.Producer.HTTP
  import Plug.Conn, only: [get_req_header: 2, put_resp_header: 3, send_resp: 3]

  describe "init/1" do
    test "parser can be a module" do
      defmodule TestParser do
        @behaviour Concentrate.Parser
        def parse(_body), do: []
      end

      assert {:producer, state} = init({"url", parser: __MODULE__.TestParser})
      assert state.parser == &__MODULE__.TestParser.parse/1
    end
  end

  describe "handle_info/2" do
    @tag :capture_log
    test "ignores unknown messages" do
      state = %Concentrate.Producer.HTTP.State{}
      assert {:noreply, [], ^state} = handle_info(:unknown, state)
    end
  end

  describe "bypass" do
    setup do
      Application.ensure_all_started(:bypass)
      Application.ensure_all_started(:httpoison)
      bypass = Bypass.open()
      {:ok, bypass: bypass}
    end

    test "does not connect without a consumer", %{bypass: bypass} do
      Bypass.down(bypass)

      {:ok, _producer} = start_producer(bypass)

      # make sure the producer doesn't crash
      assert :timer.sleep(50)
    end

    test "sends the result of parsing", %{bypass: bypass} do
      Bypass.expect_once(bypass, fn conn ->
        send_resp(conn, 200, "body")
      end)

      {:ok, producer} = start_producer(bypass)
      assert take_events(producer, 1) == [["body"]]
    end

    test "schedules a fetch again", %{bypass: bypass} do
      {:ok, agent} = response_agent()

      agent
      |> add_response(fn conn ->
        send_resp(conn, 200, "first")
      end)
      |> add_response(fn conn ->
        send_resp(conn, 200, "second")
      end)

      Bypass.expect(bypass, fn conn -> agent_response(agent, conn) end)

      {:ok, producer} = start_producer(bypass, fetch_after: 50)

      assert take_events(producer, 2) == [["first"], ["second"]]
    end

    @tag timeout: 2_000
    @tag :capture_log
    test "schedules a fetch again if there's a disconnection", %{bypass: bypass} do
      {:ok, agent} = response_agent()

      agent
      |> add_response(fn conn ->
        Bypass.down(bypass)
        Bypass.up(bypass)
        send_resp(conn, 200, "first")
      end)
      |> add_response(fn conn ->
        send_resp(conn, 200, "reconnect")
      end)

      Bypass.expect(bypass, fn conn -> agent_response(agent, conn) end)

      {:ok, producer} =
        start_producer(bypass, fetch_after: 50, get_opts: [timeout: 100, recv_timeout: 100])

      assert take_events(producer, 1) == [["reconnect"]]
      Bypass.pass(bypass)
    end

    test "if there's a cached response, retries again", %{bypass: bypass} do
      {:ok, agent} = response_agent()

      agent
      |> add_response(fn conn ->
        conn
        |> put_resp_header("Last-Modified", "last mod")
        |> put_resp_header("ETag", "tag")
        |> send_resp(200, "first")
      end)
      |> add_response(fn conn ->
        assert get_req_header(conn, "if-modified-since") == ["last mod"]
        assert get_req_header(conn, "if-none-match") == ["tag"]
        send_resp(conn, 304, "not modified")
      end)
      |> add_response(fn conn ->
        assert get_req_header(conn, "if-modified-since") == ["last mod"]
        assert get_req_header(conn, "if-none-match") == ["tag"]
        send_resp(conn, 200, "second")
      end)

      Bypass.expect(bypass, fn conn -> agent_response(agent, conn) end)

      {:ok, producer} = start_producer(bypass, fetch_after: 50)
      assert take_events(producer, 3) == [["first"], ["second"], ["agent"]]
    end

    test "if there's a redirect, fetches from the new URL", %{bypass: bypass} do
      # fetches are:
      # 1. bypass (immediate)
      # 2. temp redirect (immediate)
      # 3. bypass (after timeout)
      # 4. permanent redirect (immediate)
      # 5. permanent redirect (after timeout)
      temp_redirect = Bypass.open()
      permanent_redirect = Bypass.open()
      {:ok, agent} = response_agent()

      agent
      |> add_response(fn conn ->
        conn
        |> put_resp_header("location", "http://127.0.0.1:#{temp_redirect.port}/temp")
        |> send_resp(302, "should have temp redirected")
      end)
      |> add_response(fn conn ->
        conn
        |> put_resp_header("location", "http://127.0.0.1:#{permanent_redirect.port}/perm")
        |> send_resp(301, "should have permanently redirect")
      end)

      Bypass.expect_once(temp_redirect, fn conn ->
        send_resp(conn, 200, "in temp redirect")
      end)

      Bypass.expect(permanent_redirect, fn conn ->
        send_resp(conn, 200, "in permanent redirect")
      end)

      Bypass.expect(bypass, fn conn -> agent_response(agent, conn) end)
      {:ok, producer} = start_producer(bypass, fetch_after: 10)

      assert take_events(producer, 3) == [
               ["in temp redirect"],
               ["in permanent redirect"],
               ["in permanent redirect"]
             ]
    end

    @tag :capture_log
    test "an error in parsing isn't fatal", %{bypass: bypass} do
      {:ok, agent} = response_agent()

      agent
      |> add_response(fn conn ->
        send_resp(conn, 200, "failure")
      end)

      Bypass.expect(bypass, fn conn -> agent_response(agent, conn) end)

      {:ok, producer} =
        start_producer(
          bypass,
          fetch_after: 10,
          parser: fn
            "failure" -> throw("error")
            body -> [body]
          end
        )

      assert take_events(producer, 1) == [["agent"]]
    end

    @tag :capture_log
    test "a FunctionClauseError error in parsing isn't fatal", %{bypass: bypass} do
      {:ok, agent} = response_agent()

      agent
      |> add_response(fn conn ->
        send_resp(conn, 200, "failure")
      end)

      Bypass.expect(bypass, fn conn -> agent_response(agent, conn) end)

      {:ok, producer} =
        start_producer(
          bypass,
          fetch_after: 10,
          parser: fn "agent" -> ["agent"] end
        )

      assert take_events(producer, 1) == [["agent"]]
    end

    @tag :capture_log
    test "a fetch error is not fatal" do
      {:ok, pid} = start_link({"nodomain.dne", []})
      # this will never finish, so run it in a separate process
      Task.async(fn -> take_events(pid, 1) end)
      :timer.sleep(50)
      assert Process.alive?(pid)
    end

    defp start_producer(bypass, opts \\ []) do
      url = "http://127.0.0.1:#{bypass.port}/"
      opts = Keyword.put_new(opts, :parser, fn body -> [body] end)

      start_link({url, opts})
    end

    defp take_events(producer, event_count) do
      [{producer, max_demand: event_count}]
      |> GenStage.stream()
      |> Enum.take(event_count)
    end

    defp response_agent do
      Agent.start_link(fn -> [] end)
    end

    defp add_response(agent, fun) do
      :ok = Agent.update(agent, fn funs -> funs ++ [fun] end)
      agent
    end

    defp agent_response(agent, conn) do
      fun =
        Agent.get_and_update(agent, fn
          [] -> {&default_response/1, []}
          [fun | funs] -> {fun, funs}
        end)

      fun.(conn)
    end

    defp default_response(conn) do
      send_resp(conn, 200, "agent")
    end
  end
end
