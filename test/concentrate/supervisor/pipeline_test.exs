defmodule Concentrate.Supervisor.PipelineTest do
  @moduledoc false
  use ExUnit.Case, async: true
  import Concentrate.Supervisor.Pipeline

  describe "children/1" do
    test "builds the right number of children" do
      # currently, the right number is 6 (2 urls + 1 merge/filter + 2
      # encoders + 1 sink)
      config = [
        sources: [
          gtfs_realtime: [
            name: "url",
            name2: "url2"
          ]
        ],
        encoders: [files: [{"filename", :module}, {"other_file", :other_mod}]],
        sinks: [filesystem: [directory: "/tmp"]]
      ]

      actual = children(config)

      assert length(actual) == 6
    end
  end
end