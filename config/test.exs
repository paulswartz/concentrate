use Mix.Config

config :logger,
  level: :info,
  backends: []

config :concentrate, :sink_s3, ex_aws: Concentrate.TestExAws

# Configure StreamData to use more rounds in CI
if System.get_env("CI") do
  max_runs =
    case System.get_env("STREAM_DATA_MAX_RUNS") do
      nil -> 500
      value -> String.to_integer(value)
    end

  config :stream_data, max_runs: max_runs
end
