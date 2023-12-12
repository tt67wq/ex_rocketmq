defmodule ExRocketmq.MixProject do
  @moduledoc false
  use Mix.Project

  @name "lib_oss"
  @repo_url "https://github.com/tt67wq/ex_rocketmq"
  @description "Elixir client for Apache RocketMQ"
  @version "0.1.0"

  def project do
    [
      app: :ex_rocketmq,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      source_url: @repo_url,
      name: @name,
      package: package(),
      description: @description
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:dev), do: ["lib", "examples"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:nimble_options, "~> 0.5"},
      {:connection, "~> 1.1"},
      {:jason, "~> 1.4"},
      {:styler, "~> 0.10", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.3", only: [:dev], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @repo_url
      }
    ]
  end
end
