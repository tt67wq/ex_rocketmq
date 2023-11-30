use Mix.Config

config :ex_rocketmq,
  namesrvs: {"127.0.0.1", 9876},
  brokers: [
    {"broker-a", {"127.0.0.1", 10911}}
  ],
  consumer: %{
    topic: "TopicTest",
    group: "my_consumer_group"
  },
  producer: %{
    topic: "TopicTest"
  }
