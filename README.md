# ExRocketmq

ExRocketmq is an Elixir client for Apache RocketMQ, a distributed messaging and streaming platform. 
With ExRocketmq, you can easily produce and consume messages from RocketMQ clusters, as well as manage topics and subscriptions. 
It provides a simple and flexible API for working with RocketMQ, and is designed to be easy to use and integrate into your Elixir applications.

NOTE: **This project is still under development, and is not ready for production use yet.**


## Usage

### producer
```Elixir
Mix.install([
  {:ex_rocketmq, github: "tt67wq/ex_rocketmq", branch: "master"}
])

defmodule DemoProducer do
  @moduledoc """
  producer task
  """

  use Task

  alias ExRocketmq.Producer
  alias ExRocketmq.Models.Message

  @topic "POETRY"

  @msgs "豫章故郡，洪都新府。
星分翼轸，地接衡庐。
襟三江而带五湖，控蛮荆而引瓯越。
物华天宝，龙光射牛斗之墟；人杰地灵，徐孺下陈蕃之榻。
雄州雾列，俊采星驰。
台隍枕夷夏之交，宾主尽东南之美。
都督阎公之雅望，棨戟遥临；宇文新州之懿范，襜帷暂驻。
十旬休假，胜友如云；千里逢迎，高朋满座。
腾蛟起凤，孟学士之词宗；紫电青霜，王将军之武库。
家君作宰，路出名区；童子何知，躬逢胜饯。
时维九月，序属三秋。
潦水尽而寒潭清，烟光凝而暮山紫。
俨骖騑于上路，访风景于崇阿。
临帝子之长洲，得天人之旧馆。
层峦耸翠，上出重霄；飞阁流丹，下临无地。
鹤汀凫渚，穷岛屿之萦回；桂殿兰宫，即冈峦之体势。
披绣闼，俯雕甍，山原旷其盈视，川泽纡其骇瞩。
闾阎扑地，钟鸣鼎食之家；舸舰弥津，青雀黄龙之舳。
云销雨霁，彩彻区明。
落霞与孤鹜齐飞，秋水共长天一色。
渔舟唱晚，响穷彭蠡之滨，雁阵惊寒，声断衡阳之浦。
遥襟甫畅，逸兴遄飞。
爽籁发而清风生，纤歌凝而白云遏。
睢园绿竹，气凌彭泽之樽；邺水朱华，光照临川之笔。
四美具，二难并。穷睇眄于中天，极娱游于暇日。
天高地迥，觉宇宙之无穷；兴尽悲来，识盈虚之有数。
望长安于日下，目吴会于云间。
地势极而南溟深，天柱高而北辰远。
关山难越，谁悲失路之人；萍水相逢，尽是他乡之客。
怀帝阍而不见，奉宣室以何年？
嗟乎！时运不齐，命途多舛。
冯唐易老，李广难封。
屈贾谊于长沙，非无圣主；窜梁鸿于海曲，岂乏明时？
所赖君子见机，达人知命。
老当益壮，宁移白首之心？
穷且益坚，不坠青云之志。
酌贪泉而觉爽，处涸辙以犹欢。
北海虽赊，扶摇可接；东隅已逝，桑榆非晚。
孟尝高洁，空余报国之情；阮籍猖狂，岂效穷途之哭！
勃，三尺微命，一介书生。
无路请缨，等终军之弱冠；有怀投笔，慕宗悫之长风。
舍簪笏于百龄，奉晨昏于万里。
非谢家之宝树，接孟氏之芳邻。
他日趋庭，叨陪鲤对；
今兹捧袂，喜托龙门。
杨意不逢，抚凌云而自惜；钟期既遇，奏流水以何惭？
呜呼！胜地不常，盛筵难再；兰亭已矣，梓泽丘墟。
临别赠言，幸承恩于伟饯；登高作赋，是所望于群公。
敢竭鄙怀，恭疏短引；一言均赋，四韵俱成。
请洒潘江，各倾陆海云尔。
滕王高阁临江渚，佩玉鸣鸾罢歌舞。
画栋朝飞南浦云，珠帘暮卷西山雨。
闲云潭影日悠悠，物换星移几度秋。
阁中帝子今何在？槛外长江空自流。"
  def start_link(opts) do
    Task.start_link(__MODULE__, :run, [opts])
  end

  defp get_msg() do
    @msgs
    |> String.split("\n")
    |> Enum.random()
  end

  def run(opts) do
    Producer.send_sync(:producer, [
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()}
    ])

    Process.sleep(1000)
    run(opts)
  end
end

alias ExRocketmq.{Producer, Namesrvs, Transport}

Supervisor.start_link(
  [
    {Namesrvs,
     remotes: [
       [transport: Transport.Tcp.new(host: "test.rocket-mq.net", port: 31120)]
     ],
     opts: [
       name: :namesrvs
     ]},
    {
      Producer,
      group_name: "GID_POETRY",
      namesrvs: :namesrvs,
      opts: [
        name: :producer
      ]
    },
    {
      DemoProducer,
      []
    }
  ],
  strategy: :one_for_one
)

Process.sleep(:infinity)

```

### consumer
```Elixir
Mix.install([
  {:ex_rocketmq, github: "tt67wq/ex_rocketmq", branch: "master"}
])

alias ExRocketmq.{Consumer, Namesrvs, Transport, Models.MsgSelector}

defmodule MyProcessor do
  @moduledoc """
  A mock processor for testing.
  """

  alias ExRocketmq.{
    Consumer.Processor,
    Models.MessageExt,
    Typespecs
  }

  @behaviour Processor

  defstruct []

  @type t :: %__MODULE__{}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec process(t(), Typespecs.topic(), [MessageExt.t()]) ::
          Processor.consume_result() | {:error, term()}
  def process(_, topic, msgs) do
    msgs
    |> Enum.map(fn msg -> IO.puts("#{topic}: #{msg.queue_offset} -- #{msg.message.body}") end)

    :success
  end
end

Supervisor.start_link(
  [
    {Namesrvs,
     remotes: [
       [transport: Transport.Tcp.new(host: "test.rocket-mq.net", port: 31120)]
     ],
     opts: [
       name: :namesrvs
     ]},
    {Consumer,
     consumer_group: "GID_POETRY",
     namesrvs: :namesrvs,
     processor: MyProcessor.new(),
     subscriptions: %{"POETRY" => MsgSelector.new(:tag, "*")},
     opts: [
       name: :consumer
     ]}
  ],
  strategy: :one_for_one
)

Process.sleep(:infinity)
```

More examples can be found in [examples](https://github.com/tt67wq/ex_rocketmq/tree/master/examples)

## Roadmap
- [X] Support normal message producer;
- [X] Support transactional message producer;
- [X] Support concurrent message consumer;
- [X] Support orderly message consume;
- [X] Support broadcast message consume;
- [x] Support trace report;
- [ ] Support client stats collect;
- [ ] Support request-reply mode;
- [ ] Benchmark and optimize performance;
- [ ] More document and examples;




