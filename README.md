# ADAP
## Awesome (big) Data Augmentation Pipeline

Create a data stream across your information systems to query,
augment and transform data according to Elixir matching rules.

[![Build Status](https://travis-ci.org/kbrw/adap.svg?branch=master)](https://travis-ci.org/kbrw/adap)

See the [generated documentation](http://hexdocs.pm/adap) for more detailed explanations.

The principle is:

- to make each element hop from node to node in order to be processed
  using the locally present data.
- that any node at any time can emit new elements in the pipeline stream
- to construct processing units on each node on demand. They can die at any time
  to free memory or because of an exception: they will be restarted on demand. 
- to pull elements by chunk in order to allow long processing time
  without the need of any back-pressure mechanism.

Let's see a processing pipe example: 

- the input is a product stream : stream of `{:product,%{field1: value1, field2: value2}}`
- `user@jsonserver1` contains a json file "/color.json" containing a COLOR mapping
- `user@jsonserver2` contains a json file "/size.json" containing a SIZE mapping 
- you want to map product color and size according to these mappings
- you want to add a field "deleted" when the mapped color is red

```elixir
Adap.Piper.defpipe ColorPipe, [{ColorPipe.Rules,[]}]
defmodule JSONMap do
  use Adap.Unit.Simple, ttl: 1_000
  def init(mapping), do: 
    {:ok,File.read!("/#{mapping}.json") |> JSON.decode!}
  def node("color"), do: :"user@jsonserver1"
  def node("size"), do: :"user@jsonserver2"
end
defmodule ColorPipe.Rules do
  use Adap.Piper, for: :product
  defrule map_color(%{color: color}=prod,_) do
    {JSONMap,"color"},color_map->
      %{prod| color: color_map[color]}
  end
  defrule map_size(%{size: size}=prod,_) do
    {JSONMap,"size"},size_map->
      %{prod| size: size_map[size]}
  end
  defrule red_is_deleted(%{color: "red"}=prod,_) do
    Dict.put(prod,:deleted,true)
  end
end
result = [
  {:product,%{gender: "male", category: "ipad"}},
  {:product,%{color: "carmine", category: "shirt"}},
  {:product,%{color: "periwinkle", size: "xxl"}}
] |> Adap.Stream.new(ColorPipe) |> Enum.to_list
assert result == [
  {:product,%{gender: "male", category: "ipad"}},
  {:product,%{color: "red", category: "shirt", deleted: true}},
  {:product,%{color: "blue", size: "large"}}
]
```
