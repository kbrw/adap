defmodule Adap.Piper do
  @moduledoc ~S"""
  Piper proposes an implementation of `Adap.Stream.Emitter` where
  the distributed processing of each element is defined as a
  succession of matching rules. 
  
  Each rule can use external data to process the element or emit new
  ones. When external data is needed, a process is spawned on the node
  containing it, will receive the element and continue to apply rules.

  The principle is to make each element hop from node to node in
  order to be processed using the locally present data.

  The element will go to the stream sink when no more rule matches.

  The `Adap.Stream` stream data by chunk, so that the
  construction of the external state server can take as much time as
  necessary without congestion: never more than the chunk size number
  of elements will be queued.

  Let's see a processing pipe example: 

  - the input is a product stream : stream of `{:product,%{field1: value1, field2: value2}}`
  - `user@jsonserver1` contains a json file "/color.json" containing a COLOR mapping
  - `user@jsonserver2` contains a json file "/size.json" containing a SIZE mapping 
  - you want to map product color and size according to these mappings
  - you want to add a field "deleted" when the mapped color is red

  This can be implemented using:

      iex> Adap.Piper.defpipe ColorPipe, [{ColorPipe.Rules,[]}]
      iex> defmodule JSONMap do
      iex>   use Adap.StateServer, ttl: 1_000
      iex>   def init(mapping) do
      iex>     {:ok,File.read!("/#{mapping}.json") |> JSON.decode!}
      iex>   end
      iex>   def node("color") do :"user@jsonserver1" end
      iex>   def node("size") do :"user@jsonserver2" end
      iex> end
      iex> defmodule ColorPipe.Rules do
      iex>   use Adap.Piper, for: :product
      iex>   defrule map_color(%{color: color}=prod,_) do
      iex>     {JSONMap,"color"},color_map->
      iex>       %{prod| color: color_map[color]}
      iex>   end
      iex>   defrule map_size(%{size: size}=prod,_) do
      iex>     {JSONMap,"size"},size_map->
      iex>       %{prod| size: size_map[size]}
      iex>   end
      iex>   defrule red_is_deleted(%{color: "red"}=prod,_) do
      iex>     Dict.put(prod,:deleted,true)
      iex>   end
      iex> end
      iex> [
      iex>   {:product,%{gender: "male", category: "ipad"}},
      iex>   {:product,%{color: "carmine", category: "shirt"}},
      iex>   {:product,%{color: "periwinkle", size: "xxl"}}
      iex> ] |> Adap.Stream.new(ColorPipe) |> Enum.to_list
      [{:product,%{gender: "male", category: "ipad"}},
       {:product,%{color: "red", category: "shirt", deleted: true}},
       {:product,%{color: "blue", size: "large"}}]
  """

  @doc false
  def next(type,elem,[{next,args}|nexts],sink), do:
    next.pipe(type,elem,args,nexts,sink)
  def next(type,elem,[],sink), do:
    Adap.Stream.done(sink,{type,elem})

  @doc false
  def wrap_result(sink,{:emit,elems},prev_elem,prev_state), do:
    (Adap.Stream.emit(sink,elems); {prev_elem,prev_state})
  def wrap_result(sink,{:emit,elems,elem},_prev_elem,prev_state), do:
    (Adap.Stream.emit(sink,elems); {elem,prev_state})
  def wrap_result(sink,{:emit,elems,elem,state},_prev_elem,_prev_state), do:
    (Adap.Stream.emit(sink,elems); {elem,state})
  def wrap_result(_sink,{:newstate,state},prev_elem,_prev_state), do:
    {prev_elem,state}
  def wrap_result(_sink,{:newstate,state,elem},_prev_elem,_prev_state), do:
    {elem,state}
  def wrap_result(_sink,elem,_prev_elem,prev_state), do:
    {elem,prev_state}

  defmacro defpipe(alias,pipers) do
    quote do
      defmodule unquote(alias) do
        use Adap.Stream.Emitter
        def do_emit(sink,{type,elem}), do:
          Adap.Piper.next(type,elem,unquote(pipers),sink)
      end
    end
  end

  defmacro __using__(opts) do
    quote do
      import Adap.Piper
      @behaviour Adap.Piper
      @rules []
      @rules_for unquote(opts[:for])
      @before_compile Adap.Piper

      def pipe(type,elem,args,nexts,sink) do
        {elem,pipe_state} = init(elem,args)
        pipe(type,init_apply_map,elem,pipe_state,nexts,sink)
      end

      def init(e,arg), do: {e,arg}

      defoverridable [init: 2]
    end
  end
  use Behaviour
  defcallback init(elem :: term,args :: term) :: {elem :: term,pipe_state :: term}

  defmacro __before_compile__(_env) do # add to the end of your module (after parsing so before compilation)
    quote do
      def pipe(type,_apply_map,elem,_pipe_state,nexts,sink) do
        Adap.Piper.next(type,elem,nexts,sink)
      end

      def init_apply_map, do:
        (@rules|>Enum.map(&{&1,false})|>Enum.into(%{}))
    end
  end


  defmacro defrule(sig,blocks) do
    {name,[elem_q,pipestate_q],guards_q} = sig_normalizer(sig)
    quote do
      @rules [unquote(name)|@rules]
      def pipe(@rules_for,%{unquote(name)=>false}=apply_map, unquote(elem_q)=prev_elem, unquote(pipestate_q)=prev_state,nexts,sink) when unquote(guards_q) do
        unquote(rule_body(blocks,name))
      end
    end
  end

  defp sig_normalizer({:when ,_,[{name,_,params},guards]}), do: {name,params,guards}
  defp sig_normalizer({name,_,params}), do: {name,params,true}

  defp rule_body([do: [{:->, _,[[server_spec|args], body]}]],name) do
    quote do
      Adap.Unit.Router.cast(unquote(server_spec), fn unquote_splicing(args)->
        spawn(fn->
          {elem,state} = Adap.Piper.wrap_result(sink,unquote(body),prev_elem,prev_state)
          pipe(@rules_for,%{apply_map|unquote(name)=>true},elem,state,nexts,sink)
        end)
      end)
    end
  end
  defp rule_body([do: body],name) do
    quote do
      {elem,state} = Adap.Piper.wrap_result(sink,unquote(body),prev_elem,prev_state)
      pipe(@rules_for,%{apply_map|unquote(name)=>true},elem,state,nexts,sink)
    end
  end
end
