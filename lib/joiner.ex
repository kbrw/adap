defmodule Adap.Joiner do
      
  @doc """
  Make a stream wich reduces input elements joining them according to specified key pattern. 
  The principle is to keep a fixed length queue of elements waiting to
  receive joined elements. 

  This is for stream of elements where order is unknown, but elements to join
  are supposed to be close. 
  
  - each element of `enum` must be like `{:sometype,elem}`
  - you want to merge elements of type `from_type` into element of type `to_type`
  - `opts[:fk_from]` must contain an anonymous function taking an element 
  """
  def join(enum,from_type,to_type, opts \\ []) do
    opts = set_default_opts(opts,from_type,to_type)
    enum |> Stream.concat([:last]) |> Stream.transform({HashDict.new,:queue.new,0}, fn
      :last, {tolink,queue,_}-> 
        {elems,tolink} = Enum.reduce(:queue.to_list(queue),{[],tolink}, fn e,{elems,tolink}->
          {e,tolink} = merge(e,tolink,opts)
          {[{to_type,e}|elems],tolink}
        end)
        IO.puts "end join, #{Enum.count(tolink)} elements failed to join and are ignored"
        {elems,nil}
      {type,obj_from}, {tolink,queue,count} when type == from_type->
        if (fk=opts.fk_from.(obj_from)) do
          tolink = Dict.update(tolink,fk,[obj_from],& [obj_from|&1])
          {if(opts.keep, do: [{from_type,obj_from}], else: []), {tolink,queue,count}}
        else
          {[{from_type,obj_from}],{tolink,queue,count}}
        end
      {type,obj_to}, {tolink,queue,count} when type == to_type->
        {queue,count} = {:queue.in(obj_to,queue),count+1}
        if count > opts.queue_len do
          {{{:value,obj_to_merge},queue},count} = {:queue.out(queue),count-1}
          {obj,tolink} = merge(obj_to_merge,tolink,opts)
          {[{to_type,obj}],{tolink,queue,count}}
        else
          {[],{tolink,queue,count}}
        end
      {type,obj}, acc->{[{type,obj}],acc}
    end)
  end

  defp set_default_opts(opts,from_type,to_type) do
    from_types = :"#{from_type}s"
    %{fk_from: opts[:fk_from] || &(&1[to_type]),
      fk_to: opts[:fk_to] || &(&1.id),
      keep: opts[:keep] || false,
      reducer: opts[:reducer] || fn from_obj,to_obj-> Dict.update(to_obj,from_types,[from_obj],& [from_obj|&1]) end,
      queue_len: opts[:queue_len] || 10}
  end

  defp merge(obj,tolink,opts) do
    {objs_tolink,tolink} = Dict.pop(tolink,opts.fk_to.(obj),[])
    {Enum.reduce(objs_tolink,obj,opts.reducer), tolink}
  end
end
