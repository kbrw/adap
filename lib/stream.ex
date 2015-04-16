defmodule Adap.Stream do
  @moduledoc """
  `Adap.Stream.new/3` create a stream, it takes a source enumerable, an emitter module and a chunk size.

  - each element from the source is emitted and processed accross processes/nodes by `emitter.do_emit/2`
  - these element processing (which may take place on any node) can 
      - append elements to the source using : `Adap.Stream.emit/2`, which will be emitted in turn
      - send processed element to the stream output using `Adap.Stream.done/2`
  - the streamed elements are pulled, emitted and received by `chunk_size` in
    order to avoid message congestion if element processing is too slow.

  Let's see an example:
      
      defmodule MyEmitter do
        use Adap.Stream.Emitter
        # if augment_from_local_data(elem) returns a modified elem according to local data
        # and new_from_local_data(elem) returns new elements taken from local data and elem
        def do_emit(sink,elem) do
          Node.spawn(n1,fn->
            elem = augment_from_local_data(elem)
            emit(new_from_local_data(elem))
            Node.spawn(n2,fn->
              elem = append_local_data(elem)
              done(sink,elem)
            end)
          end)
        end
      end
      Adap.Stream.new(initial_elems,MyEmitter,200)
  """
  alias Adap.Stream.Emitter

  def new(stream,emit_mod,chunk_size \\ 200), do:
    Stream.resource(fn->start!(stream,emit_mod,chunk_size) end, &{next(&1),&1},fn _-> end)

  def emit(sink,elems) when is_list(elems), do:
    GenServer.cast(sink,{:new_elems,elems})
  def emit(sink,elems), do:
    GenServer.cast(sink,{:new_emitter,Emitter.start!(elems,sink)})

  def done(sink,elem), do:
    GenServer.cast(sink,{:done,elem})

  defp start!(elems,emit_mod,chunk_size), do:
    ({:ok,pid} = GenServer.start_link(__MODULE__,{elems,emit_mod,chunk_size});pid)

  defp next(sink), do: GenServer.call(sink,:next,:infinity)

  ###### Stream Sink GenServer callbacks ####
  use GenServer

  def init({elems,emit_mod,chunk_size}), do:
    {:ok,%{emitters: [Emitter.start!(elems,self)],elems: [],count: 0,req: nil, chunk_size: chunk_size,emit_mod: emit_mod}}

  ## when no more chunk source available, wait done_timeout to ensure a time
  ## window when you have received your last chunk elem but one of its emitted emitter arrived afterward
  @done_timeout 200
  @doc false
  def handle_info(:try_done,%{emitters: [],req: req}=state), do:
    (GenServer.reply(req,:halt);{:stop,:normal,state})
  def handle_info(:try_done,%{req: req}=state), do:
    (GenServer.reply(req,[]);{:noreply,state})

  @doc false
  def handle_call(:next,reply_to,%{emitters: []}=state) do
    Process.send_after(self,:try_done,@done_timeout)
    {:noreply,%{state|req: reply_to}}
  end
  ## make sure that chsize elems are emitted
  def handle_call(:next,reply_to,%{chunk_size: chsize}=state), do:
    {:noreply,%{state|req: reply_to}|>emit_chunk(chsize)}

  ## when sink receives an elem: reply if chunk count is reached, else buffer it
  @doc false
  def handle_cast({:done,elem},%{count: c,chunk_size: chsize}=state) when c+1 == chsize, do:
    (GenServer.reply(state.req,[elem|state.elems]) ; {:noreply,%{state|count: 0, elems: []}})
  def handle_cast({:done,elem},%{count: count, elems: elems}=state), do:
    {:noreply,%{state|count: count+1, elems: [elem|elems]}}

  ## for small emitter (list): make it local to sink (:new_elems), else create a remote Emitter and send its pid (:new_emitter)
  def handle_cast({:new_emitter,pid},state), do:
    {:noreply,%{state| emitters: [pid|state.emitters]}}
  def handle_cast({:new_elems,elems},state), do:
    {:noreply,%{state| emitters: [Emitter.start!(elems,self)|state.emitters]}}

  defp emit_chunk(%{emitters: [], count: c}=state,rem), do: 
    %{state|count: c+rem}
  defp emit_chunk(%{emitters: [emitter|rest]=emitters,emit_mod: emit_mod}=state,rem) do
    case Emitter.next(emitter,rem,emit_mod) do
      ^rem -> %{state|emitters: emitters}
      l -> emit_chunk(%{state|emitters: rest},rem-l)
    end
  end
end

defmodule Adap.Stream.Emitter do
  use GenServer

  def start!(elems,sink), do:
    ({:ok,pid}=GenServer.start_link(__MODULE__,reduce_fn(elems,sink)); pid)
  def next(emitter,n,emit_mod), do:
    GenServer.call(emitter,{:next,n,emit_mod},:infinity)

  def handle_call({:next,n,emit_mod},_,cont) do
    case cont.({:cont,{n,emit_mod}}) do
      {:suspended,_,newcont}->{:reply,n,newcont}
      {:done,{rem,_}}->{:stop,:normal,n-rem,[]}
    end
  end
  defp reduce_fn(elems,sink) do
    &Enumerable.reduce(elems,&1,fn 
      elem,{1,emit}-> spawn_link(fn->emit.do_emit(sink,elem)end); {:suspend,{0,emit}}
      elem,{rem,emit}-> spawn_link(fn->emit.do_emit(sink,elem)end); {:cont,{rem-1,emit}}
    end)
  end

  use Behaviour
  defcallback do_emit(sink :: pid,elem :: term) :: :ok
  defmacro __using__(_) do
    quote do
      @behaviour Adap.Stream.Emitter
      import Adap.Stream, only: [done: 2, emit: 2]
    end
  end
end
