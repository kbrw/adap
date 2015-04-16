defmodule Adap.Unit do
  @moduledoc "Behaviour describing an ADAP distributed processing unit"
  use Behaviour
  defcallback start_link(args :: term) :: {:ok,pid}
  defcallback cast(pid,fun) :: :ok
  defcallback node(args :: term) :: node
end

defmodule Adap.Unit.Router do
  @moduledoc """
  Route element to a node/process started on demand: `Adap.Unit.Router.cast({mod,arg}=unit_spec,elem)` will:

  - route the query to `mod.node(arg)`
  - see if a process for the spec `{mod,arg}` is running locally
  - if not start a process tree with `mod.start_link(arg)`
  - route the query to existing or newly created process with `mod.cast(pid,elem)`

  Processes are monitored in order to restart them on demand when they die.

  A process specification is defined as a tuple `{module,args}`: module must
  implement behaviour `Adap.Unit` with previously described callbacks.

  A Unit can represent : a GenServer, a pool of GenServers, a pool of
  node of GenServer, etc.  The reference unit is a simple GenServer:
  
  - which dies itself after a given "time to live" 
  - where the routed element is an anonymous function with one parameter
  - casting the function on server and apply it with the server state as parameter

  You can `use Adap.Unit.Simple` to take the default implementation for this
  kind of processing unit.
  """

  use GenServer
  def start_link, do: GenServer.start_link(__MODULE__,[], name: __MODULE__)

  def cast({m,a},fun), do:
    GenServer.cast({__MODULE__,m.node(a)},{:route,{m,a},fun})

  def init(_), do:
    {:ok,%{pids: HashDict.new,specs: HashDict.new}}

  def handle_cast({:route,{m,a}=spec,fun},%{pids: pids,specs: specs}=state) do
    if (pid=Dict.get(pids,spec)) do
      m.cast(pid,fun); {:noreply,state}
    else
      {:ok,pid} = m.start_link(a)
      m.cast(pid,fun)
      {:noreply,%{state| pids: Dict.put(pids,spec,pid), specs: Dict.put(specs,pid,spec)}}
    end
  end

  def handle_info({:EXIT, pid, _},%{pids: pids,specs: specs}=state), do: # no need to supervise backends, since they will be restarted by next query
    {:noreply,%{state|pids: Dict.delete(pids,Dict.fetch!(specs,pid)), specs: Dict.delete(specs,pid)}}

  def terminate(_,%{pids: pids}), do:
    Enum.each(pids,fn {_,pid}->Process.exit(pid,:shutdown) end)
end

defmodule Adap.Unit.Simple do
  defmacro __using__(opts) do
    quote do
      @behaviour Adap.Unit
      use GenServer
      def start_link(arg), do: GenServer.start_link(__MODULE__,arg)
      def cast(pid,fun), do: GenServer.cast(pid,{:apply,fun})
      def node(_), do: node
      def handle_cast({:apply,fun},state), do:
        (fun.(state); {:noreply,state,unquote(opts[:ttl])})
      def handle_info(:timeout,state), do:
        {:stop,:normal,state}
    end
  end
end
