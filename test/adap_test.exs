defmodule StreamTest do
  use ExUnit.Case
  import Adap.Piper

  defmodule EmitterMock do
    use Adap.Stream.Emitter
    def do_emit(sink,{:t1,e}) do
      :random.seed(:erlang.now)
      Stream.cycle([
        fn->emit(sink,[{:t2,e}]) end,
        fn->:timer.sleep(:random.uniform(200)) end,
        fn->done(sink,{:t1,e}) end,
        fn->:timer.sleep(:random.uniform(200)) end
      ]) |> Enum.slice(:random.uniform(4),4) |> Enum.each(& &1.())
    end
    def do_emit(sink,{:t2,1000}) do
      emit(sink,Stream.map(1001..1500,&{:t2,&1}))
      done(sink,{:t2,1000})
    end
    def do_emit(sink,{:t2,e}) do
      :random.seed(:erlang.now)
      :timer.sleep(:random.uniform(200))
      done(sink,{:t2,e})
    end
  end

  defmodule Source1 do
    use Adap.Unit.Simple, ttl: 1_000
    def init(arg), do: {:ok,arg}
  end

  defmodule Rules1 do
    use Adap.Piper, for: :product
    def init(elem,_args), do: {elem,[]}

    defrule has_provider(%{"provider"=>provider}=e,_), do:
      %{e| "provider"=>"#{provider}XXX"}
    defrule provider_a(%{"provider"=>"a"<>_}=e,_), do:
      Dict.put(e,"starts_with","a")
    defrule provider_b(%{"provider"=>"b"<>_}=e,_), do:
      Dict.put(e,"starts_with","b")
  end

  defmodule Rules2 do
    use Adap.Piper, for: :product
    def init(elem,_args), do: {elem,[]}

    defrule add_f1(e,_), do:
      Dict.put(e,"f1","v1")
    defrule add_f2(e,_), do:
      Dict.put(e,"f2","v2")
    defrule from_server1(%{"with_remote"=>true}=e,_) do
      {Source1,"d1"}, source_data-> Dict.put(e,"source_data",source_data)
    end
    defrule from_server2(%{"source_data"=> prev_data}=e,_) do
      {Source1,"d2"}, source_data-> %{e | "source_data"=>[prev_data,source_data]}
    end
  end

  defpipe EmitterPipe, [{Rules1,[]},{Rules2,[]}]

  @tag timeout: 30_000_000
  test "sink stream test" do
    out = Enum.map(0..1000, &{:t1,&1}) |> Adap.Stream.new(EmitterMock) |> Enum.to_list |> Enum.sort
    expected = Enum.concat(Enum.map(0..1000, &{:t1,&1}),Enum.map(0..1500, &{:t2,&1}))
    assert out == expected
  end

  test "sink stream with rules" do
    res = [{:product,%{"provider"=>"casto"}},{:product,%{"provider"=>"berenice"}}]
      |> Adap.Stream.new(EmitterPipe)
      |> Enum.sort
    assert res == [
      {:product,%{"provider"=>"castoXXX","f1"=>"v1","f2"=>"v2"}},
      {:product,%{"provider"=>"bereniceXXX","starts_with"=>"b","f1"=>"v1","f2"=>"v2"}}
    ]
  end

  test "sink stream with remote rules" do
    res = [{:product,%{"provider"=>"casto","with_remote"=>true}}]
      |> Adap.Stream.new(EmitterPipe)
      |> Enum.at(0)
    assert res == {:product,%{"provider"=>"castoXXX","f1"=>"v1","f2"=>"v2","source_data"=>["d1","d2"],"with_remote"=>true}}
  end
end
