defmodule Adap do
  @moduledoc """
  # ADAP: the Awesome Data Augmentation Pipeline

  This library allows you to create a data processing stream where elements
  will go accross nodes applying data processing rules to include data, emit
  new elements, or modify it according to locally present data.

  - `Adap.Stream` will create the stream taking an input stream and a
    module implementing `do_emit/2` to define the processing pipeline of each
    element.
  - `Adap.Unit` are processes started on demand where elements can be routed to
     use locally constructed datas
  - `Adap.Piper` allows to create a data processing pipeline (module implementing `do_emit/2`) as :
       - successive matching rules
       - external dependencies for each rule as a `Adap.Unit` spec

  See an example usage in `Adap.Piper`.
  """
  use Application; import Supervisor.Spec
  @doc false
  def start(_,_), do:
    Supervisor.start_link([
        worker(Adap.Unit.Router,[])
      ], strategy: :one_for_one)
end
