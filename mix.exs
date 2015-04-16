defmodule Adap.Mixfile do
  use Mix.Project

  def project do
    [app: :adap,
     version: "0.0.1",
     elixir: "~> 1.0",
     docs: [
       main: "Adap",
       source_url: "https://github.com/awetzel/adap",
       source_ref: "master"
     ],
     description: """
       Create a data stream across your information systems to query,
       augment and transform data according to Elixir matching rules.
     """,
     package: [links: %{"Source"=>"http://github.com/awetzel/adap",
                        "Doc"=>"http://hexdocs.pm/adap"},
               contributors: ["Arnaud Wetzel"],
               licenses: ["MIT"]],
     deps: [{:ex_doc, only: :dev}]]
  end

  def application do
    [mod: {Adap,[]},applications: [:logger]]
  end
end
