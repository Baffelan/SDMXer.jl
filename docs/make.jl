using Documenter
using SDMX

makedocs(
    sitename = "SDMX.jl",
    format = Documenter.HTML(
        prettyurls = get(ENV, "CI", nothing) == "true",
        canonical = "https://baffelan.github.io/SDMX.jl",
        assets = String[],
    ),
    modules = [SDMX],
    pages = [
        "Home" => "index.md",
        "Getting Started" => "getting_started.md",
        "API Reference" => [
            "Schema & Metadata" => "api/schema.md",
            "Codelists" => "api/codelists.md",
            "Availability" => "api/availability.md",
            "Validation" => "api/validation.md",
            "Data Queries" => "api/queries.md",
            "Pipelines" => "api/pipelines.md",
            "Elements" => "api/elements.md",
            "Utilities" => "api/utilities.md",
        ],
        "Examples" => "examples.md",
    ],
    checkdocs = :exports,  # Check that all exported functions are documented
)

deploydocs(
    repo = "github.com/Baffelan/SDMX.jl.git",
    devbranch = "main",
)