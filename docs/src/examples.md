# Examples

## Complete Workflow Example

This example demonstrates a complete workflow from schema extraction to data validation:

```julia
using SDMX, CSV, DataFrames

# Define SDMX API endpoint
url = "https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC/DF_BP50/latest?references=all"

# 1. Extract schema
schema = extract_dataflow_schema(url)

# 2. Get codelists with availability filtering
codelists = extract_all_codelists(url, true)

# 3. Get data availability
availability = extract_availability_from_dataflow(url)
println("Available countries: ", join(get_available_values(availability, "GEO_PICT"), ", "))

# 4. Load your data
my_data = CSV.read("my_data.csv", DataFrame)

# 5. Create validator
validator = create_validator(schema, codelists)

# 6. Validate
result = validate_sdmx_csv(validator, "my_data.csv")

# 7. Generate report
report = generate_validation_report(result)
println(report)
```

## Working with Pacific Data Hub

Example using the Pacific Data Hub SDMX API:

```julia
using SDMX

# Balance of Payments dataflow
bp_url = "https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC/DF_BP50/latest?references=all"
bp_schema = extract_dataflow_schema(bp_url)

# Query specific data
query_url = construct_data_url(
    "https://stats-sdmx-disseminate.pacificdata.org",
    "SPC", "DF_BP50",
    dimensions = Dict(
        "GEO_PICT" => "FJ+TV",  # Fiji and Tuvalu
        "FREQ" => "A"            # Annual frequency
    ),
    start_period = "2020",
    end_period = "2023"
)

data = fetch_sdmx_data(query_url)
```

## Codelist Analysis

Working with hierarchical codelists:

```julia
using SDMX

url = "https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC/DF_BP50/latest?references=all"

# Get all codelists
codelists = extract_all_codelists(url, false)

# Find specific codelist
geo_codelist = codelists[codelists.codelist_id .== "CL_GEO_PICT", :]

# Examine hierarchical structure
for row in eachrow(geo_codelist)
    indent = row.parent_id === missing ? "" : "  "
    println(indent, row.code_id, " - ", row.name)
end
```

## Validation with Custom Rules

Create a validator with custom validation rules:

```julia
using SDMX

schema = extract_dataflow_schema(url)
codelists = extract_all_codelists(url, true)

# Create validator
validator = create_validator(schema, codelists)

# Validate with preview
result = validate_sdmx_csv(validator, "data.csv")

# Preview the output
preview = preview_validation_output(result; max_rows=10)
println(preview)
```

## Pipeline Operations

Using functional pipeline style:

```julia
using SDMX

# Define a validation pipeline
validation_pipeline = pipeline(
    extract_dataflow_schema,
    s -> (s, extract_all_codelists(url, true)),
    ((s, c),) -> create_validator(s, c),
    v -> validate_sdmx_csv(v, "data.csv")
)

# Execute pipeline
result = validation_pipeline(url)
```

## Data Coverage Analysis

Analyze temporal and dimensional coverage:

```julia
using SDMX

url = "https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC/DF_BP50/latest?references=all"

# Get availability
availability = extract_availability_from_dataflow(url)

# Get coverage summary
summary = get_data_coverage_summary(availability, schema)
println("Data coverage: ", summary["coverage_percentage"], "%")

# Find gaps
gaps = find_data_gaps(availability, schema)
if !isempty(gaps)
    println("Data gaps found in: ", join(gaps, ", "))
end
```

## Querying Multiple Series

Query multiple data series at once:

```julia
using SDMX, DataFrames

# Define multiple queries
queries = [
    Dict("GEO_PICT" => "FJ", "INDICATOR" => "BP_CA_P6_BAL_BP6"),
    Dict("GEO_PICT" => "TV", "INDICATOR" => "BP_CA_P6_BAL_BP6"),
    Dict("GEO_PICT" => "VU", "INDICATOR" => "BP_CA_P6_BAL_BP6")
]

# Fetch all
all_data = DataFrame()
for q in queries
    url = construct_data_url(
        "https://stats-sdmx-disseminate.pacificdata.org",
        "SPC", "DF_BP50",
        dimensions = q
    )
    data = fetch_sdmx_data(url)
    append!(all_data, data)
end
```