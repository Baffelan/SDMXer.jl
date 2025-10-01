# Getting Started

## Installation

Install SDMX.jl from the Julia package registry:

```julia
using Pkg
Pkg.add("SDMX")
```

## Basic Workflow

### 1. Extract SDMX Schema

The first step is to extract the dataflow schema from an SDMX-ML document or API endpoint:

```julia
using SDMX

url = "https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC/DF_BP50/latest?references=all"
schema = extract_dataflow_schema(url)

# Inspect the schema
println("Dimensions: ", length(schema.dimensions))
println("Measures: ", length(schema.measures))
println("Attributes: ", length(schema.attributes))
```

### 2. Extract Codelists

Extract codelists that define valid values for dimensions:

```julia
# Get all codelists
all_codelists = extract_all_codelists(url, false)

# Or get only codelists filtered by availability
available_codelists = extract_all_codelists(url, true)

# Access specific codelist
geo_codes = available_codelists[available_codelists.codelist_id .== "CL_GEO_PICT", :]
```

### 3. Analyze Data Availability

Understand what data is available:

```julia
availability = extract_availability_from_dataflow(url)

# Get available values for a dimension
country_values = get_available_values(availability, "GEO_PICT")

# Get time coverage
time_range = get_time_coverage(availability)
```

### 4. Validate Data

Validate your data against the SDMX schema:

```julia
using CSV, DataFrames

# Create validator
validator = create_validator(schema, available_codelists)

# Validate CSV file
result = validate_sdmx_csv(validator, "my_data.csv")

# Check results
if result.is_valid
    println("Data is valid!")
else
    println("Validation errors:")
    for error in result.errors
        println("  ", error.message)
    end
end
```

### 5. Query Data

Construct and execute SDMX API queries:

```julia
# Build a data query
data_url = construct_data_url(
    "https://stats-sdmx-disseminate.pacificdata.org",
    "SPC", "DF_BP50",
    dimensions = Dict("GEO_PICT" => "FJ", "INDICATOR" => "BP_CA_P6_BAL_BP6")
)

# Fetch data
data = fetch_sdmx_data(data_url)
```

## Using Pipelines

SDMX.jl supports functional pipeline operations:

```julia
# Chain operations
result = schema |>
    s -> create_validator(s, codelists) |>
    v -> validate_sdmx_csv(v, "data.csv")

# Or use the pipe operator
using SDMX: ⇒

schema ⇒ validate_with(codelists, "data.csv")
```

## Next Steps

- Explore the [API Reference](api/schema.md) for detailed function documentation
- Check out [Examples](examples.md) for more use cases
- For LLM-powered data transformation, see [SDMXLLM.jl](https://github.com/Baffelan/SDMXLLM.jl)