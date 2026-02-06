# SDMXer.jl

[![Build Status](https://github.com/Baffelan/SDMXer.jl/workflows/CI/badge.svg)](https://github.com/Baffelan/SDMXer.jl/actions/workflows/CI.yml)
[![Coverage](https://codecov.io/gh/Baffelan/SDMXer.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/Baffelan/SDMXer.jl)
[![Aqua](https://raw.githubusercontent.com/JuliaTesting/Aqua.jl/master/badge.svg)](https://github.com/JuliaTesting/Aqua.jl)
[![SciML Code Style](https://img.shields.io/static/v1?label=code%20style&message=SciML&color=9558b2&labelColor=389826)](https://github.com/SciML/SciMLStyle)

Core Julia package for SDMX (Statistical Data and Metadata eXchange) processing. Extract and analyze structural metadata from SDMX-ML documents.

## Installation

```julia
using Pkg
Pkg.add(url="https://github.com/Baffelan/SDMXer.jl")
```

## Quick Start

```julia
using SDMXer, DataFrames, CSV

# 1. Extract SDMX schema
url = "https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC/DF_BP50/latest?references=all"
schema = extract_dataflow_schema(url)

# 2. Get codelists (only values used in actual data)
codelists = extract_all_codelists(url, true)

# 3. Profile your data
source_data = CSV.read("my_data.csv", DataFrame)
profile = profile_source_data(source_data, "my_data.csv")

# 4. Get mapping suggestions
mappings = suggest_column_mappings(profile, schema)

# 5. Validate SDMX compliance
validator = create_validator(schema)
result = validate_sdmx_csv(validator, "my_data.csv")
```

## Core Functionality

### Extract SDMX Metadata
```julia
# Dataflow schema
schema = extract_dataflow_schema(url)
required = get_required_columns(schema)
optional = get_optional_columns(schema)

# Codelists
all_codes = extract_all_codelists(url)
used_codes = extract_all_codelists(url, true)  # Only used values

# Data availability
availability = extract_availability(construct_availability_url(url))
countries = get_available_values(availability, "GEO_PICT")
time_range = get_time_coverage(availability)
```

### Profile & Map Data
```julia
# Read various formats
data = read_source_data("file.xlsx")  # Auto-detects CSV/Excel

# Profile structure
profile = profile_source_data(data, "file.xlsx")
print_source_profile(profile)

# Suggest mappings
mappings = suggest_column_mappings(profile, schema)
```

### Validate Data
```julia
# Create validator with custom rules
validator = create_validator(schema; 
    check_codelists=true,
    check_time_format=true
)

# Validate and report
result = validate_sdmx_csv(validator, "data.csv")
report = generate_validation_report(result)
preview_validation_output(result)

# Check specific formats
is_valid_time_format("2024-01")  # true
```

### Query SDMX APIs
```julia
# Fetch data with filters (supports flexible filter formats)
data = query_sdmx_data(base_url, "SPC", "DF_BP50",
    filters=Dict("GEO_PICT" => "FJ", "TIME_PERIOD" => "2023")
)

# Multiple values per dimension
data = query_sdmx_data(base_url, "SPC", "DF_BP50",
    filters=Dict("GEO_PICT" => ["FJ", "VU"], "TIME_PERIOD" => "2020:2023")
)

# With automatic retries and timeout handling
data = query_sdmx_data(base_url, "SPC", "DF_BP50",
    filters=Dict("GEO_PICT" => "FJ"),
    max_retries=3,  # Default: 3 attempts
    timeout=30      # Default: 30 seconds
)

# Summarize results
summary = summarize_data(data)
```

### Pipeline Operations
```julia
# Chain operations
pipeline = chain(
    profile_with("source.csv"),
    validate_with(schema),
    tap(df -> println("Processing " * string(nrow(df)) * " rows"))
)

result = data |> pipeline

# Use operators
validated = data ⇒ validator  # Validate operator
conforms = data ⊆ schema      # Conformance check
```

## Working with Pacific Data Hub

```julia
using SDMXer, DataFrames, CSV

# Complete workflow example
base_url = "https://stats-sdmx-disseminate.pacificdata.org/rest/"
dataflow_url = base_url * "dataflow/SPC/DF_BP50/latest?references=all"

# 1. Extract metadata
schema = extract_dataflow_schema(dataflow_url)
codelists = extract_all_codelists(dataflow_url, true)

# 2. Check data availability
avail_url = construct_availability_url(dataflow_url)
availability = extract_availability(avail_url)
print_availability_summary(availability)

# 3. Find gaps in coverage
expected = Dict(
    "GEO_PICT" => ["FJ", "SB", "VU"],
    "TIME_PERIOD" => ["2020", "2021", "2022"]
)
gaps = find_data_gaps(availability, expected)

# 4. Validate your data
validator = create_validator(schema)
result = validate_sdmx_csv(validator, "pacific_trade.csv")
```

## API Reference

### Main Types
- `DataflowSchema` - Complete SDMX dataflow structure
- `SourceDataProfile` - Source data analysis results  
- `AvailabilityConstraint` - Data availability information
- `ValidationResult` - Validation outcome with issues
- `SDMXValidator` - Configurable validation engine

### Key Functions

**Schema & Metadata**
- `extract_dataflow_schema(url)` - Extract complete schema
- `extract_all_codelists(url, filter_by_availability)` - Get codelists
- `extract_availability(url)` - Get data availability

**Data Processing**
- `read_source_data(file)` - Smart file reading
- `profile_source_data(data, file)` - Analyze structure
- `suggest_column_mappings(profile, schema)` - Map columns

**Validation**
- `create_validator(schema; kwargs...)` - Configure validator
- `validate_sdmx_csv(validator, file)` - Validate file
- `is_valid_time_format(str)` - Check time format

**Queries**
- `query_sdmx_data(base, agency, dataflow; filters, max_retries, timeout)` - Query API with retry logic
- `construct_sdmx_key(schema, filters)` - Build query key (supports arrays and ranges)

**Pipeline**
- `chain(ops...)` - Chain operations
- `validate_with(schema)` - Validation operation
- `profile_with(file)` - Profiling operation

## Testing

```julia
using Pkg
Pkg.test("SDMXer")
```

## Documentation

- **[Generated Function Parsing](docs/GENERATED_PARSING.md)** - High-performance parsing system
- **Examples** - See `test/` directory for comprehensive usage

## See Also

- [SDMXerWizard.jl](https://github.com/Baffelan/SDMXerWizard.jl) - LLM-powered extension
- [SDMX.org](https://sdmx.org) - Official SDMX documentation
- [PDH .Stat](https://stats.pacificdata.org) - Pacific Data Hub

## License

MIT License - see [LICENSE](LICENSE) file for details.