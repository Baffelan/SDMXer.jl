# SDMXer.jl Documentation

```@meta
CurrentModule = SDMXer
```

```@docs
SDMXer.SDMXer
```

Core Julia package for SDMX (Statistical Data and Metadata eXchange) processing. Extract and analyze structural metadata from SDMX-ML documents.

## Features

- **Schema Extraction**: Parse SDMX dataflow schemas from XML or APIs
- **Codelist Processing**: Extract and filter codelists with availability constraints
- **Data Availability Analysis**: Understand data coverage and temporal ranges
- **Validation Framework**: Validate data against SDMX standards
- **Pipeline Operations**: Chain operations with functional programming style
- **Data Queries**: Construct and execute SDMX API queries

## Installation

```julia
using Pkg
Pkg.add("SDMX")
```

## Quick Example

```julia
using SDMXer, DataFrames

# Extract SDMX schema from API
url = "https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC/DF_BP50/latest?references=all"
schema = extract_dataflow_schema(url)

# Get codelists filtered by availability
codelists = extract_all_codelists(url, true)

# Validate data against schema
validator = create_validator(schema, codelists)
result = validate_sdmx_csv(validator, "my_data.csv")
```

## Package Structure

- **Schema & Metadata**: Extract dataflow schemas, concepts, and structures
- **Codelists**: Process code lists with hierarchical relationships
- **Availability**: Analyze data availability and temporal coverage
- **Validation**: Validate data quality and SDMX compliance
- **Data Queries**: Construct and execute SDMX REST API queries
- **Pipelines**: Functional operations for workflow composition

## See Also

- [SDMXerWizard.jl](https://github.com/Baffelan/SDMXerWizard.jl) - LLM-powered extension for intelligent data transformation