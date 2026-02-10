"""
SDMX Data Query Functions for SDMXer.jl

This module provides functional approaches to construct and execute SDMX data queries,
retrieving actual statistical data (as opposed to structural metadata).

Features:
- Simple functions to build SDMX data query URLs for any provider
- Fetch data in SDMX-CSV format using DataFrames
- Support for dimension filtering and time ranges
- Works with any SDMX provider and dataflow
"""

using DataFrames, CSV, HTTP, Statistics


"""
Helper function to format dimension values for SDMX keys.
"""
format_dimension_value(value::String) = value
format_dimension_value(values::Vector{String}) = join(values, "+")
format_dimension_value(::Nothing) = ""

"""
    construct_sdmx_key(schema::DataflowSchema, filters::Dict{String,String}) -> String

Constructs a proper SDMX key using dataflow schema dimension ordering.

This function builds an SDMX data key by ordering dimensions according to the
schema definition and validates that all filter dimensions exist in the schema.
The resulting key follows SDMX standards with dot-separated dimension values.

# Arguments
- `schema::DataflowSchema`: DataflowSchema containing dimension definitions and order
- `filters::Dict{String,String}`: Dict mapping dimension names to filter values

# Returns
- `String`: Properly formatted SDMX key with dimensions in correct order

# Examples
```julia
# Get schema first
xml_doc = read_sdmx_structure(url)
schema = extract_dataflow_schema(xml_doc)

# Construct key with validation
filters = Dict("FREQ" => "A", "GEO_PICT" => "TO")
key = construct_sdmx_key(schema, filters)
# Returns: "A.TO..." (proper SDMX key based on schema dimension order)
```

# Throws
- `ArgumentError`: If any filter dimension is not found in the schema

# See also
[`construct_data_url`](@ref), [`get_dimension_order`](@ref)
"""
function construct_sdmx_key(schema::DataflowSchema, filters::Dict{String,String})
    # Get dimension order from schema
    dimension_order = get_dimension_order(schema)
    
    # Validate that all filter dimensions exist in schema
    schema_dimensions = Set(dimension_order)
    for dim in keys(filters)
        if !(dim in schema_dimensions)
            throw(ArgumentError("Dimension '$dim' not found in dataflow schema. Available dimensions: $(join(dimension_order, ", "))"))
        end
    end
    
    # Construct key with proper dot notation
    key_parts = String[]
    for dim in dimension_order
        value = get(filters, dim, "")  # Empty string for unfiltered dimensions
        push!(key_parts, value)
    end
    
    return join(key_parts, ".")
end

"""
    construct_sdmx_key(schema::DataflowSchema, filters::Dict{String,<:Any}) -> String

Constructs a proper SDMX key with support for multiple values per dimension.

This method handles filters with array values, automatically converting them
to SDMX '+' notation for multiple selections.

# Arguments
- `schema::DataflowSchema`: DataflowSchema containing dimension definitions and order
- `filters::Dict{String,<:Any}`: Dict with String or Vector{String} values

# Examples
```julia
# With multiple values
filters = Dict("GEO_PICT" => ["FJ", "VU"], "FREQ" => "A")
key = construct_sdmx_key(schema, filters)
# Returns: "A..FJ+VU..." (with '+' for multiple values)
```
"""
function construct_sdmx_key(schema::DataflowSchema, filters::Dict{String,<:Any})
    # Get dimension order from schema
    dimension_order = get_dimension_order(schema)
    
    # Validate that all filter dimensions exist in schema
    schema_dimensions = Set(dimension_order)
    for dim in keys(filters)
        if !(dim in schema_dimensions)
            throw(ArgumentError("Dimension '$dim' not found in dataflow schema. Available dimensions: $(join(dimension_order, ", "))"))
        end
    end
    
    # Construct key with proper dot notation
    key_parts = String[]
    for dim in dimension_order
        value = get(filters, dim, nothing)
        push!(key_parts, format_dimension_value(value))
    end
    
    return join(key_parts, ".")
end


"""
    construct_data_url(base_url::String, agency_id::String, dataflow_id::String, version::String; kwargs...) -> String

Constructs SDMX data query URLs with flexible filtering options.

This function builds complete SDMX REST API data query URLs according to the
SDMX 2.1 standard, supporting various filtering approaches including pre-built
keys, schema-based dimension filtering, and time period constraints.

# Arguments
- `base_url::String`: SDMX REST API base URL  
- `agency_id::String`: Data provider agency (e.g., "SPC", "ECB", "OECD")
- `dataflow_id::String`: Dataflow identifier (e.g., "DF_BP50", "EXR", "QNA")
- `version::String`: Dataflow version (e.g., "1.0" or "latest")
- `schema::Union{DataflowSchema,Nothing}=nothing`: Optional schema for key construction
- `key::String=""`: Pre-constructed key string (overrides dimension_filters)
- `dimension_filters::Dict{String,String}=Dict{String,String}()`: Dimension name-value pairs
- `start_period::Union{String,Nothing}=nothing`: Start date/period for time filtering
- `end_period::Union{String,Nothing}=nothing`: End date/period for time filtering
- `dimension_at_observation::String="AllDimensions"`: Response structure format

# Returns
- `String`: Complete SDMX REST API data query URL

# Examples
```julia
# Using pre-constructed key (most flexible)
url = construct_data_url(
    "https://stats-sdmx-disseminate.pacificdata.org/rest",
    "SPC", "DF_BP50", "1.0",
    key="A.TO.BX_TRF_PWKR._T._T._T._T._T._T._T....",
    start_period="2022"
)

# Using dimension filters with schema validation
url = construct_data_url(
    "https://sdw-wsrest.ecb.europa.eu/service",
    "ECB", "EXR", "1.0", 
    schema=schema,
    dimension_filters=Dict("FREQ" => "D", "CURRENCY" => "USD"),
    start_period="2023-01"
)

# Simple case - get all data with time filtering
url = construct_data_url(
    "https://stats-sdmx-disseminate.pacificdata.org/rest",
    "SPC", "DF_BP50", "1.0",
    start_period="2022"
)
```

# See also
[`construct_sdmx_key`](@ref), [`fetch_sdmx_data`](@ref), [`query_sdmx_data`](@ref)
"""
function construct_data_url(base_url::String, agency_id::String, dataflow_id::String, version::String;
                           schema::Union{DataflowSchema,Nothing}=nothing,
                           key::String="",
                           dimension_filters::Dict{String,<:Any}=Dict{String,Any}(),
                           start_period::Union{String,Nothing}=nothing,
                           end_period::Union{String,Nothing}=nothing,
                           dimension_at_observation::String="AllDimensions")
    
    # Build dataflow reference
    dataflow_ref = agency_id * "," * dataflow_id * "," * version
    
    # Use provided key or construct from filters and schema
    final_key = if !isempty(key)
        key
    elseif !isempty(dimension_filters) && schema !== nothing
        construct_sdmx_key(schema, dimension_filters)
    elseif !isempty(dimension_filters)
        @warn "Dimension filters provided without schema - key construction may be incorrect"
        join(values(dimension_filters), ".")  # Fallback - join values
    else
        ""  # Empty key - get all data
    end
    
    # Build URL - handle trailing slash in base_url
    base_url_clean = rstrip(base_url, '/')
    url = base_url_clean * "/data/" * dataflow_ref * "/" * final_key
    
    # Add query parameters
    params = String[]
    start_period !== nothing && push!(params, "startPeriod=" * start_period)
    end_period !== nothing && push!(params, "endPeriod=" * end_period)
    push!(params, "dimensionAtObservation=" * dimension_at_observation)
    
    !isempty(params) && (url *= "?" * join(params, "&"))
    
    return url
end

"""
    fetch_sdmx_data(url::String; timeout::Int=30) -> DataFrame

Fetches and parses SDMX data in CSV format from REST API endpoints.

This function retrieves SDMX data using the standard SDMX-CSV format, performs
basic data cleaning and type conversion, and returns a structured DataFrame
suitable for analysis. Works with any SDMX 2.1 compliant provider.

# Arguments
- `url::String`: Complete SDMX REST API data query URL
- `timeout::Int=30`: HTTP timeout in seconds

# Returns
- `DataFrame`: Cleaned dataset with appropriate column types

# Examples
```julia
# Pacific Data Hub
url = construct_data_url("https://stats-sdmx-disseminate.pacificdata.org/rest", 
                        "SPC", "DF_BP50", "1.0", start_period="2022")
data = fetch_sdmx_data(url)

# ECB exchange rates with custom timeout
url = construct_data_url("https://sdw-wsrest.ecb.europa.eu/service",
                        "ECB", "EXR", "1.0", 
                        dimension_filters=Dict("FREQ" => "D", "CURRENCY" => "USD"))
data = fetch_sdmx_data(url; timeout=60)

# Handle empty responses gracefully
data = fetch_sdmx_data(url)
if nrow(data) == 0
    println("No data available for query")
end
```

# Throws
- `ArgumentError`: For HTTP errors, invalid responses, or network issues

# See also
[`construct_data_url`](@ref), [`clean_sdmx_data`](@ref), [`query_sdmx_data`](@ref)
"""
function fetch_sdmx_data(url::String; timeout::Int=30)
    # Set SDMX-CSV headers
    headers = Dict(
        "Accept" => "application/vnd.sdmx.data+csv;version=2.0.0",
        "User-Agent" => "SDMXer.jl/0.1.0"
    )
    
    try
        response = HTTP.get(url; headers=headers, timeout=timeout)
        
        response.status == 200 || throw(ArgumentError("HTTP $(response.status): Failed to fetch data"))
        
        csv_content = String(response.body)
        isempty(strip(csv_content)) && return DataFrame()  # Empty response
        
        # Parse CSV and clean data
        data = CSV.read(IOBuffer(csv_content), DataFrame)
        return clean_sdmx_data(data)
        
    catch e
        isa(e, HTTP.StatusError) ? 
            throw(ArgumentError("SDMX API error $(e.status): $(String(e.response.body))")) :
            throw(ArgumentError("Failed to fetch SDMX data: $e"))
    end
end

"""
    clean_sdmx_data(data::DataFrame) -> DataFrame

Performs standardized cleaning and type conversion on SDMX-CSV data.

This function applies standard SDMX data cleaning procedures including numeric
conversion of observation values, string formatting of time periods, and removal
of empty rows. Works with CSV output from any SDMX 2.1 compliant provider.

# Arguments
- `data::DataFrame`: Raw DataFrame from SDMX-CSV parsing

# Returns
- `DataFrame`: Cleaned DataFrame with standardized column types

# Examples
```julia
# Manual cleaning after CSV import
raw_data = CSV.read("sdmx_data.csv", DataFrame)
cleaned_data = clean_sdmx_data(raw_data)

# Automatic cleaning within fetch_sdmx_data
data = fetch_sdmx_data(url)  # Cleaning applied automatically
```

# See also
[`fetch_sdmx_data`](@ref)
"""
function clean_sdmx_data(data::DataFrame)
    isempty(data) && return data
    
    # Create a copy to avoid mutations
    cleaned = copy(data)
    
    # Convert OBS_VALUE to numeric (standard SDMX column)
    if hasproperty(cleaned, :OBS_VALUE)
        cleaned.OBS_VALUE = map(cleaned.OBS_VALUE) do val
            ismissing(val) || val == "" ? missing :
            isa(val, Number) ? Float64(val) :
            tryparse(Float64, string(val))
        end
    end
    
    # Ensure TIME_PERIOD is string (standard SDMX column)
    if hasproperty(cleaned, :TIME_PERIOD)
        cleaned.TIME_PERIOD = string.(cleaned.TIME_PERIOD)
    end
    
    # Remove completely empty rows
    if nrow(cleaned) > 0
        non_empty_mask = map(eachrow(cleaned)) do row
            !all(ismissing, row)
        end
        cleaned = cleaned[non_empty_mask, :]
    end
    
    return cleaned
end

"""
    query_sdmx_data(base_url::String, agency_id::String, dataflow_id::String, version::String="latest"; kwargs...) -> DataFrame

Convenience function for complete SDMX data retrieval in a single call.

This high-level function combines URL construction and data fetching into a single
operation, providing the most convenient way to retrieve SDMX data from any
provider. It handles URL building, HTTP requests, and data cleaning automatically.

# Arguments
- `base_url::String`: SDMX REST API base URL
- `agency_id::String`: Data provider agency identifier
- `dataflow_id::String`: Dataflow identifier
- `version::String="latest"`: Dataflow version
- `key::String=""`: Pre-constructed SDMX key
- `filters::Dict{String,<:Any}`: Combined filters including dimensions and TIME_PERIOD
  - Dimension values can be String or Vector{String} for multiple values
  - TIME_PERIOD can be a single value, array, or range format "start:end"
- `dimension_filters::Dict{String,<:Any}`: Dimension filters (deprecated, use `filters`)
- `start_period::Union{String,Nothing}=nothing`: Start date/period filter
- `end_period::Union{String,Nothing}=nothing`: End date/period filter

# Returns
- `DataFrame`: Cleaned SDMX data ready for analysis

# Examples
```julia
# Pacific Data Hub - multiple countries with time range
data = query_sdmx_data(
    "https://stats-sdmx-disseminate.pacificdata.org/rest",
    "SPC", "DF_BP50",
    filters=Dict("GEO_PICT" => ["FJ", "VU"], "TIME_PERIOD" => "2020:2023")
)

# Single country, specific year
data = query_sdmx_data(
    "https://stats-sdmx-disseminate.pacificdata.org/rest",
    "SPC", "DF_BP50",
    filters=Dict("GEO_PICT" => "TO", "TIME_PERIOD" => "2022")
)

# ECB - EUR/USD daily exchange rates
data = query_sdmx_data(
    "https://sdw-wsrest.ecb.europa.eu/service",
    "ECB", "EXR", "1.0",
    filters=Dict("FREQ" => "D", "CURRENCY" => "USD", "CURRENCY_DENOM" => "EUR"),
    start_period="2024-01-01"
)

# OECD - using pre-constructed key
data = query_sdmx_data(
    "https://stats.oecd.org/restsdmx/sdmx.ashx",
    "OECD", "QNA", "1.0",
    key="AUS.GDP.CPC.Y.L",  # Australia, GDP, Current prices, Yearly, Levels
    start_period="2020"
)

# Handle potential empty results
data = query_sdmx_data(base_url, agency, dataflow, version)
println("Retrieved ", nrow(data), " observations")
```

# See also
[`construct_data_url`](@ref), [`fetch_sdmx_data`](@ref), [`summarize_data`](@ref), [`validate_sdmx_csv`](@ref)
"""
function query_sdmx_data(base_url::String, agency_id::String, dataflow_id::String, version::String="latest";
                        key::String="",
                        filters::Dict{String,<:Any}=Dict{String,Any}(),
                        dimension_filters::Dict{String,<:Any}=Dict{String,Any}(),
                        start_period::Union{String,Nothing}=nothing,
                        end_period::Union{String,Nothing}=nothing)
    
    # Merge filters and dimension_filters for backward compatibility
    # Prefer filters if both are provided
    actual_filters = !isempty(filters) ? filters : dimension_filters
    
    # Extract TIME_PERIOD from filters if present
    if haskey(actual_filters, "TIME_PERIOD")
        time_value = actual_filters["TIME_PERIOD"]
        
        # Handle time period range format "2020:2023"
        if isa(time_value, String) && occursin(":", time_value)
            parts = split(time_value, ":")
            if length(parts) == 2
                if start_period === nothing
                    start_period = String(strip(parts[1]))
                end
                if end_period === nothing
                    end_period = String(strip(parts[2]))
                end
            else
                throw(ArgumentError("Invalid TIME_PERIOD range format. Use 'start:end' (e.g., '2020:2023')"))
            end
        else
            # Single value or other format - set both start and end to same value
            if start_period === nothing
                start_period = string(time_value)
            end
            if end_period === nothing
                end_period = string(time_value)
            end
        end
        
        # Remove TIME_PERIOD from dimension filters as it's not a dimension
        actual_filters = Dict(k => v for (k, v) in actual_filters if k != "TIME_PERIOD")
    end
    
    # If dimension_filters are provided without a key, fetch the schema
    schema = nothing
    actual_version = version
    
    if !isempty(actual_filters) && isempty(key)
        # Construct dataflow structure URL (works with "latest")
        base_url_clean = rstrip(base_url, '/')
        dataflow_url = base_url_clean * "/dataflow/" * agency_id * "/" * dataflow_id * "/" * version
        
        try
            # Fetch and parse the dataflow schema
            schema = extract_dataflow_schema(dataflow_url)
            # Extract the actual version from the schema if we used "latest"
            if version == "latest" && schema !== nothing
                actual_version = schema.dataflow_info.version
            end
        catch e
            @warn "Could not fetch dataflow schema: " * string(e)
            @warn "Falling back to simple key construction"
        end
    elseif version == "latest"
        # Even without dimension_filters, if version is "latest" we should fetch the actual version
        base_url_clean = rstrip(base_url, '/')
        dataflow_url = base_url_clean * "/dataflow/" * agency_id * "/" * dataflow_id * "/" * version
        
        try
            # Fetch and parse the dataflow schema just to get the version
            temp_schema = extract_dataflow_schema(dataflow_url)
            actual_version = temp_schema.dataflow_info.version
            # If we also need the schema for key construction, use it
            if !isempty(actual_filters) && isempty(key)
                schema = temp_schema
            end
        catch e
            @warn "Could not fetch dataflow schema to resolve version: " * string(e)
            # Keep version as "latest" and hope the API supports it
        end
    end
    
    url = construct_data_url(base_url, agency_id, dataflow_id, actual_version,
                           schema=schema,
                           key=key,
                           dimension_filters=actual_filters,
                           start_period=start_period, 
                           end_period=end_period)
    
    return fetch_sdmx_data(url)
end

"""
    summarize_data(data::DataFrame) -> Dict{String, Any}

Provides comprehensive statistical summary of SDMX datasets.

This function generates a summary report containing key statistics about an SDMX
dataset including observation counts, time range coverage, value statistics, and
dimension value distributions. Works with data from any SDMX provider.

# Arguments
- `data::DataFrame`: SDMX dataset to summarize

# Returns
- `Dict{String, Any}`: Summary statistics including observation counts, time ranges, and dimension values

# Examples
```julia
data = query_sdmx_data(base_url, "SPC", "DF_BP50", "1.0")
summary = summarize_data(data)

println("Total observations: ", summary["total_observations"])
println("Time range: ", summary["time_range"])
if haskey(summary, "obs_stats")
    println("Value range: ", summary["obs_stats"].min, " - ", summary["obs_stats"].max)
end

# Check dimensions present in data
for (key, values) in summary
    if isa(values, Vector) && !isempty(values)
        println(key, ": ", length(values), " unique values")
    end
end
```

# See also
[`query_sdmx_data`](@ref), [`clean_sdmx_data`](@ref)
"""
function summarize_data(data::DataFrame)
    isempty(data) && return Dict("total_observations" => 0)
    
    summary = Dict{String, Any}("total_observations" => nrow(data))
    
    # Time range (standard SDMX)
    if hasproperty(data, :TIME_PERIOD)
        periods = sort(unique(skipmissing(data.TIME_PERIOD)))
        !isempty(periods) && (summary["time_range"] = (first(periods), last(periods)))
    end
    
    # Observation statistics (standard SDMX)
    if hasproperty(data, :OBS_VALUE)
        valid_obs = filter(!ismissing, data.OBS_VALUE)
        if !isempty(valid_obs)
            summary["obs_stats"] = (
                count=length(valid_obs),
                min=minimum(valid_obs), 
                max=maximum(valid_obs),
                mean=round(mean(valid_obs), digits=2)
            )
        end
    end
    
    # Generic dimension summary - detect common SDMX dimensions
    common_dimensions = ["FREQ", "INDICATOR", "GEO_PICT", "REF_AREA", "CURRENCY", "SUBJECT"]
    for dim in common_dimensions
        if hasproperty(data, Symbol(dim))
            values = sort(unique(skipmissing(data[!, dim])))
            !isempty(values) && (summary[lowercase(dim)] = values)
        end
    end
    
    return summary
end