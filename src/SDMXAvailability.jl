"""
SDMX Availability Constraint extraction for SDMXer.jl

This module extracts actual data availability information from SDMX availability constraints,
showing which dimension values actually have published data (vs. theoretical schema possibilities).

Key differences from dataflow schema:
- Schema: Shows ALL possible/allowed dimension values
- Availability: Shows ONLY dimension values that have actual data

Features:
- Extract available time periods (actual date ranges with data)
- Get available codelist values (countries, indicators, etc. with data)  
- Observation counts and data coverage metrics
- Time range analysis for data gaps
- Integration with dataflow schema for completeness analysis
"""

using EzXML, DataFrames, HTTP, Dates

"""
    TimeAvailability

Structure containing comprehensive information about actual time period coverage in SDMX datasets.

This struct captures detailed temporal availability information from SDMX availability
constraints, including date ranges, format specifications, period counts, and data gaps
to provide complete temporal coverage analysis.

# Fields
- `start_date::Union{Date, String}`: Earliest available time period in the dataset
- `end_date::Union{Date, String}`: Latest available time period in the dataset
- `format::String`: Time period format ("date", "year", "quarter", "month", etc.)
- `total_periods::Int`: Total number of distinct time periods with data
- `gaps::Vector{String}`: Missing time periods within the overall range

# Examples
```julia
# Create time availability information
time_availability = TimeAvailability(
    Date("2020-01-01"),
    Date("2023-12-31"),
    "year", 
    4,
    ["2021"]
)

# Access coverage information
println("Data available from: ", time_availability.start_date)
println("Data available to: ", time_availability.end_date)
println("Total periods: ", time_availability.total_periods)

# Check for gaps
if !isempty(time_availability.gaps)
    println("Missing periods: ", join(time_availability.gaps, ", "))
end
```

# See also
[`AvailabilityConstraint`](@ref), [`extract_availability`](@ref), [`get_time_coverage`](@ref)
"""
struct TimeAvailability
    start_date::Union{Date, String, Int}  # Can be Date, String (for quarters/semesters), or Int (for years)
    end_date::Union{Date, String, Int}    # Can be Date, String (for quarters/semesters), or Int (for years)
    format::String  # "date", "year", "quarter", etc.
    total_periods::Int
    gaps::Vector{String}  # Missing periods within the range
end

"""
    in(year::Int, time::TimeAvailability) -> Bool

Check if a year is within the time coverage range.
"""
function Base.in(year::Int, time::TimeAvailability)
    if time.format == "year" && time.start_date isa Int && time.end_date isa Int
        return time.start_date <= year <= time.end_date
    elseif time.format == "date" && time.start_date isa Date && time.end_date isa Date
        return Dates.year(time.start_date) <= year <= Dates.year(time.end_date)
    else
        return false
    end
end

"""
    in(period::String, time::TimeAvailability) -> Bool

Check if a time period string is within the time coverage range.
"""
function Base.in(period::String, time::TimeAvailability)
    # Try to parse as year
    year_match = match(r"^(\d{4})$", period)
    if year_match !== nothing
        year = parse(Int, year_match[1])
        return in(year, time)
    end
    
    # For other formats, generate the full range and check membership
    periods = get_time_period_range(time)
    if periods isa UnitRange{Int}
        # Convert string year to int and check
        @assert occursin(r"^\d{4}$", period) "Expected year format YYYY, got: " * period
        year = parse(Int, period)
        return year in periods
    else
        return period in periods
    end
end

"""
    DimensionAvailability

Availability information for a single dimension.
"""
struct DimensionAvailability
    dimension_id::String
    available_values::Vector{String}
    total_count::Int
    value_type::String  # "codelist", "time", "free_text"
    coverage_ratio::Float64  # available / theoretical (if schema provided)
end

"""
    AvailabilityConstraint

Complete availability constraint information from SDMXer.
"""
struct AvailabilityConstraint
    constraint_id::String
    constraint_name::String
    agency_id::String
    version::String
    dataflow_ref::NamedTuple  # Reference to the dataflow
    total_observations::Int
    dimensions::Vector{DimensionAvailability}
    time_coverage::Union{TimeAvailability, Nothing}
    extraction_timestamp::String
end

"""
    extract_availability(url::String) -> AvailabilityConstraint

Extracts availability constraint information from an SDMX availability URL.

# Example
```julia
# For Pacific Data Hub
availability = extract_availability("https://stats-sdmx-disseminate.pacificdata.org/rest/availableconstraint/DF_DISABILITY/")
```
"""
function extract_availability(input::String)
    # Use the robust URL handling from SDMXHelpers
    xml_content = fetch_sdmx_xml(input)
    doc = EzXML.parsexml(xml_content)
    return extract_availability(doc)
end

"""
    extract_availability_from_dataflow(doc::EzXML.Document) -> Union{AvailabilityConstraint, Nothing}

Extracts the Actual type ContentConstraint from a dataflow document's Constraints section.
This is used when fetching codelists without a specific filtering key.

Returns nothing if no Actual ContentConstraint is found (which is okay - not all dataflows have it).
"""
function extract_availability_from_dataflow(doc::EzXML.Document)
    doc_root = root(doc)
    
    # Look for ContentConstraint with type="Actual" in the Constraints section
    # Try different namespace patterns
    constraint_node = findfirst("//structure:ContentConstraint[@type='Actual']", doc_root)
    if constraint_node === nothing
        constraint_node = findfirst("//str:ContentConstraint[@type='Actual']", doc_root)
    end
    if constraint_node === nothing  
        constraint_node = findfirst("//ContentConstraint[@type='Actual']", doc_root)
    end
    if constraint_node === nothing
        # Try without namespace prefix but check for type attribute
        constraint_nodes = findall("//*[local-name()='ContentConstraint']", doc_root)
        for node in constraint_nodes
            if haskey(node, "type") && node["type"] == "Actual"
                constraint_node = node
                break
            end
        end
    end
    
    # If no Actual constraint found, return nothing (not an error)
    if constraint_node === nothing
        return nothing
    end
    
    # Extract using the existing logic
    return extract_availability_from_node(constraint_node)
end

"""
    extract_availability(doc::EzXML.Document) -> AvailabilityConstraint

Extracts availability constraint information from a parsed XML document.
"""
function extract_availability(doc::EzXML.Document)
    # Register common SDMX namespaces to handle different XML structures
    doc_root = root(doc)
    
    # Try different namespace patterns for ContentConstraint
    constraint_node = findfirst("//structure:ContentConstraint", doc_root)
    if constraint_node === nothing
        constraint_node = findfirst("//str:ContentConstraint", doc_root)
    end
    if constraint_node === nothing  
        constraint_node = findfirst("//ContentConstraint", doc_root)
    end
    if constraint_node === nothing
        # Try without namespace prefix
        constraint_nodes = findall("//*[local-name()='ContentConstraint']", doc_root)
        if !isempty(constraint_nodes)
            constraint_node = constraint_nodes[1]
        end
    end
    
    if constraint_node === nothing
        all_elements = [nodename(n) for n in findall("//*", doc_root)]
        sample_elements = all_elements[1:min(10, length(all_elements))]
        separator = ", "
        throw(ArgumentError("No ContentConstraint found in the document. Available elements: $(join(sample_elements, separator))"))
    end
    
    # Use the refactored extraction logic
    return extract_availability_from_node(constraint_node)
end

"""
    extract_availability_from_node(constraint_node::EzXML.Node) -> AvailabilityConstraint

Extracts availability constraint information from a ContentConstraint XML node.
This is the core extraction logic used by both extract_availability and extract_availability_from_dataflow.
"""
function extract_availability_from_node(constraint_node::EzXML.Node)
    # Extract basic constraint info
    constraint_id = haskey(constraint_node, "id") ? constraint_node["id"] : "unknown"
    agency_id = haskey(constraint_node, "agencyID") ? constraint_node["agencyID"] : "unknown" 
    version = haskey(constraint_node, "version") ? constraint_node["version"] : "1.0"
    
    # Get constraint name (namespace-agnostic)
    name_node = findfirst(".//*[local-name()='Name']", constraint_node)
    constraint_name = name_node !== nothing ? strip(name_node.content) : "Availability Constraint"
    
    # Get observation count from annotations (namespace-agnostic)
    obs_count_node = findfirst(".//*[local-name()='Annotation'][@id='obs_count']/*[local-name()='AnnotationTitle']", constraint_node)
    total_observations = if obs_count_node !== nothing
        content = strip(obs_count_node.content)
        if occursin(r"^\d+$", content)  # Check if it's all digits
            parse(Int, content)
        else
            @warn "Invalid observation count format: '$content', defaulting to 0"
            0
        end
    else
        0
    end
    
    # Get dataflow reference (namespace-agnostic)
    dataflow_ref_node = findfirst(".//*[local-name()='Dataflow']/*[local-name()='Ref']", constraint_node)
    dataflow_ref = if dataflow_ref_node !== nothing
        (
            id = haskey(dataflow_ref_node, "id") ? dataflow_ref_node["id"] : "unknown",
            agency = haskey(dataflow_ref_node, "agencyID") ? dataflow_ref_node["agencyID"] : "unknown",
            version = haskey(dataflow_ref_node, "version") ? dataflow_ref_node["version"] : "1.0"
        )
    else
        (id="unknown", agency="unknown", version="1.0")
    end
    
    # Extract dimension availability from CubeRegion (namespace-agnostic)
    cube_region = findfirst(".//*[local-name()='CubeRegion']", constraint_node)
    dimensions = Vector{DimensionAvailability}()
    time_coverage = nothing
    
    if cube_region !== nothing
        key_values = findall(".//*[local-name()='KeyValue']", cube_region)
        
        for kv_node in key_values
            dim_id = kv_node["id"]
            
            # Handle time dimension specially
            if dim_id == "TIME_PERIOD"
                time_coverage = extract_time_availability(kv_node)
                # Also add as regular dimension
                time_values = get_time_period_values(kv_node)
                push!(dimensions, DimensionAvailability(
                    dim_id,
                    time_values,
                    length(time_values),
                    "time",
                    1.0  # Can't calculate coverage without schema
                ))
            else
                # Regular dimension
                values = extract_dimension_values(kv_node)
                push!(dimensions, DimensionAvailability(
                    dim_id,
                    values,
                    length(values),
                    "codelist",  # Assume codelist for non-time dimensions
                    1.0  # Can't calculate coverage without schema
                ))
            end
        end
    end
    
    return AvailabilityConstraint(
        constraint_id,
        constraint_name,
        agency_id,
        version,
        dataflow_ref,
        total_observations,
        dimensions,
        time_coverage,
        string(Dates.now())
    )
end

"""
    extract_time_availability(time_node::EzXML.Node) -> TimeAvailability

Extracts time coverage information from a TIME_PERIOD KeyValue node.
"""
function extract_time_availability(time_node::EzXML.Node)
    # Check for TimeRange (namespace-agnostic)
    time_range = findfirst(".//*[local-name()='TimeRange']", time_node)
    
    if time_range !== nothing
        start_node = findfirst(".//*[local-name()='StartPeriod']", time_range)
        end_node = findfirst(".//*[local-name()='EndPeriod']", time_range)
        
        start_date = start_node !== nothing ? strip(start_node.content) : ""
        end_date = end_node !== nothing ? strip(end_node.content) : ""
        
        # Parse as dates with validation
        start_parsed = if length(start_date) >= 10 && occursin(r"^\d{4}-\d{2}-\d{2}", start_date)
            Date(start_date[1:10])  # Take just YYYY-MM-DD part
        else
            start_date  # Keep as string if not valid date format
        end
        
        end_parsed = if length(end_date) >= 10 && occursin(r"^\d{4}-\d{2}-\d{2}", end_date)
            Date(end_date[1:10])
        else
            end_date  # Keep as string if not valid date format
        end
        
        # Calculate total periods (rough estimate for years)
        total_periods = if start_parsed isa Date && end_parsed isa Date
            year(end_parsed) - year(start_parsed) + 1
        else
            1
        end
        
        return TimeAvailability(
            start_parsed,
            end_parsed,
            "date",
            total_periods,
            String[]  # Would need additional analysis to find gaps
        )
    else
        # Discrete time values
        time_values = extract_dimension_values(time_node)
        return TimeAvailability(
            length(time_values) > 0 ? time_values[1] : "",
            length(time_values) > 0 ? time_values[end] : "",
            "discrete",
            length(time_values),
            String[]
        )
    end
end

"""
    get_time_period_values(time_node::EzXML.Node) -> Vector{String}

Gets time period values as strings for dimension analysis.
"""
function get_time_period_values(time_node::EzXML.Node)
    # Check for TimeRange first (namespace-agnostic)
    time_range = findfirst(".//*[local-name()='TimeRange']", time_node)
    
    if time_range !== nothing
        start_node = findfirst(".//*[local-name()='StartPeriod']", time_range)
        end_node = findfirst(".//*[local-name()='EndPeriod']", time_range)
        
        start_str = start_node !== nothing ? strip(start_node.content) : ""
        end_str = end_node !== nothing ? strip(end_node.content) : ""
        
        # For ranges, return start-end representation
        if !isempty(start_str) && !isempty(end_str)
            return ["$(start_str[1:4])-$(end_str[1:4])"]  # Year range
        end
    end
    
    # Fall back to discrete values
    return extract_dimension_values(time_node)
end

"""
    extract_dimension_values(kv_node::EzXML.Node) -> Vector{String}

Extracts all available values for a dimension from a KeyValue node.
"""
function extract_dimension_values(kv_node::EzXML.Node)
    values = String[]
    value_nodes = findall(".//*[local-name()='Value']", kv_node)
    
    for value_node in value_nodes
        push!(values, strip(value_node.content))
    end
    
    return sort(values)  # Return sorted for consistency
end

"""
    get_available_values(availability::AvailabilityConstraint, dimension_id::String) -> Vector{String}

Gets available values for a specific dimension.

# Example
```julia
countries = get_available_values(availability, "GEO_PICT")
indicators = get_available_values(availability, "INDICATOR")
```
"""
function get_available_values(availability::AvailabilityConstraint, dimension_id::String)
    dim_index = findfirst(d -> d.dimension_id == dimension_id, availability.dimensions)
    return dim_index !== nothing ? availability.dimensions[dim_index].available_values : String[]
end

"""
    get_time_coverage(availability::AvailabilityConstraint; frequency_aware::Bool=true) -> Union{TimeAvailability, Nothing}

Gets time coverage information if available. When frequency_aware is true, adjusts the 
representation based on the FREQ dimension if present, following SDMX time period formats.

# SDMX Time Period Formats
- A: Annual (YYYY) - e.g., 2010
- S: Semester/half year (YYYY-Sn) - e.g., 2010-S1
- T: Trimester (YYYY-Tn) - e.g., 2010-T1
- Q: Quarterly (YYYY-Qn) - e.g., 2010-Q1
- M: Monthly (YYYY-MM) - e.g., 2010-01
- D: Daily (YYYY-MM-DD) - e.g., 2010-01-01
- H: Hourly (YYYY-MM-DDThh) - e.g., 2010-01-01T13
- I: DateTime (YYYY-MM-DDThh:mm:ss) - e.g., 2010-01-01T20:22:00

# Arguments
- `availability`: The availability constraint
- `frequency_aware`: Whether to adjust representation based on frequency (default: true)

# Examples
```julia
# For annual data (FREQ="A"), returns years instead of full dates
time_coverage = get_time_coverage(availability)
# TimeAvailability with start_date=1970, end_date=2030, format="year"

# Force date representation regardless of frequency
time_coverage = get_time_coverage(availability, frequency_aware=false)
```
"""
function get_time_coverage(availability::AvailabilityConstraint; frequency_aware::Bool=true)
    time_cov = availability.time_coverage
    
    if time_cov === nothing || !frequency_aware
        return time_cov
    end
    
    # Check if FREQ dimension exists
    freq_values = get_available_values(availability, "FREQ")
    if isempty(freq_values)
        return time_cov
    end
    
    freq = freq_values[1]  # Assume single frequency value
    
    # Parse start and end dates based on frequency
    start_parsed = parse_time_period(time_cov.start_date, freq)
    end_parsed = parse_time_period(time_cov.end_date, freq)
    
    # Determine format string based on frequency
    format_str = get_frequency_format(freq)
    
    # Calculate total periods based on frequency
    total_periods = calculate_periods_between(start_parsed, end_parsed, freq, time_cov.total_periods)
    
    return TimeAvailability(
        start_parsed,
        end_parsed,
        format_str,
        total_periods,
        time_cov.gaps
    )
end

"""
    parse_time_period(date_input::Union{Date, String}, freq::String) -> Union{Int, String, Date}

Parses a time period based on the frequency specification.
"""
function parse_time_period(date_input::Union{Date, String}, freq::String)
    if freq == "A"  # Annual - return just the year as Int
        if date_input isa Date
            return year(date_input)
        elseif date_input isa String && length(date_input) >= 4
            return parse(Int, date_input[1:4])
        else
            return date_input
        end
    elseif freq == "S"  # Semester
        if date_input isa Date
            y = year(date_input)
            s = month(date_input) <= 6 ? 1 : 2
            return string(y) * "-S" * string(s)
        elseif date_input isa String && occursin("-S", date_input)
            return date_input  # Already in correct format
        elseif date_input isa String && length(date_input) >= 7
            y = date_input[1:4]
            m = parse(Int, date_input[6:7])
            s = m <= 6 ? 1 : 2
            return string(y) * "-S" * string(s)
        else
            return date_input
        end
    elseif freq == "T"  # Trimester
        if date_input isa Date
            y = year(date_input)
            t = div(month(date_input) - 1, 4) + 1
            return string(y) * "-T" * string(t)
        elseif date_input isa String && occursin("-T", date_input)
            return date_input
        elseif date_input isa String && length(date_input) >= 7
            y = date_input[1:4]
            m = parse(Int, date_input[6:7])
            t = div(m - 1, 4) + 1
            return string(y) * "-T" * string(t)
        else
            return date_input
        end
    elseif freq == "Q"  # Quarterly
        if date_input isa Date
            y = year(date_input)
            q = div(month(date_input) - 1, 3) + 1
            return string(y) * "-Q" * string(q)
        elseif date_input isa String && occursin("-Q", date_input)
            return date_input
        elseif date_input isa String && length(date_input) >= 7
            y = date_input[1:4]
            m = parse(Int, date_input[6:7])
            q = div(m - 1, 3) + 1
            return string(y) * "-Q" * string(q)
        else
            return date_input
        end
    elseif freq == "M"  # Monthly
        if date_input isa Date
            return Dates.format(date_input, "yyyy-mm")
        elseif date_input isa String && length(date_input) >= 7
            return date_input[1:7]  # YYYY-MM format
        else
            return date_input
        end
    elseif freq == "D"  # Daily
        if date_input isa Date
            return date_input
        elseif date_input isa String && length(date_input) >= 10
            return Date(date_input[1:10])
        else
            return date_input
        end
    elseif freq == "H"  # Hourly
        if date_input isa Date
            return Dates.format(date_input, "yyyy-mm-ddTHH")
        else
            return date_input  # Keep as is
        end
    elseif freq == "I"  # DateTime
        return date_input  # Keep full datetime
    else
        return date_input  # Unknown frequency
    end
end

"""
    get_frequency_format(freq::String) -> String

Returns the format description for a given frequency code.
"""
function get_frequency_format(freq::String)
    freq_formats = Dict(
        "A" => "year",
        "S" => "semester",
        "T" => "trimester",
        "Q" => "quarter",
        "M" => "month",
        "D" => "day",
        "H" => "hour",
        "I" => "datetime"
    )
    return get(freq_formats, freq, "unknown")
end

"""
    calculate_periods_between(start_period, end_period, freq::String, default::Int) -> Int

Calculates the number of periods between start and end based on frequency.
"""
function calculate_periods_between(start_period, end_period, freq::String, default::Int)
    try
        if freq == "A" && start_period isa Int && end_period isa Int
            return end_period - start_period + 1
        elseif freq == "S" && start_period isa String && end_period isa String
            # Parse semester format YYYY-Sn
            start_year = parse(Int, start_period[1:4])
            start_sem = parse(Int, start_period[end])
            end_year = parse(Int, end_period[1:4])
            end_sem = parse(Int, end_period[end])
            return (end_year - start_year) * 2 + (end_sem - start_sem) + 1
        elseif freq == "T" && start_period isa String && end_period isa String
            # Parse trimester format YYYY-Tn
            start_year = parse(Int, start_period[1:4])
            start_tri = parse(Int, start_period[end])
            end_year = parse(Int, end_period[1:4])
            end_tri = parse(Int, end_period[end])
            return (end_year - start_year) * 3 + (end_tri - start_tri) + 1
        elseif freq == "Q" && start_period isa String && end_period isa String
            # Parse quarter format YYYY-Qn
            start_year = parse(Int, start_period[1:4])
            start_qtr = parse(Int, start_period[end])
            end_year = parse(Int, end_period[1:4])
            end_qtr = parse(Int, end_period[end])
            return (end_year - start_year) * 4 + (end_qtr - start_qtr) + 1
        elseif freq == "M" && start_period isa String && end_period isa String
            # Parse month format YYYY-MM
            start_year = parse(Int, start_period[1:4])
            start_month = parse(Int, start_period[6:7])
            end_year = parse(Int, end_period[1:4])
            end_month = parse(Int, end_period[6:7])
            return (end_year - start_year) * 12 + (end_month - start_month) + 1
        elseif freq == "D" && start_period isa Date && end_period isa Date
            return Dates.value(end_period - start_period) + 1
        else
            return default
        end
    catch
        return default  # Return default if calculation fails
    end
end

"""
    compare_schema_availability(schema::DataflowSchema, availability::AvailabilityConstraint) -> Dict{String, Any}

Compares theoretical schema possibilities with actual data availability.

Returns coverage ratios, missing values, and data gaps analysis.
"""
function compare_schema_availability(schema::DataflowSchema, availability::AvailabilityConstraint)
    comparison = Dict{String, Any}()
    
    # Get codelist information from schema
    schema_codelists = get_codelist_columns(schema)
    
    coverage_summary = Dict{String, Any}()
    missing_analysis = Dict{String, Vector{String}}()
    
    for dim_avail in availability.dimensions
        dim_id = dim_avail.dimension_id
        available_values = Set(dim_avail.available_values)
        
        if haskey(schema_codelists, dim_id)
            # This dimension has a codelist in the schema
            # We would need to fetch the full codelist to compare
            # For now, just record what we have
            coverage_summary[dim_id] = Dict(
                "available_count" => length(available_values),
                "available_values" => sort(collect(available_values)),
                "note" => "Full schema comparison requires codelist fetch"
            )
        else
            # Dimension without codelist (free text or time)
            coverage_summary[dim_id] = Dict(
                "available_count" => length(available_values),
                "available_values" => sort(collect(available_values)),
                "type" => "non_codelist"
            )
        end
    end
    
    comparison["coverage_by_dimension"] = coverage_summary
    comparison["total_observations"] = availability.total_observations
    comparison["dataflow_match"] = availability.dataflow_ref.id == schema.dataflow_info.id
    
    # Time coverage analysis
    if availability.time_coverage !== nothing
        time_info = availability.time_coverage
        comparison["time_coverage"] = Dict(
            "start" => time_info.start_date,
            "end" => time_info.end_date,
            "total_periods" => time_info.total_periods,
            "format" => time_info.format
        )
    end
    
    return comparison
end

"""
    get_data_coverage_summary(availability::AvailabilityConstraint) -> DataFrame

Creates a summary DataFrame of data coverage by dimension.
"""
function get_data_coverage_summary(availability::AvailabilityConstraint)
    rows = []
    
    for dim in availability.dimensions
        push!(rows, (
            dimension_id = dim.dimension_id,
            available_values = dim.total_count,
            sample_values = join(dim.available_values[1:min(5, length(dim.available_values))], ", "),
            value_type = dim.value_type
        ))
    end
    
    df = DataFrame(rows)
    
    # Add summary row
    push!(df, (
        dimension_id = "TOTAL_OBSERVATIONS",
        available_values = availability.total_observations,
        sample_values = "N/A",
        value_type = "count"
    ))
    
    return df
end

"""
    find_data_gaps(availability::AvailabilityConstraint, expected_values::Dict{String, Vector{String}}) -> Dict{String, Vector{String}}

Identifies missing values by comparing availability with expected values.

# Arguments
- `availability`: The availability constraint
- `expected_values`: Dict mapping dimension_id to expected value lists

# Returns
Dict mapping dimension_id to missing values
"""
function find_data_gaps(availability::AvailabilityConstraint, expected_values::Dict{String, Vector{String}})
    gaps = Dict{String, Vector{String}}()
    
    for (dim_id, expected_list) in expected_values
        if dim_id == "TIME_PERIOD" && availability.time_coverage !== nothing
            # Special handling for TIME_PERIOD - check against time coverage
            missing_values = String[]
            for period in expected_list
                if !(period in availability.time_coverage)
                    push!(missing_values, period)
                end
            end
            if !isempty(missing_values)
                gaps[dim_id] = sort(missing_values)
            end
        else
            # Regular dimension handling
            available_values = get_available_values(availability, dim_id)
            available_set = Set(available_values)
            expected_set = Set(expected_list)
            
            missing_values = collect(setdiff(expected_set, available_set))
            if !isempty(missing_values)
                gaps[dim_id] = sort(missing_values)
            end
        end
    end
    
    return gaps
end

"""
    get_time_period_range(time_coverage::TimeAvailability) -> Union{UnitRange{Int}, Vector{String}, Vector{Date}}

Returns an appropriate range or vector of time periods based on the format.

# Examples
```julia
time_coverage = get_time_coverage(availability)
periods = get_time_period_range(time_coverage)

# For annual data: returns 1970:2030
# For quarterly data: returns ["2020-Q1", "2020-Q2", ..., "2023-Q4"]
# For daily data: returns Date range
```
"""
function get_time_period_range(time_coverage::TimeAvailability)
    if time_coverage.format == "year" && time_coverage.start_date isa Int && time_coverage.end_date isa Int
        return time_coverage.start_date:time_coverage.end_date
    elseif time_coverage.format == "day" && time_coverage.start_date isa Date && time_coverage.end_date isa Date
        return time_coverage.start_date:Day(1):time_coverage.end_date
    elseif time_coverage.format in ["quarter", "semester", "trimester", "month"]
        # For these, generate the full sequence
        return generate_period_sequence(time_coverage)
    else
        # Return empty for unknown formats
        return String[]
    end
end

"""
    generate_period_sequence(time_coverage::TimeAvailability) -> Vector{String}

Generates a sequence of period strings for formats like quarters, semesters, etc.
"""
function generate_period_sequence(time_coverage::TimeAvailability)
    periods = String[]
    
    if time_coverage.format == "quarter" && time_coverage.start_date isa String && time_coverage.end_date isa String
        # Parse start and end quarters
        start_year = parse(Int, time_coverage.start_date[1:4])
        start_qtr = parse(Int, time_coverage.start_date[end])
        end_year = parse(Int, time_coverage.end_date[1:4])
        end_qtr = parse(Int, time_coverage.end_date[end])
        
        for year in start_year:end_year
            q_start = (year == start_year) ? start_qtr : 1
            q_end = (year == end_year) ? end_qtr : 4
            for q in q_start:q_end
                push!(periods, string(year) * "-Q" * string(q))
            end
        end
    elseif time_coverage.format == "semester" && time_coverage.start_date isa String && time_coverage.end_date isa String
        start_year = parse(Int, time_coverage.start_date[1:4])
        start_sem = parse(Int, time_coverage.start_date[end])
        end_year = parse(Int, time_coverage.end_date[1:4])
        end_sem = parse(Int, time_coverage.end_date[end])
        
        for year in start_year:end_year
            s_start = (year == start_year) ? start_sem : 1
            s_end = (year == end_year) ? end_sem : 2
            for s in s_start:s_end
                push!(periods, string(year) * "-S" * string(s))
            end
        end
    elseif time_coverage.format == "month" && time_coverage.start_date isa String && time_coverage.end_date isa String
        start_year = parse(Int, time_coverage.start_date[1:4])
        start_month = parse(Int, time_coverage.start_date[6:7])
        end_year = parse(Int, time_coverage.end_date[1:4])
        end_month = parse(Int, time_coverage.end_date[6:7])
        
        for year in start_year:end_year
            m_start = (year == start_year) ? start_month : 1
            m_end = (year == end_year) ? end_month : 12
            for m in m_start:m_end
                push!(periods, string(year) * "-" * lpad(m, 2, '0'))
            end
        end
    end
    
    return periods
end

"""
    print_availability_summary(availability::AvailabilityConstraint)

Prints a human-readable summary of the availability constraint.
"""
function print_availability_summary(availability::AvailabilityConstraint)
    println("=== SDMX Availability Summary ===")
    println("Constraint: $(availability.constraint_name)")
    println("Dataflow: $(availability.dataflow_ref.agency):$(availability.dataflow_ref.id)")
    println("Total Observations: $(availability.total_observations)")
    
    if availability.time_coverage !== nothing
        time_info = availability.time_coverage
        println("Time Coverage: $(time_info.start_date) to $(time_info.end_date)")
    end
    
    println("\nDimension Coverage:")
    for dim in availability.dimensions
        sample_values = join(dim.available_values[1:min(3, length(dim.available_values))], ", ")
        more_text = length(dim.available_values) > 3 ? " (and $(length(dim.available_values) - 3) more)" : ""
        println("  $(dim.dimension_id): $(dim.total_count) values - $sample_values$more_text")
    end
    
    println("Extracted: $(availability.extraction_timestamp)")
end