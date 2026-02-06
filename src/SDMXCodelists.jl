"""
Codelist-related functions for SDMXer.jl
"""

using EzXML, DataFrames, HTTP

"""
    get_parent_id(code_node::EzXML.Node) -> Union{String, Missing}

Extracts the parent code identifier from an SDMX code node's hierarchical structure.

This function searches for parent relationships in SDMX codelist hierarchies by
examining the `<structure:Parent/Ref>` XML element and extracting the referenced
parent code identifier, enabling hierarchical code analysis and navigation.

# Arguments
- `code_node::EzXML.Node`: The SDMX code XML node to examine for parent references

# Returns
- `Union{String, Missing}`: Parent code identifier if found, `missing` if no parent exists

# Examples
```julia
# Extract parent ID from hierarchical code
parent_id = get_parent_id(code_node)

if !ismissing(parent_id)
    println("Code has parent: ", parent_id)
else
    println("This is a top-level code")
end

# Use in codelist processing
for code_node in code_nodes
    parent = get_parent_id(code_node)
    code_id = code_node["id"]
    println("Code ", code_id, " -> Parent: ", parent)
end
```

# See also
[`process_code_node`](@ref)
"""
function get_parent_id(code_node::EzXML.Node)
    ref_node = findfirst(".//structure:Parent/Ref", code_node)
    return (ref_node !== nothing) ? ref_node["id"] : missing
end

"""
    process_code_node(code_node::EzXML.Node) -> Vector{NamedTuple}

Extracts comprehensive information from a single SDMX code node including multilingual
content.

This function processes an individual code element from an SDMX codelist, extracting
the code identifier, names in multiple languages, annotations, and hierarchical 
parent relationships. Returns separate records for each language variant found.

# Arguments
- `code_node::EzXML.Node`: The SDMX code XML node to process

# Returns
- `Vector{NamedTuple}`: Vector of code records, one per language, each containing:
  - `code_id::String`: The code identifier
  - `language::String`: Language code (e.g., "en", "fr")
  - `name::Union{String, Missing}`: Code name in this language
  - `annotation::Union{String, Missing}`: Code annotation/description in this language
  - `parent_id::Union{String, Missing}`: Parent code identifier if hierarchical

# Examples
```julia
# Process individual code node
code_records = process_code_node(code_node)

# Access multilingual information
for record in code_records
    println("Code: ", record.code_id)
    println("Language: ", record.language)
    println("Name: ", record.name)
    if !ismissing(record.parent_id)
        println("Parent: ", record.parent_id)
    end
end

# Filter for specific language
english_record = filter(r -> r.language == "en", code_records)[1]
println("English name: ", english_record.name)
```

# See also
[`get_parent_id`](@ref)
"""
function process_code_node(code_node::EzXML.Node)
    node_rows = []
    code_id = code_node["id"]
    names = Dict(n["xml:lang"] => nodecontent(n) 
                 for n in findall(".//common:Name", code_node))
    annotations = Dict(a["xml:lang"] => nodecontent(a) 
                      for a in findall(".//common:AnnotationText", code_node))
    parent_id = get_parent_id(code_node)
    all_langs = union(keys(names), keys(annotations))
    if isempty(all_langs)
        push!(all_langs, "default")
    end
    for lang in all_langs
        partial_row_data = (
            code_id = code_id,
            lang = (lang == "default" ? missing : lang),
            name = get(names, lang, missing),
            parent_code_id = parent_id,
            order = get(annotations, lang, missing)
        )
        push!(node_rows, partial_row_data)
    end
    return node_rows
end

"""
    extract_codes_from_codelist_node(cl_node::EzXML.Node) -> Vector{NamedTuple}

Extracts all code data from a single `<structure:Codelist>` node. It finds the codelist's
ID and merges it with the data extracted from each child Code node.

# Arguments
- `cl_node::EzXML.Node`: The codelist node to process.

# Returns
- `Vector{NamedTuple}`: A vector of named tuples with code and codelist information.
"""
function extract_codes_from_codelist_node(cl_node::EzXML.Node)
    codelist_id = cl_node["id"]
    code_nodes_in_cl = findall(".//structure:Code", cl_node)
    return mapreduce(vcat, code_nodes_in_cl, init=[]) do code_node
        partial_rows = process_code_node(code_node)
        [ (codelist_id=codelist_id, row...) for row in partial_rows ]
    end
end

"""
    extract_all_codelists(doc::EzXML.Document) -> DataFrame

The primary extraction function for a SDMX-like dataflow document. It operates on an XML
object (namely, an already-parsed `EzXML.Document` object).

This function traverses the document to find all `<structure:Codelist>` elements. For each codelist, it extracts all child `<structure:Code>` elements, capturing their IDs, names, annotations, and the ID of the parent codelist to ensure uniqueness. The results from all codelists are aggregated into a single, tidy DataFrame.

# Arguments
- `doc::EzXML.Document`: A parsed XML document object from which to extract data.

# Returns
- `DataFrame`: A tidy DataFrame containing all codes from all codelists in the document. The columns are:
    - `codelist_id::String`
    - `code_id::String`
    - `lang::Union{String, Missing}`
    - `name::Union{String, Missing}`
    - `parent_code_id::Union{String, Missing}`
    - `order::Union{String, Missing}`

If no codelists or codes are found, it returns an empty DataFrame with the correct schema.
"""
function extract_all_codelists(doc::EzXML.Document)
    doc_root = root(doc)
    codelist_nodes = findall("//structure:Codelist", doc_root)
    all_rows = mapreduce(extract_codes_from_codelist_node, vcat, codelist_nodes, init=[])
    if isempty(all_rows)
        return DataFrame(
            codelist_id=String[],
            code_id=String[],
            lang=Union{String, Missing}[], 
            name=Union{String, Missing}[],
            parent_code_id=Union{String, Missing}[],
            order=Union{String, Missing}[]
            )
    end
    return DataFrame(all_rows)
end



"""
    extract_all_codelists(input::String) -> DataFrame

Extracts all codelist data from a URL or XML string. Automatically detects whether the input is a URL or XML content.

# Arguments
- `input::String`: Either a URL to fetch SDMX XML data from, or raw XML content as a string.

# Returns  
- `DataFrame`: A tidy DataFrame containing all codes from all codelists.

# Examples
```julia
# From URL
codelists = extract_all_codelists("https://example.com/dataflow.xml")

# From XML string
codelists = extract_all_codelists(xml_string)
```
"""
function extract_all_codelists(input::String)
    @assert !isempty(input) "Input string cannot be empty"
    
    # Use the robust URL handling from SDMXHelpers
    xml_string = fetch_sdmx_xml(input)
    doc = parsexml(xml_string)
    return extract_all_codelists(doc)
end

"""
    extract_all_codelists(url::String, filter_by_availability::Bool) -> DataFrame

Extract codelists with automatic availability filtering.

# Arguments
- `url::String`: The dataflow URL to fetch the XML from.
- `filter_by_availability::Bool`: If true, automatically constructs availability URL and filters codes

# Returns
- `DataFrame`: A tidy DataFrame containing only codes that actually appear in published data.

# Examples
```julia
# Get only codes that have actual published data (auto-constructed availability URL)
available_codes = extract_all_codelists(dataflow_url, true)
```
"""
function extract_all_codelists(url::String, filter_by_availability::Bool)
    codelists_df = extract_all_codelists(url)  # Get all codelists first
    
    if filter_by_availability
        return filter_codelists_by_availability(codelists_df, url, "")  # Empty string triggers auto-construction
    else
        return codelists_df
    end
end

"""
    extract_all_codelists(url::String, availability_url::String) -> DataFrame

Extract codelists with custom availability URL filtering.

# Arguments
- `url::String`: The dataflow URL to fetch the XML from.
- `availability_url::String`: Custom availability constraint URL (can include dimension filters)

# Returns
- `DataFrame`: A tidy DataFrame containing only codes that appear in the specified availability constraint.

# Examples
```julia
# Use custom availability URL with dimension filters
available_codes = extract_all_codelists(dataflow_url, 
    "https://stats-sdmx-disseminate.pacificdata.org/rest/availableconstraint/DF_BP50/A..NR........")

# Standard availability URL
available_codes = extract_all_codelists(dataflow_url,
    "https://stats-sdmx-disseminate.pacificdata.org/rest/availableconstraint/DF_DISABILITY/")
```
"""
function extract_all_codelists(url::String, availability_url::String)
    codelists_df = extract_all_codelists(url)  # Get all codelists first
    return filter_codelists_by_availability(codelists_df, url, availability_url)
end

"""
    filter_codelists_by_availability(codelists_df::DataFrame, dataflow_url::String, availability_url::String="") -> DataFrame

Filters a codelists DataFrame to include only codes that actually appear in published data.

# Arguments
- `codelists_df::DataFrame`: The full codelists DataFrame to filter
- `dataflow_url::String`: The original dataflow URL (used to construct availability URL if needed)
- `availability_url::String`: Explicit availability constraint URL (optional)

# Returns
- `DataFrame`: Filtered codelists containing only codes with actual published data
"""
function filter_codelists_by_availability(codelists_df::DataFrame, dataflow_url::String, availability_url::String="")
    try
        # First, try to get availability constraint based on what was provided
        availability = nothing
        
        if isempty(availability_url)
            # No specific availability URL provided - try to get from dataflow document itself
            println("Attempting to extract availability constraint from dataflow document...")
            xml_string = fetch_sdmx_xml(dataflow_url)
            doc = parsexml(xml_string)
            availability = extract_availability_from_dataflow(doc)
            
            if availability === nothing
                # No embedded constraint, try constructing availability URL
                availability_url = construct_availability_url(dataflow_url)
                
                # Add trailing slash if missing (SDMX APIs often require this)
                if !isempty(availability_url) && !endswith(availability_url, "/")
                    availability_url = availability_url * "/"
                end
                
                if isempty(availability_url)
                    @warn "Could not find embedded availability constraint or construct availability URL from dataflow URL: $dataflow_url"
                    return codelists_df
                end
                
                println("No embedded constraint found, attempting to fetch from: $availability_url")
                availability = extract_availability(availability_url)
            else
                println("Using embedded availability constraint from dataflow document")
            end
        else
            # Specific availability URL provided - use it
            # Add trailing slash if missing (SDMX APIs often require this)
            if !endswith(availability_url, "/")
                availability_url = availability_url * "/"
            end
            println("Attempting to fetch availability from provided URL: $availability_url")
            availability = extract_availability(availability_url)
        end
        
        # Get dataflow schema
        schema = extract_dataflow_schema(dataflow_url)
        
        # Create dimension-specific mapping of available values
        available_by_dimension = Dict{String, Set{String}}()
        for dim in availability.dimensions
            available_by_dimension[dim.dimension_id] = Set(dim.available_values)
        end
        
        # Build complete mapping from dimension_id to codelist_id using schema
        dimension_to_codelist = Dict{String, String}()
        
        # Add dimensions
        for row in eachrow(schema.dimensions)
            if !ismissing(row.codelist_id)
                dimension_to_codelist[row.dimension_id] = row.codelist_id
            end
        end
        
        # Add attributes (they can have codelists too)
        for row in eachrow(schema.attributes)
            if !ismissing(row.codelist_id)
                dimension_to_codelist[row.attribute_id] = row.codelist_id
            end
        end
        
        # Add time dimension
        if !ismissing(schema.time_dimension.codelist_id)
            dimension_to_codelist[schema.time_dimension.dimension_id] = schema.time_dimension.codelist_id
        end
        
        # Create reverse mapping: codelist_id -> dimension_id
        codelist_to_dimension = Dict{String, String}()
        for (dim_id, codelist_id) in dimension_to_codelist
            codelist_to_dimension[codelist_id] = dim_id
        end
        
        # Filter codelists based on dimension-specific availability
        filtered_rows = []
        for row in eachrow(codelists_df)
            codelist_id = row.codelist_id
            code_id = row.code_id
            
            # Map codelist to dimension using schema
            dimension_id = get(codelist_to_dimension, codelist_id, nothing)
            
            if dimension_id !== nothing && haskey(available_by_dimension, dimension_id)
                # Filter based on dimension-specific available values
                if code_id in available_by_dimension[dimension_id]
                    push!(filtered_rows, row)
                end
            else
                # If codelist is not mapped to any dimension in availability constraints, include it
                # (could be attributes, measures, or other metadata codelists not constrained)
                push!(filtered_rows, row)
            end
        end
        
        filtered_df = DataFrame(filtered_rows)
        
        # Add metadata about the filtering
        if nrow(filtered_df) < nrow(codelists_df)
            filtered_count = nrow(filtered_df)
            total_count = nrow(codelists_df)
            println("Filtered codelists: $(filtered_count) available codes out of $(total_count) total codes")
        end
        
        return filtered_df
        
    catch e
        @warn "Failed to filter by availability: $e. Returning unfiltered codelists."
        return codelists_df
    end
end

"""
    map_codelist_to_dimension(codelist_id::String) -> Union{String, Nothing}

Maps a codelist ID to its corresponding dimension ID using common SDMX patterns.
Returns nothing if no mapping can be determined.

# Examples
- "CL_GEO_PICT" -> "GEO_PICT"  
- "CL_INDICATOR" -> "INDICATOR"
- "CL_FREQ" -> "FREQ"
- "INDICATOR" -> "INDICATOR" (direct match)
"""
function map_codelist_to_dimension(::Missing)
    return nothing
end
function map_codelist_to_dimension(codelist_id::Union{String, Nothing})
    if isnothing(codelist_id)
        return nothing
    end
    
    # Convert to uppercase for consistent matching
    codelist_upper = uppercase(codelist_id)
    
    # Common SDMX patterns:
    # 1. CL_DIMENSION -> DIMENSION
    if startswith(codelist_upper, "CL_")
        return codelist_upper[4:end]  # Remove "CL_" prefix
    end
    
    # 2. Direct dimension name matches (common dimensions)
    common_dimensions = ["GEO_PICT", "INDICATOR", "FREQ", "TIME_PERIOD", "UNIT_MEASURE", 
                        "OBS_STATUS", "OBS_CONF", "REF_AREA", "SEX", "AGE", "EDUCATION"]
    
    for dim in common_dimensions
        if codelist_upper == dim || occursin(dim, codelist_upper)
            return dim
        end
    end
    
    # 3. Pattern matching for common variations
    if occursin("GEO", codelist_upper) || occursin("COUNTRY", codelist_upper)
        return "GEO_PICT"
    elseif occursin("INDICATOR", codelist_upper) || occursin("MEASURE", codelist_upper)
        return "INDICATOR" 
    elseif occursin("FREQ", codelist_upper)
        return "FREQ"
    elseif occursin("TIME", codelist_upper)
        return "TIME_PERIOD"
    elseif occursin("UNIT", codelist_upper)
        return "UNIT_MEASURE"
    end
    
    # If no pattern matches, return the original (might be a direct dimension name)
    return codelist_id
end

"""
    construct_availability_url(dataflow_url::String) -> String

Attempts to construct an availability constraint URL from a dataflow URL.

Supports common SDMX URL patterns like:
- `.../rest/dataflow/AGENCY/DATAFLOW_ID/VERSION?references=all` 
- `.../rest/dataflow/DATAFLOW_ID/VERSION?references=all`
"""
function construct_availability_url(dataflow_url::String)
    try
        # Parse the URL to extract components
        # Common pattern: https://host/rest/dataflow/AGENCY/DATAFLOW_ID/VERSION?references=all
        
        # Remove query parameters
        base_url = split(dataflow_url, '?')[1]
        
        # Split by '/' and find the dataflow pattern
        parts = split(base_url, '/')
        
        # Find where "dataflow" appears
        dataflow_idx = findfirst(p -> p == "dataflow", parts)
        if dataflow_idx === nothing
            return ""
        end
        
        # Extract base URL (everything before /rest/dataflow)
        rest_idx = findfirst(p -> p == "rest", parts)
        if rest_idx === nothing
            return ""
        end
        
        base_host = join(parts[1:rest_idx], "/")
        
        # Extract dataflow ID (could be parts[dataflow_idx+1] or parts[dataflow_idx+2] depending on if agency is included)
        if length(parts) >= dataflow_idx + 2
            # Try with agency: /dataflow/AGENCY/DATAFLOW_ID/VERSION
            dataflow_id = parts[dataflow_idx + 2]
        elseif length(parts) >= dataflow_idx + 1  
            # Try without agency: /dataflow/DATAFLOW_ID/VERSION
            dataflow_id = parts[dataflow_idx + 1]
        else
            return ""
        end
        
        # Construct availability URL
        availability_url = "$base_host/availableconstraint/$dataflow_id/"
        return availability_url
        
    catch e
        @warn "Error constructing availability URL from $dataflow_url: $e"
        return ""
    end
end

"""
    get_available_codelist_summary(dataflow_url::String; availability_url::String="") -> Dict{String, Any}

Gets a summary of codelist availability without downloading full codelists.

Returns information about which dimensions have data and how many values are available.
"""
function get_available_codelist_summary(dataflow_url::String; availability_url::String="")
    if isempty(availability_url)
        availability_url = construct_availability_url(dataflow_url)
    end
    
    if isempty(availability_url)
        error("Could not construct availability URL from: $dataflow_url")
    end
    
    try
        availability = extract_availability(availability_url)
        
        summary = Dict{String, Any}(
            "dataflow_id" => availability.dataflow_ref.id,
            "total_observations" => availability.total_observations,
            "dimensions_with_data" => Dict{String, Int}()
        )
        
        for dim in availability.dimensions
            summary["dimensions_with_data"][dim.dimension_id] = dim.total_count
        end
        
        return summary
        
    catch e
        error("Failed to get availability summary: $e")
    end
end 