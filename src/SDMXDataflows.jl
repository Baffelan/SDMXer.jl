"""
Dataflow structure extraction functions for SDMXer.jl

This module extracts complete dataflow schema information including:
- Dataflow metadata (name, description, agency, version)
- Data Structure Definition (DSD) with dimensions, attributes, and measures
- Position ordering, data types, and codelist references
- Required vs conditional attribute assignments
"""

using EzXML, DataFrames, HTTP

export extract_dataflow_schema

"""
    DataflowSchema

Complete schema information for an SDMX dataflow including structure, dimensions, and
metadata.

This struct contains all essential schema information needed to validate, transform,
and work with SDMX-CSV data. It provides comprehensive metadata about the dataflow
structure, dimension ordering, attribute requirements, and measure definitions.

# Fields
- `dataflow_info::NamedTuple`: Basic dataflow metadata including id, agency, version,
  name, and description
- `dimensions::DataFrame`: All dimensions with position ordering, concept references,
  and codelist information
- `attributes::DataFrame`: All attributes with assignment status (required/conditional),
  concept and codelist info
- `measures::DataFrame`: Primary measure definitions with concept references and data
  type specifications
- `time_dimension::Union{NamedTuple, Nothing}`: Special time dimension information if present in the dataflow

# Examples
```julia
# Extract schema from SDMX-ML
schema = extract_dataflow_schema("SPC", "DF_BP50", "1.0")

# Access dataflow information
println("Dataflow: ", schema.dataflow_info.name)
println("Agency: ", schema.dataflow_info.agency)

# Examine dimensions
println("Number of dimensions: ", nrow(schema.dimensions))
println("Dimension names: ", schema.dimensions.concept_id)

# Check for time dimension
if schema.time_dimension !== nothing
    println("Time dimension: ", schema.time_dimension.concept_id)
end

# Review attributes
required_attrs = filter(row -> row.assignment_status == "Mandatory", schema.attributes)
println("Required attributes: ", required_attrs.concept_id)
```

# See also
- [`extract_dataflow_schema`](@ref): constructs this type from XML or URL
- [`compare_schemas`](@ref): compares two `DataflowSchema` objects for joinability
- [`create_validator`](@ref): builds a validator from a schema
- [`query_sdmx_data`](@ref): fetches data described by a schema
"""
struct DataflowSchema
    dataflow_info::NamedTuple
    dimensions::DataFrame
    attributes::DataFrame
    measures::DataFrame
    time_dimension::Union{NamedTuple, Nothing}
end

"""
    extract_dimension_info(dim_node::EzXML.Node) -> NamedTuple

Extracts comprehensive dimension information from a single SDMX Dimension or TimeDimension XML node.

This function parses an SDMX dimension node to extract all relevant metadata including
position, concept references, codelist associations, and data type information needed
for proper dimension handling in SDMX data processing.

# Arguments
- `dim_node::EzXML.Node`: XML node representing a Dimension or TimeDimension element

# Returns
- `NamedTuple`: Dimension metadata with fields:
  - `id::String`: Dimension identifier
  - `position::Int`: Position in dimension ordering
  - `concept_id::Union{String, Missing}`: Referenced concept identifier
  - `concept_scheme::Union{String, Missing}`: Concept scheme reference
  - `codelist_id::Union{String, Missing}`: Associated codelist identifier
  - `codelist_agency::Union{String, Missing}`: Codelist maintaining agency
  - `data_type::String`: Dimension data type specification

# Examples
```julia
# Extract dimension info from XML node
dim_info = extract_dimension_info(dimension_node)

# Access dimension properties
println("Dimension ID: ", dim_info.id)
println("Position: ", dim_info.position)
println("Concept: ", dim_info.concept_id)

# Check for codelist reference
if !ismissing(dim_info.codelist_id)
    println("Uses codelist: ", dim_info.codelist_id)
end
```

# See also
[`extract_dataflow_schema`](@ref), [`extract_attribute_info`](@ref), [`DataflowSchema`](@ref)
"""
function extract_dimension_info(dim_node::EzXML.Node)
    dim_id = dim_node["id"]
    position = parse(Int, dim_node["position"])

    # Get concept reference
    concept_ref = findfirst(".//structure:ConceptIdentity/Ref", dim_node)
    concept_id = concept_ref !== nothing ? concept_ref["id"] : missing
    concept_scheme = concept_ref !== nothing && haskey(concept_ref, "maintainableParentID") ? concept_ref["maintainableParentID"] : missing

    # Get codelist reference (if enumeration)
    codelist_ref = findfirst(".//structure:LocalRepresentation/structure:Enumeration/Ref", dim_node)
    codelist_id = codelist_ref !== nothing ? codelist_ref["id"] : missing
    codelist_agency = codelist_ref !== nothing ? codelist_ref["agencyID"] : missing
    codelist_version = codelist_ref !== nothing ? codelist_ref["version"] : missing

    # Get text format (if non-enumerated)
    text_format = findfirst(".//structure:LocalRepresentation/structure:TextFormat", dim_node)
    data_type = text_format !== nothing && haskey(text_format, "textType") ? text_format["textType"] : missing

    # Determine if this is a time dimension
    is_time_dimension = nodename(dim_node) == "TimeDimension"

    return (
        dimension_id = dim_id,
        position = position,
        concept_id = concept_id,
        concept_scheme = concept_scheme,
        codelist_id = codelist_id,
        codelist_agency = codelist_agency,
        codelist_version = codelist_version,
        data_type = data_type,
        is_time_dimension = is_time_dimension
    )
end

"""
    extract_attribute_info(attr_node::EzXML.Node) -> NamedTuple

Extracts comprehensive attribute information from a single SDMX Attribute XML node.

This function parses an SDMX attribute node to extract metadata including assignment
status, concept references, codelist associations, and attachment level information
required for proper attribute handling in SDMX data structures.

# Arguments
- `attr_node::EzXML.Node`: XML node representing an Attribute element

# Returns
- `NamedTuple`: Attribute metadata with fields:
  - `id::String`: Attribute identifier
  - `assignment_status::String`: Required status ("Mandatory" or "Conditional")
  - `concept_id::Union{String, Missing}`: Referenced concept identifier
  - `concept_scheme::Union{String, Missing}`: Concept scheme reference
  - `codelist_id::Union{String, Missing}`: Associated codelist identifier
  - `codelist_agency::Union{String, Missing}`: Codelist maintaining agency
  - `attachment_level::String`: Where attribute is attached (dataset, dimension, observation)

# Examples
```julia
# Extract attribute info from XML node
attr_info = extract_attribute_info(attribute_node)

# Check attribute requirements
if attr_info.assignment_status == "Mandatory"
    println("Required attribute: ", attr_info.id)
end

# Access concept and codelist information
println("Concept: ", attr_info.concept_id)
println("Attachment: ", attr_info.attachment_level)

# Check for codelist constraints
if !ismissing(attr_info.codelist_id)
    println("Constrained by codelist: ", attr_info.codelist_id)
end
```

# See also
[`extract_dimension_info`](@ref), [`extract_dataflow_schema`](@ref), [`DataflowSchema`](@ref)
"""
function extract_attribute_info(attr_node::EzXML.Node)
    attr_id = attr_node["id"]
    assignment_status = haskey(attr_node, "assignmentStatus") ? attr_node["assignmentStatus"] : "Mandatory"

    # Get concept reference
    concept_ref = findfirst(".//structure:ConceptIdentity/Ref", attr_node)
    concept_id = concept_ref !== nothing ? concept_ref["id"] : missing
    concept_scheme = concept_ref !== nothing && haskey(concept_ref, "maintainableParentID") ? concept_ref["maintainableParentID"] : missing

    # Get codelist reference (if enumeration)
    codelist_ref = findfirst(".//structure:LocalRepresentation/structure:Enumeration/Ref", attr_node)
    codelist_id = codelist_ref !== nothing ? codelist_ref["id"] : missing
    codelist_agency = codelist_ref !== nothing ? codelist_ref["agencyID"] : missing
    codelist_version = codelist_ref !== nothing ? codelist_ref["version"] : missing

    # Get text format (if non-enumerated)
    text_format = findfirst(".//structure:LocalRepresentation/structure:TextFormat", attr_node)
    data_type = text_format !== nothing && haskey(text_format, "textType") ? text_format["textType"] : missing

    # Get attribute relationship (what it attaches to)
    relationship = "Dataset"  # default
    if findfirst(".//structure:AttributeRelationship/structure:PrimaryMeasure", attr_node) !== nothing
        relationship = "Observation"
    elseif findfirst(".//structure:AttributeRelationship/structure:Dimension", attr_node) !== nothing
        relationship = "Dimension"
    end

    return (
        attribute_id = attr_id,
        assignment_status = assignment_status,
        concept_id = concept_id,
        concept_scheme = concept_scheme,
        codelist_id = codelist_id,
        codelist_agency = codelist_agency,
        codelist_version = codelist_version,
        data_type = data_type,
        relationship = relationship
    )
end

"""
    extract_measure_info(measure_node::EzXML.Node) -> NamedTuple

Extracts information from a PrimaryMeasure node.
"""
function extract_measure_info(measure_node::EzXML.Node)
    measure_id = measure_node["id"]

    # Get concept reference
    concept_ref = findfirst(".//structure:ConceptIdentity/Ref", measure_node)
    concept_id = concept_ref !== nothing ? concept_ref["id"] : missing
    concept_scheme = concept_ref !== nothing && haskey(concept_ref, "maintainableParentID") ? concept_ref["maintainableParentID"] : missing

    # Get text format
    text_format = findfirst(".//structure:LocalRepresentation/structure:TextFormat", measure_node)
    data_type = text_format !== nothing && haskey(text_format, "textType") ? text_format["textType"] : "Double"

    return (
        measure_id = measure_id,
        concept_id = concept_id,
        concept_scheme = concept_scheme,
        data_type = data_type
    )
end

"""
    extract_dataflow_schema(doc::EzXML.Document) -> DataflowSchema

Extracts complete dataflow schema information from an SDMX structure document.

# Arguments
- `doc::EzXML.Document`: The parsed SDMX XML document.

# Returns
- `DataflowSchema`: A comprehensive schema object with all dataflow structure information.

# See also
[`extract_dataflow_schema(::String)`](@ref), [`DataflowSchema`](@ref), [`compare_schemas`](@ref), [`create_validator`](@ref)
"""
function extract_dataflow_schema(doc::EzXML.Document)
    rootnode = root(doc)

    # Extract dataflow basic information
    dataflow_node = findfirst("//structure:Dataflow", rootnode)
    if dataflow_node === nothing
        error("No dataflow found in document")
    end

    dataflow_info = (
        id = dataflow_node["id"],
        agency = dataflow_node["agencyID"],
        version = dataflow_node["version"],
        name = begin
            name_node = findfirst(".//common:Name[@xml:lang='en']", dataflow_node)
            name_node !== nothing ? nodecontent(name_node) : missing
        end,
        description = begin
            desc_node = findfirst(".//common:Description[@xml:lang='en']", dataflow_node)
            desc_node !== nothing ? nodecontent(desc_node) : missing
        end,
        dsd_id = begin
            dsd_ref = findfirst(".//structure:Structure/Ref", dataflow_node)
            dsd_ref !== nothing ? dsd_ref["id"] : missing
        end
    )

    # Find the corresponding Data Structure Definition
    dsd_node = findfirst("//structure:DataStructure[@id='$(dataflow_info.dsd_id)']", rootnode)
    if dsd_node === nothing
        error("Data Structure Definition '$(dataflow_info.dsd_id)' not found")
    end

    # Extract dimensions
    dimension_nodes = findall(".//structure:DimensionList/structure:Dimension", dsd_node)
    time_dim_nodes = findall(".//structure:DimensionList/structure:TimeDimension", dsd_node)

    all_dim_nodes = vcat(dimension_nodes, time_dim_nodes)
    dimension_data = [extract_dimension_info(node) for node in all_dim_nodes]

    # Separate time dimension if present
    time_dimension = nothing
    regular_dimensions = dimension_data
    if !isempty(time_dim_nodes)
        time_dims = filter(d -> d.is_time_dimension, dimension_data)
        if !isempty(time_dims)
            time_dimension = time_dims[1]
            regular_dimensions = filter(d -> !d.is_time_dimension, dimension_data)
        end
    end

    dimensions_df = DataFrame(regular_dimensions)

    # Extract attributes
    attribute_nodes = findall(".//structure:AttributeList/structure:Attribute", dsd_node)
    attribute_data = [extract_attribute_info(node) for node in attribute_nodes]
    attributes_df = DataFrame(attribute_data)

    # Extract measures
    measure_nodes = findall(".//structure:MeasureList/structure:PrimaryMeasure", dsd_node)
    measure_data = [extract_measure_info(node) for node in measure_nodes]
    measures_df = DataFrame(measure_data)

    return DataflowSchema(dataflow_info, dimensions_df, attributes_df, measures_df, time_dimension)
end

"""
    extract_dataflow_schema(input::String) -> DataflowSchema

Extracts dataflow schema from SDMX-ML content provided as URL or XML string.

This convenience function automatically handles both URL-based SDMX API calls and
direct XML string parsing. It downloads schema information from SDMX web services
or processes local XML content to build a complete DataflowSchema.

# Arguments
- `input::String`: Either a URL to SDMX dataflow schema or raw SDMX-ML XML content

# Returns
- `DataflowSchema`: Complete schema with dimensions, attributes, measures, and metadata

# Examples
```julia
# Extract from SDMX API URL
url = "https://stats-sdmx-disseminate.pacificdata.org/rest/datastructure/SPC/DF_BP50"
schema = extract_dataflow_schema(url)

# Extract from local XML file content
xml_content = read("dataflow.xml", String)
schema = extract_dataflow_schema(xml_content)

# Use the schema
println("Dataflow: ", schema.dataflow_info.name)
println("Dimensions: ", nrow(schema.dimensions))
println("Attributes: ", nrow(schema.attributes))
```

# Throws
- `HTTP.ExceptionRequest.StatusError`: If URL request fails
- `EzXML.XMLError`: If XML parsing fails
- `KeyError`: If required SDMX elements are missing

# See also
[`extract_dataflow_schema(::EzXML.Document)`](@ref), [`DataflowSchema`](@ref), [`fetch_sdmx_xml`](@ref), [`compare_schemas`](@ref), [`create_validator`](@ref)
"""
function extract_dataflow_schema(input::String)
    try
        # Use the robust URL handling from SDMXHelpers
        xml_string = fetch_sdmx_xml(input)
        doc = parsexml(xml_string)
        schema = extract_dataflow_schema(doc)
        if is_url(input)
            dataflow_info = merge(schema.dataflow_info, (url=normalize_sdmx_url(input),))
            return DataflowSchema(dataflow_info, schema.dimensions, schema.attributes, schema.measures, schema.time_dimension)
        end
        return schema
    catch e
        println("Error during HTTP request or parsing: ", e)
        rethrow(e)
    end
end

"""
    get_required_columns(schema::DataflowSchema) -> Vector{String}

Returns a vector of column names that are required for SDMX-CSV output.
This includes all dimensions, the primary measure, and mandatory attributes.
"""
function get_required_columns(schema::DataflowSchema)
    required_cols = String[]

    # All dimensions are required
    append!(required_cols, schema.dimensions.dimension_id)

    # Time dimension if present
    if schema.time_dimension !== nothing
        push!(required_cols, schema.time_dimension.dimension_id)
    end

    # Primary measure is required
    append!(required_cols, schema.measures.measure_id)

    # Mandatory attributes
    mandatory_attrs = filter(row -> row.assignment_status == "Mandatory", schema.attributes)
    append!(required_cols, mandatory_attrs.attribute_id)

    return required_cols
end

"""
    get_optional_columns(schema::DataflowSchema) -> Vector{String}

Returns a vector of column names that are optional for SDMX-CSV output.
This includes conditional attributes.
"""
function get_optional_columns(schema::DataflowSchema)
    optional_attrs = filter(row -> row.assignment_status == "Conditional", schema.attributes)
    return collect(optional_attrs.attribute_id)
end

"""
    get_codelist_columns(schema::DataflowSchema) -> Dict{String, NamedTuple}

Returns a dictionary mapping column names to their codelist information.
Only includes columns that have associated codelists.
"""
function get_codelist_columns(schema::DataflowSchema)
    codelist_cols = Dict{String, NamedTuple}()

    # Check dimensions
    for row in eachrow(schema.dimensions)
        if !ismissing(row.codelist_id)
            codelist_cols[row.dimension_id] = (
                codelist_id = row.codelist_id,
                agency = row.codelist_agency,
                version = row.codelist_version
            )
        end
    end

    # Check attributes
    for row in eachrow(schema.attributes)
        if !ismissing(row.codelist_id)
            codelist_cols[row.attribute_id] = (
                codelist_id = row.codelist_id,
                agency = row.codelist_agency,
                version = row.codelist_version
            )
        end
    end

    return codelist_cols
end

"""
    get_dimension_order(schema::DataflowSchema) -> Vector{String}

Returns the ordered list of regular dimension IDs for constructing SDMX data query keys.
Dimensions are ordered by their position. The time dimension is excluded because
per the SDMX 2.1 REST API spec it must be filtered via startPeriod/endPeriod query
parameters, not as a key position.
"""
function get_dimension_order(schema::DataflowSchema)
    # Sort dimensions by position
    sorted_dims = sort(schema.dimensions, :position)
    return collect(sorted_dims.dimension_id)
end
