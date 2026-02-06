"""
Concept extraction functions for SDMXer.jl
"""

using EzXML, DataFrames, HTTP


"""
    extract_concepts(doc::EzXML.Document) -> DataFrame

Extracts concept definitions and their structural roles from SDMX documents.

This function parses SDMX structure documents to extract concept schemas,
including concept identifiers, descriptions, variable mappings, and their roles
within the data structure (dimension, attribute, measure, or time dimension).

# Arguments
- `doc::EzXML.Document`: Parsed SDMX XML structure document

# Returns
- `DataFrame`: Concept definitions with columns:
  - `concept_id::String`: Unique concept identifier
  - `description::Union{String,Missing}`: Human-readable concept description
  - `variable::String`: Variable identifier used in data structure
  - `role::String`: Structural role ("dimension", "attribute", "measure", "time_dimension")

# Examples
```julia
# Extract from parsed XML document
doc = parsexml(xml_string)
concepts = extract_concepts(doc)

# View concept roles
println("Dimensions: ", filter(r -> r.role == "dimension", concepts).concept_id)
println("Measures: ", filter(r -> r.role == "measure", concepts).concept_id)

# Find concept descriptions
concept_desc = Dict(c.concept_id => c.description for c in eachrow(concepts))
```

# See also
[`extract_concepts`](@ref), [`extract_dataflow_schema`](@ref)
"""
function extract_concepts(doc::EzXML.Document)
    rootnode = root(doc)
    # Find all ConceptScheme nodes
    concept_nodes = findall("//structure:Concept", rootnode)
    # Map concept id to description
    concept_map = Dict{String, String}()
    for node in concept_nodes
        cid = node["id"]
        desc = missing
        name_nodes = findall(".//common:Name", node)
        if !isempty(name_nodes)
            desc = nodecontent(name_nodes[1])
        end
        concept_map[cid] = desc
    end
    # Find DSD (DataStructureDefinition) and its components
    # Dimensions
    dim_nodes = findall("//structure:Dimension", rootnode)
    # Attributes
    attr_nodes = findall("//structure:Attribute", rootnode)
    # Measures
    meas_nodes = findall("//structure:PrimaryMeasure", rootnode)
    # Time dimension
    time_nodes = findall("//structure:TimeDimension", rootnode)
    # Collect all
    rows = []
    for node in dim_nodes
        concept_ref = findfirst(".//structure:ConceptIdentity/Ref", node)
        if concept_ref !== nothing
            cid = concept_ref["id"]
            push!(rows, (concept_id=cid, description=get(concept_map, cid, missing), variable=node["id"], role="dimension"))
        end
    end
    for node in attr_nodes
        concept_ref = findfirst(".//structure:ConceptIdentity/Ref", node)
        if concept_ref !== nothing
            cid = concept_ref["id"]
            push!(rows, (concept_id=cid, description=get(concept_map, cid, missing), variable=node["id"], role="attribute"))
        end
    end
    for node in meas_nodes
        concept_ref = findfirst(".//structure:ConceptIdentity/Ref", node)
        if concept_ref !== nothing
            cid = concept_ref["id"]
            push!(rows, (concept_id=cid, description=get(concept_map, cid, missing), variable=node["id"], role="measure"))
        end
    end
    for node in time_nodes
        concept_ref = findfirst(".//structure:ConceptIdentity/Ref", node)
        if concept_ref !== nothing
            cid = concept_ref["id"]
            push!(rows, (concept_id=cid, description=get(concept_map, cid, missing), variable=node["id"], role="time_dimension"))
        end
    end
    return DataFrame(rows)
end

"""
    extract_concepts(input::String) -> DataFrame

Convenience function for concept extraction from URLs or XML strings.

This function automatically handles URL fetching and XML parsing, providing a
simple interface for concept extraction from either SDMX REST API endpoints
or XML content strings. It includes error handling for network and parsing issues.

# Arguments
- `input::String`: Either a URL to SDMX structure endpoint or XML content string

# Returns
- `DataFrame`: Concept definitions (same structure as document-based extraction), 
  or empty DataFrame with correct schema if extraction fails

# Examples
```julia
# Extract from SDMX REST API URL
url = "https://stats-sdmx-disseminate.pacificdata.org/rest/datastructure/SPC/DF_BP50/1.0"
concepts = extract_concepts(url)

# Extract from XML string
xml_content = read("datastructure.xml", String)
concepts = extract_concepts(xml_content)

# Handle potential failures gracefully
concepts = extract_concepts(possibly_invalid_url)
if nrow(concepts) == 0
    println("No concepts extracted - check URL or XML format")
else
    println("Extracted ", nrow(concepts), " concept definitions")
end
```

# See also
[`extract_concepts`](@ref), [`fetch_sdmx_xml`](@ref)
"""
function extract_concepts(input::String)
    try
        # Use the robust URL handling from SDMXHelpers
        xml_string = fetch_sdmx_xml(input)
        doc = parsexml(xml_string)
        return extract_concepts(doc)
    catch e
        println("Error during HTTP request or parsing: ", e)
        return DataFrame(concept_id=String[], description=String[], variable=String[], role=String[])
    end
end 