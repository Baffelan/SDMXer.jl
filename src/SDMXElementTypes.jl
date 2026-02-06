"""
SDMX Element Type Definitions for Generated Function Specialization

This module defines a type hierarchy for SDMX elements that enables compile-time
specialization of parsing functions using @generated functions. Each SDMX element
type corresponds to specific XML structures and parsing requirements.
"""

using EzXML

"""
    SDMXElement

Abstract base type for all SDMX element types used in generated function dispatch.

This type hierarchy enables compile-time specialization of parsing functions,
allowing the Julia compiler to generate optimized code paths for each specific
SDMX element type without runtime type checking overhead.

# See also
[`extract_sdmx_element`](@ref), [`DimensionElement`](@ref), [`AttributeElement`](@ref)
"""
abstract type SDMXElement end

"""
    DimensionElement <: SDMXElement

Type representing SDMX dimension elements for specialized parsing.

Dimensions define the key attributes that categorize statistical data in SDMX datasets.
This type enables compile-time generation of optimized parsing code for dimension
structures including concept references, codelist bindings, and position information.

# Examples
```julia
# Use with generated parsing function
dimension_data = extract_sdmx_element(DimensionElement, dimension_node)
println(dimension_data.dimension_id)
println(dimension_data.position)
```

# See also
[`extract_sdmx_element`](@ref), [`AttributeElement`](@ref), [`MeasureElement`](@ref)
"""
struct DimensionElement <: SDMXElement end

"""
    AttributeElement <: SDMXElement

Type representing SDMX attribute elements for specialized parsing.

Attributes provide additional metadata about statistical observations in SDMX datasets.
This type enables compile-time generation of optimized parsing code for attribute
structures including assignment status, attachment levels, and concept references.

# Examples
```julia
# Use with generated parsing function
attribute_data = extract_sdmx_element(AttributeElement, attribute_node)
println(attribute_data.attribute_id)
println(attribute_data.assignment_status)
```

# See also
[`extract_sdmx_element`](@ref), [`DimensionElement`](@ref), [`MeasureElement`](@ref)
"""
struct AttributeElement <: SDMXElement end

"""
    MeasureElement <: SDMXElement

Type representing SDMX measure elements for specialized parsing.

Measures define the actual statistical values being reported in SDMX datasets.
This type enables compile-time generation of optimized parsing code for measure
structures including data types, units of measure, and concept references.

# Examples
```julia
# Use with generated parsing function
measure_data = extract_sdmx_element(MeasureElement, measure_node)
println(measure_data.measure_id)
println(measure_data.data_type)
```

# See also
[`extract_sdmx_element`](@ref), [`DimensionElement`](@ref), [`AttributeElement`](@ref)
"""
struct MeasureElement <: SDMXElement end

"""
    ConceptElement <: SDMXElement

Type representing SDMX concept elements for specialized parsing.

Concepts define the semantic meaning of dimensions, attributes, and measures in SDMXer.
This type enables compile-time generation of optimized parsing code for concept
structures including names, descriptions, and classifications.

# Examples
```julia
# Use with generated parsing function
concept_data = extract_sdmx_element(ConceptElement, concept_node)
println(concept_data.concept_id)
println(concept_data.name)
```

# See also
[`extract_sdmx_element`](@ref), [`CodelistElement`](@ref)
"""
struct ConceptElement <: SDMXElement end

"""
    CodelistElement <: SDMXElement

Type representing SDMX codelist elements for specialized parsing.

Codelists define the valid values (codes) that can be used for dimensions and attributes.
This type enables compile-time generation of optimized parsing code for codelist
structures including code hierarchies, names, and parent-child relationships.

# Examples
```julia
# Use with generated parsing function
codelist_data = extract_sdmx_element(CodelistElement, codelist_node)
println(codelist_data.codelist_id)
println(codelist_data.codes)
```

# See also
[`extract_sdmx_element`](@ref), [`ConceptElement`](@ref)
"""
struct CodelistElement <: SDMXElement end

"""
    AvailabilityElement <: SDMXElement

Type representing SDMX availability constraint elements for specialized parsing.

Availability constraints define what data is actually available for specific
dimensions and time periods. This type enables compile-time generation of optimized
parsing code for availability structures including dimension values and time ranges.

# Examples
```julia
# Use with generated parsing function
availability_data = extract_sdmx_element(AvailabilityElement, availability_node)
println(availability_data.dimension_values)
println(availability_data.time_periods)
```

# See also
[`extract_sdmx_element`](@ref), [`TimeElement`](@ref)
"""
struct AvailabilityElement <: SDMXElement end

"""
    TimeElement <: SDMXElement

Type representing SDMX time dimension elements for specialized parsing.

Time elements define temporal aspects of statistical data including periods,
frequency, and time ranges. This type enables compile-time generation of optimized
parsing code for time structures including start/end dates and period formats.

# Examples
```julia
# Use with generated parsing function
time_data = extract_sdmx_element(TimeElement, time_node)
println(time_data.start_period)
println(time_data.end_period)
```

# See also
[`extract_sdmx_element`](@ref), [`AvailabilityElement`](@ref)
"""
struct TimeElement <: SDMXElement end