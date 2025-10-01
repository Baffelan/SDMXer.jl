"""
Integration Examples and Utilities for Generated Function SDMX Parsing

This module provides examples, benchmarks, and migration utilities for the new
@generated function-based SDMX parsing system. It demonstrates how to use the
type-specialized parsing functions and provides performance comparisons.
"""

using EzXML
using ..SDMX: SDMXElement, DimensionElement, AttributeElement, MeasureElement, 
              extract_sdmx_element, get_xpath_patterns


"""
    demonstrate_generated_parsing(; verbose::Bool=true) -> Nothing

Demonstrate the usage of generated function SDMX parsing with examples.

This function provides a comprehensive demonstration of how to use the new
@generated function system for parsing different types of SDMX elements,
showing the performance benefits and ease of use.

# Arguments
- `verbose::Bool=true`: Whether to print output. Set to false for quiet operation during tests.

# Examples
```julia
# Run the full demonstration with output
demonstrate_generated_parsing()

# Run quietly (for testing)
demonstrate_generated_parsing(verbose=false)

# This will show examples of:
# - Type-specialized parsing for different element types
# - Performance comparisons with traditional methods
# - Integration with existing SDMX workflows
```

# See also
[`extract_sdmx_element`](@ref)
"""
function demonstrate_generated_parsing(; verbose::Bool=true)
    verbose && println("🚀 Generated Function SDMX Parsing Demonstration")
    verbose && println("=" ^ 60)
    
    # Example XML content for demonstration
    sample_xml = """
    <structure:DataStructure xmlns:structure="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
                            xmlns:common="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
        <structure:Dimension id="COUNTRY" position="1">
            <structure:ConceptIdentity>
                <Ref id="COUNTRY" maintainableParentID="CONCEPTS"/>
            </structure:ConceptIdentity>
            <structure:LocalRepresentation>
                <structure:Enumeration>
                    <Ref id="COUNTRY_CODES" agencyID="SDMX"/>
                </structure:Enumeration>
            </structure:LocalRepresentation>
        </structure:Dimension>
        
        <structure:Attribute id="UNIT_MEASURE" assignmentStatus="Mandatory">
            <structure:ConceptIdentity>
                <Ref id="UNIT_MEASURE"/>
            </structure:ConceptIdentity>
            <structure:AttributeRelationship>
                <structure:PrimaryMeasure/>
            </structure:AttributeRelationship>
        </structure:Attribute>
        
        <structure:PrimaryMeasure id="OBS_VALUE">
            <structure:ConceptIdentity>
                <Ref id="OBS_VALUE"/>
            </structure:ConceptIdentity>
            <structure:LocalRepresentation>
                <structure:TextFormat textType="Double" decimals="2"/>
            </structure:LocalRepresentation>
        </structure:PrimaryMeasure>
    </structure:DataStructure>
    """
    
    doc = parsexml(sample_xml)
    root_node = root(doc)
    
    # Find sample nodes
    dim_node = findfirst(".//structure:Dimension", root_node)
    attr_node = findfirst(".//structure:Attribute", root_node)
    measure_node = findfirst(".//structure:PrimaryMeasure", root_node)
    
    if dim_node !== nothing && attr_node !== nothing && measure_node !== nothing
        verbose && println("\\n📊 Parsing Different Element Types:")
        verbose && println("-" ^ 40)
        
        # Demonstrate dimension parsing
        verbose && println("\\n🎯 Dimension Element:")
        dim_data = extract_sdmx_element(DimensionElement, dim_node)
        verbose && println("  ID: " * string(dim_data.dimension_id))
        verbose && println("  Position: " * string(dim_data.position))
        verbose && println("  Concept: " * string(dim_data.concept_id))
        verbose && println("  Codelist: " * string(dim_data.codelist_id))
        
        # Demonstrate attribute parsing
        verbose && println("\\n🏷️  Attribute Element:")
        attr_data = extract_sdmx_element(AttributeElement, attr_node)
        verbose && println("  ID: " * string(attr_data.attribute_id))
        verbose && println("  Assignment: " * string(attr_data.assignment_status))
        verbose && println("  Attachment: " * string(attr_data.attachment_level))
        
        # Demonstrate measure parsing
        verbose && println("\\n📈 Measure Element:")
        measure_data = extract_sdmx_element(MeasureElement, measure_node)
        verbose && println("  ID: " * string(measure_data.measure_id))
        verbose && println("  Data Type: " * string(measure_data.data_type))
        verbose && println("  Decimals: " * string(measure_data.decimals))
        
        verbose && println("\\n⚡ Performance Benefits:")
        verbose && println("-" ^ 25)
        verbose && println("✅ Compile-time XPath optimization")
        verbose && println("✅ Type-specialized extraction paths")
        verbose && println("✅ Reduced memory allocations")
        verbose && println("✅ Better compiler optimization")
        
        verbose && println("\\n🎉 Generated function parsing demonstrated successfully!")
    else
        verbose && println("❌ Could not find sample nodes in XML")
    end
end

# Benchmark functions removed per user request - SDMX processing doesn't need to be ultra-fast

"""
    migration_guide(; verbose::Bool=true) -> Nothing

Provide a comprehensive guide for migrating to generated function parsing.

This function explains how to update existing code to use the new @generated
function system while maintaining compatibility and gaining performance benefits.

# Arguments
- `verbose::Bool=true`: Whether to print output. Set to false for quiet operation during tests.

# Examples
```julia
# Display migration instructions
migration_guide()

# Run quietly (for testing)
migration_guide(verbose=false)
```

# See also
[`extract_sdmx_element`](@ref), [`demonstrate_generated_parsing`](@ref)
"""
function migration_guide(; verbose::Bool=true)
    verbose && println("🔄 Migration Guide: Upgrading to Generated Function Parsing")
    verbose && println("=" ^ 60)
    
    verbose && println("\\n📝 Step 1: Update Function Calls")
    verbose && println("-" ^ 35)
    verbose && println("Old approach:")
    verbose && println("```julia")
    verbose && println("# Traditional parsing")
    verbose && println("dim_data = extract_dimension_info(dim_node)")
    verbose && println("attr_data = extract_attribute_info(attr_node)")
    verbose && println("```")
    
    verbose && println("\\nNew approach:")
    verbose && println("```julia")
    verbose && println("# Generated function parsing")
    verbose && println("dim_data = extract_sdmx_element(DimensionElement, dim_node)")
    verbose && println("attr_data = extract_sdmx_element(AttributeElement, attr_node)")
    verbose && println("```")
    
    verbose && println("\\n🎯 Step 2: Import Required Types")
    verbose && println("-" ^ 35)
    verbose && println("```julia")
    verbose && println("using SDMX: DimensionElement, AttributeElement, MeasureElement,")
    verbose && println("           extract_sdmx_element")
    verbose && println("```")
    
    verbose && println("\\n⚡ Step 3: Update Batch Processing")
    verbose && println("-" ^ 35)
    verbose && println("```julia")
    verbose && println("# Process multiple elements efficiently")
    verbose && println("dimensions = [extract_sdmx_element(DimensionElement, node) ")
    verbose && println("             for node in dimension_nodes]")
    verbose && println("             ")
    verbose && println("attributes = [extract_sdmx_element(AttributeElement, node)")
    verbose && println("             for node in attribute_nodes]")
    verbose && println("```")
    
    verbose && println("\\n🔧 Step 4: Performance Monitoring")
    verbose && println("-" ^ 35)
    verbose && println("```julia")
    verbose && println("# Benchmark your specific use case")
    verbose && println("# results = benchmark_parsing_performance(DimensionElement, sample_node)")
    verbose && println("# println(\"Speedup: \", results.speedup_factor, \"x\")")
    verbose && println("```")
    
    verbose && println("\\n✅ Benefits After Migration:")
    verbose && println("-" ^ 30)
    verbose && println("• Immediate performance improvements")
    verbose && println("• Better type safety at compile time")
    verbose && println("• Enhanced IDE support")
    verbose && println("• Future-proof API design")
    
    verbose && println("\\n🎉 Migration completed! Enjoy faster SDMX parsing!")
end

# =================== HELPER FUNCTIONS ===================

"""
    create_benchmark_xml() -> String

Create sample XML content for benchmarking generated function performance.
"""
function create_benchmark_xml()
    return(
    """
    <?xml version="1.0" encoding="UTF-8"?>
    <structure:DataStructure xmlns:structure="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
                            xmlns:common="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
        <structure:Dimension id="COUNTRY" position="1">
            <structure:ConceptIdentity>
                <Ref id="COUNTRY" maintainableParentID="CONCEPTS"/>
            </structure:ConceptIdentity>
            <structure:LocalRepresentation>
                <structure:Enumeration>
                    <Ref id="COUNTRY_CODES" agencyID="SDMX"/>
                </structure:Enumeration>
            </structure:LocalRepresentation>
        </structure:Dimension>
        
        <structure:Attribute id="UNIT_MEASURE" assignmentStatus="Mandatory">
            <structure:ConceptIdentity>
                <Ref id="UNIT_MEASURE"/>
            </structure:ConceptIdentity>
            <structure:AttributeRelationship>
                <structure:PrimaryMeasure/>
            </structure:AttributeRelationship>
        </structure:Attribute>
        
        <structure:PrimaryMeasure id="OBS_VALUE">
            <structure:ConceptIdentity>
                <Ref id="OBS_VALUE"/>
            </structure:ConceptIdentity>
            <structure:LocalRepresentation>
                <structure:TextFormat textType="Double" decimals="2"/>
            </structure:LocalRepresentation>
        </structure:PrimaryMeasure>
    </structure:DataStructure>
    """
    )
end
