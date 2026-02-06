using Test
using SDMXer
using EzXML
using DataFrames

@testset "SDMXGeneratedIntegration" begin
    
    @testset "demonstrate_generated_parsing" begin
        # Test that the demonstration function runs without errors (silently)
        @test_nowarn SDMXer.demonstrate_generated_parsing(verbose=false)
        
        # Just verify it doesn't throw an error
        # Full documentation is available in docs/GENERATED_PARSING.md
    end
    
    @testset "migration_guide" begin
        # Test that the migration guide runs without errors (silently)
        @test_nowarn SDMXer.migration_guide(verbose=false)
        
        # Just verify it doesn't throw an error
        # Full documentation is available in docs/GENERATED_PARSING.md
    end
    
    @testset "create_benchmark_xml" begin
        # Test the helper function that creates benchmark XML
        xml_content = SDMXer.create_benchmark_xml()
        @test !isempty(xml_content)
        @test occursin("<?xml version", xml_content)
        @test occursin("structure:DataStructure", xml_content)
        @test occursin("structure:Dimension", xml_content)
        @test occursin("structure:Attribute", xml_content)
        @test occursin("structure:PrimaryMeasure", xml_content)
        
        # Verify the XML is valid
        @test_nowarn parsexml(xml_content)
        
        # Parse and check structure
        doc = parsexml(xml_content)
        root_node = root(doc)
        
        # Verify it contains expected elements
        dim_nodes = findall(".//structure:Dimension", root_node)
        @test length(dim_nodes) == 1
        @test dim_nodes[1]["id"] == "COUNTRY"
        
        attr_nodes = findall(".//structure:Attribute", root_node)
        @test length(attr_nodes) == 1
        @test attr_nodes[1]["id"] == "UNIT_MEASURE"
        
        # Find PrimaryMeasure with id attribute (not references in AttributeRelationship)
        measure_nodes = findall(".//structure:PrimaryMeasure[@id]", root_node)
        @test length(measure_nodes) == 1
        @test measure_nodes[1]["id"] == "OBS_VALUE"
    end
    
    @testset "Integration with real fixture data" begin
        # Load real fixture for integration testing
        spc_file = fixture_path("spc_df_bp50.xml")
        @test isfile(spc_file)
        doc = readxml(spc_file)
        root_node = root(doc)
        
        # Test extracting multiple element types from the same document
        dimensions = findall("//structure:DataStructure[@id='DSD_BP50']//structure:Dimension", root_node)
        attributes = findall("//structure:Attribute", root_node)
        measures = findall("//structure:DataStructure[@id='DSD_BP50']//structure:PrimaryMeasure[@id]", root_node)
        
        @test length(dimensions) > 0
        @test length(attributes) > 0
        @test length(measures) > 0
        
        # Extract using generated functions
        dim_results = [SDMXer.extract_sdmx_element(SDMXer.DimensionElement, d) for d in dimensions[1:min(3, length(dimensions))]]
        attr_results = [SDMXer.extract_sdmx_element(SDMXer.AttributeElement, a) for a in attributes[1:min(3, length(attributes))]]
        measure_results = [SDMXer.extract_sdmx_element(SDMXer.MeasureElement, m) for m in measures]
        
        # Verify results
        @test all(r -> !ismissing(r.dimension_id), dim_results)
        @test all(r -> !ismissing(r.attribute_id), attr_results)
        @test all(r -> !ismissing(r.measure_id), measure_results)
        
        # Test that positions are in order for dimensions
        positions = [r.position for r in dim_results]
        @test issorted(positions)
    end
    
    @testset "Generated function workflow example" begin
        # Simulate a typical workflow using generated functions
        spc_file = fixture_path("spc_df_bp50.xml")
        doc = readxml(spc_file)
        root_node = root(doc)
        
        # Step 1: Extract codelists
        codelists = findall("//structure:Codelist", root_node)
        @test length(codelists) > 0
        
        # Extract first few codelists
        codelist_data = []
        for cl in codelists[1:min(3, length(codelists))]
            result = SDMXer.extract_sdmx_element(SDMXer.CodelistElement, cl)
            push!(codelist_data, result)
        end
        
        @test length(codelist_data) > 0
        @test all(cl -> !ismissing(cl.codelist_id), codelist_data)
        
        # Step 2: Extract concepts
        concepts = findall("//structure:Concept", root_node)
        @test length(concepts) > 0
        
        concept_data = []
        for c in concepts[1:min(5, length(concepts))]
            result = SDMXer.extract_sdmx_element(SDMXer.ConceptElement, c)
            push!(concept_data, result)
        end
        
        @test length(concept_data) > 0
        @test all(c -> !ismissing(c.concept_id), concept_data)
        
        # Step 3: Extract data structure components
        dsd_node = findfirst("//structure:DataStructure[@id='DSD_BP50']", root_node)
        @test dsd_node !== nothing
        
        # Extract all dimensions
        dim_nodes = findall(".//structure:Dimension", dsd_node)
        time_dim_nodes = findall(".//structure:TimeDimension", dsd_node)
        all_dims = vcat(dim_nodes, time_dim_nodes)
        
        dim_data = [SDMXer.extract_sdmx_element(SDMXer.DimensionElement, d) for d in all_dims]
        @test length(dim_data) > 0
        @test all(d -> !ismissing(d.dimension_id), dim_data)
        
        # Verify we have both regular and time dimensions
        time_dims = filter(d -> d.is_time_dimension, dim_data)
        regular_dims = filter(d -> !d.is_time_dimension, dim_data)
        @test length(time_dims) > 0
        @test length(regular_dims) > 0
    end
    
    @testset "XPath pattern usage" begin
        # Test that XPath patterns work correctly with real data
        spc_file = fixture_path("spc_df_bp50.xml")
        doc = readxml(spc_file)
        root_node = root(doc)
        
        # Get XPath patterns for dimensions
        dim_patterns = SDMXer.get_xpath_patterns(SDMXer.DimensionElement)
        
        # Find a dimension and use the patterns
        dim_node = findfirst("//structure:Dimension[@id='FREQ']", root_node)
        @test dim_node !== nothing
        
        # Use the XPath patterns to find sub-elements
        concept_ref = findfirst(dim_patterns.concept_ref, dim_node)
        @test concept_ref !== nothing
        @test concept_ref["id"] == "FREQ"
        
        codelist_ref = findfirst(dim_patterns.codelist_ref, dim_node)
        @test codelist_ref !== nothing
        @test codelist_ref["id"] == "CL_COM_FREQ"
        
        # Test attribute XPath patterns
        attr_patterns = SDMXer.get_xpath_patterns(SDMXer.AttributeElement)
        attr_node = findfirst("//structure:Attribute[@id='UNIT_MEASURE']", root_node)
        @test attr_node !== nothing
        
        attachment = findfirst(attr_patterns.attachment, attr_node)
        @test attachment !== nothing
    end
    
    @testset "Mixed element extraction" begin
        # Test extracting different element types in a single workflow
        spc_file = fixture_path("spc_df_bp50.xml")
        doc = readxml(spc_file)
        root_node = root(doc)
        
        # Create a DataFrame with mixed element metadata
        results = DataFrame(
            element_type = String[],
            element_id = String[],
            extra_info = Any[]
        )
        
        # Extract dimensions
        for dim in findall("//structure:DataStructure[@id='DSD_BP50']//structure:Dimension", root_node)[1:2]
            data = SDMXer.extract_sdmx_element(SDMXer.DimensionElement, dim)
            push!(results, ("Dimension", data.dimension_id, data.position))
        end
        
        # Extract time dimension
        time_dim = findfirst("//structure:TimeDimension", root_node)
        if time_dim !== nothing
            data = SDMXer.extract_sdmx_element(SDMXer.DimensionElement, time_dim)
            push!(results, ("TimeDimension", data.dimension_id, data.position))
        end
        
        # Extract attributes
        for attr in findall("//structure:Attribute", root_node)[1:2]
            data = SDMXer.extract_sdmx_element(SDMXer.AttributeElement, attr)
            push!(results, ("Attribute", data.attribute_id, data.assignment_status))
        end
        
        # Extract measure (only those with id attribute)
        measure = findfirst("//structure:PrimaryMeasure[@id]", root_node)
        if measure !== nothing
            data = SDMXer.extract_sdmx_element(SDMXer.MeasureElement, measure)
            push!(results, ("Measure", data.measure_id, data.data_type))
        end
        
        @test nrow(results) > 0
        @test "Dimension" in results.element_type
        @test "Attribute" in results.element_type
        @test "Measure" in results.element_type
    end
    
    @testset "Error handling and edge cases" begin
        # Test with empty or minimal nodes
        minimal_xml = """
        <structure:Dimension id="TEST" position="1"
                           xmlns:structure="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
        </structure:Dimension>
        """
        doc = parsexml(minimal_xml)
        node = root(doc)
        
        result = SDMXer.extract_sdmx_element(SDMXer.DimensionElement, node)
        @test result.dimension_id == "TEST"
        @test result.position == 1
        @test ismissing(result.concept_id)
        @test ismissing(result.codelist_id)
        
        # Test generic fallback for unsupported types
        category_xml = """
        <structure:Category id="CAT1"
                          xmlns:structure="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
        </structure:Category>
        """
        doc = parsexml(category_xml)
        node = root(doc)
        
        result = SDMXer.extract_generic_element(node)
        @test result.element_id == "CAT1"
        @test result.element_name == "Category"
    end
end