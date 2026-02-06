using Test
using SDMXer
using EzXML
using DataFrames

@testset "SDMXGeneratedParsing" begin
    
    # Load the real fixture file
    spc_file = fixture_path("spc_df_bp50.xml")
    @test isfile(spc_file)
    doc = readxml(spc_file)
    root_node = root(doc)
    
    @testset "extract_code_info helper" begin
        # Find a real code node from the fixture
        codelist_node = findfirst("//structure:Codelist[@id='CL_COM_SEX']", root_node)
        @test codelist_node !== nothing
        
        code_node = findfirst(".//structure:Code[@id='M']", codelist_node)
        @test code_node !== nothing
        
        result = SDMXer.extract_code_info(code_node)
        @test result.code_id == "M"
        @test result.name == "Male"
        @test ismissing(result.parent_code_id)
        
        # Test code with parent
        code_with_parent = findfirst(".//structure:Code[@parentCode]", root_node)
        if code_with_parent !== nothing
            result_parent = SDMXer.extract_code_info(code_with_parent)
            @test !ismissing(result_parent.parent_code_id)
        end
    end
    
    @testset "extract_sdmx_element - DimensionElement" begin
        # Find a real dimension from the DataStructure
        dim_node = findfirst("//structure:DataStructure[@id='DSD_BP50']//structure:Dimension[@id='FREQ']", root_node)
        @test dim_node !== nothing
        
        result = SDMXer.extract_sdmx_element(SDMXer.DimensionElement, dim_node)
        @test result.dimension_id == "FREQ"
        @test result.position == 1
        @test result.concept_id == "FREQ"
        @test result.concept_scheme == "CS_COMMON"
        @test result.codelist_id == "CL_COM_FREQ"
        @test result.codelist_agency == "SPC"
        @test result.is_time_dimension == false
        
        # Test another dimension with different characteristics
        geo_dim = findfirst("//structure:DataStructure[@id='DSD_BP50']//structure:Dimension[@id='GEO_PICT']", root_node)
        @test geo_dim !== nothing
        
        result_geo = SDMXer.extract_sdmx_element(SDMXer.DimensionElement, geo_dim)
        @test result_geo.dimension_id == "GEO_PICT"
        @test result_geo.position == 3
        @test result_geo.codelist_id == "CL_COM_GEO_PICT"
        @test result_geo.data_type == "String"  # Has EnumerationFormat
    end
    
    @testset "extract_sdmx_element - TimeDimension" begin
        # Find the TimeDimension from the fixture
        time_node = findfirst("//structure:TimeDimension[@id='TIME_PERIOD']", root_node)
        @test time_node !== nothing
        
        result = SDMXer.extract_sdmx_element(SDMXer.DimensionElement, time_node)
        @test result.dimension_id == "TIME_PERIOD"
        @test result.position == 12
        @test result.concept_id == "TIME_PERIOD"
        @test result.data_type == "ObservationalTimePeriod"
        @test result.is_time_dimension == true
        @test ismissing(result.codelist_id)  # TimeDimension typically doesn't have codelist
    end
    
    @testset "extract_sdmx_element - AttributeElement" begin
        # Find a real attribute with PrimaryMeasure attachment
        attr_node = findfirst("//structure:Attribute[@id='UNIT_MEASURE']", root_node)
        @test attr_node !== nothing
        
        result = SDMXer.extract_sdmx_element(SDMXer.AttributeElement, attr_node)
        @test result.attribute_id == "UNIT_MEASURE"
        @test result.assignment_status == "Conditional"
        @test result.concept_id == "UNIT_MEASURE"
        @test result.codelist_id == "CL_COM_UNIT_MEASURE"
        @test result.attachment_level == "PrimaryMeasure"
        
        # Test conditional attribute
        cond_attr = findfirst("//structure:Attribute[@id='OBS_STATUS']", root_node)
        @test cond_attr !== nothing
        
        result_cond = SDMXer.extract_sdmx_element(SDMXer.AttributeElement, cond_attr)
        @test result_cond.attribute_id == "OBS_STATUS"
        @test result_cond.assignment_status == "Conditional"
        @test result_cond.attachment_level == "PrimaryMeasure"
    end
    
    @testset "extract_sdmx_element - MeasureElement" begin
        # Find the PrimaryMeasure from the fixture
        measure_node = findfirst("//structure:PrimaryMeasure[@id='OBS_VALUE']", root_node)
        @test measure_node !== nothing
        
        result = SDMXer.extract_sdmx_element(SDMXer.MeasureElement, measure_node)
        @test result.measure_id == "OBS_VALUE"
        @test result.concept_id == "OBS_VALUE"
        @test result.data_type == "Double"
        @test ismissing(result.decimals)  # Not specified in this fixture
    end
    
    @testset "extract_sdmx_element - ConceptElement" begin
        # Find a real concept from the ConceptScheme
        concept_node = findfirst("//structure:Concept[@id='FREQ']", root_node)
        @test concept_node !== nothing
        
        result = SDMXer.extract_sdmx_element(SDMXer.ConceptElement, concept_node)
        @test result.concept_id == "FREQ"
        @test result.name == "Frequency"
        # Description might be missing for some concepts
        
        # Test concept with both name and description
        time_concept = findfirst("//structure:Concept[@id='TIME_PERIOD']", root_node)
        if time_concept !== nothing
            result_time = SDMXer.extract_sdmx_element(SDMXer.ConceptElement, time_concept)
            @test result_time.concept_id == "TIME_PERIOD"
            @test !ismissing(result_time.name)
        end
    end
    
    @testset "extract_sdmx_element - CodelistElement" begin
        # Find a real codelist from the fixture
        codelist_node = findfirst("//structure:Codelist[@id='CL_COM_SEX']", root_node)
        @test codelist_node !== nothing
        
        result = SDMXer.extract_sdmx_element(SDMXer.CodelistElement, codelist_node)
        @test result.codelist_id == "CL_COM_SEX"
        @test result.agency_id == "SPC"
        @test result.version == "1.0"
        @test result.name == "Common codelist for sex"
        @test length(result.codes) > 0
        
        # Check that codes were properly extracted
        code_ids = [code.code_id for code in result.codes]
        @test "M" in code_ids
        @test "F" in code_ids
        
        # Test a larger codelist
        geo_codelist = findfirst("//structure:Codelist[@id='CL_COM_GEO_PICT']", root_node)
        @test geo_codelist !== nothing
        
        result_geo = SDMXer.extract_sdmx_element(SDMXer.CodelistElement, geo_codelist)
        @test result_geo.codelist_id == "CL_COM_GEO_PICT"
        @test length(result_geo.codes) > 10  # Pacific countries codelist has many codes
    end
    
    @testset "get_xpath_patterns" begin
        # Test that XPath patterns are correctly generated
        dim_patterns = SDMXer.get_xpath_patterns(SDMXer.DimensionElement)
        @test dim_patterns.concept_ref == ".//structure:ConceptIdentity/Ref"
        @test dim_patterns.codelist_ref == ".//structure:LocalRepresentation/structure:Enumeration/Ref"
        @test dim_patterns.text_format == ".//structure:LocalRepresentation/structure:TextFormat"
        
        attr_patterns = SDMXer.get_xpath_patterns(SDMXer.AttributeElement)
        @test attr_patterns.concept_ref == ".//structure:ConceptIdentity/Ref"
        @test attr_patterns.codelist_ref == ".//structure:LocalRepresentation/structure:Enumeration/Ref"
        @test attr_patterns.attachment == ".//structure:AttributeRelationship"
        @test attr_patterns.group_ref == ".//structure:AttributeRelationship/structure:Group"
        @test attr_patterns.dimension_ref == ".//structure:AttributeRelationship/structure:Dimension"
        
        measure_patterns = SDMXer.get_xpath_patterns(SDMXer.MeasureElement)
        @test measure_patterns.concept_ref == ".//structure:ConceptIdentity/Ref"
        @test measure_patterns.text_format == ".//structure:LocalRepresentation/structure:TextFormat"
    end
    
    @testset "extract_generic_element fallback" begin
        # Use a Category node which doesn't have a specialized extractor
        category_node = findfirst("//structure:Category[@id='SDG']", root_node)
        @test category_node !== nothing
        
        result = SDMXer.extract_generic_element(category_node)
        @test result.element_id == "SDG"
        @test result.element_name == "Category"
        @test result.raw_node === category_node
    end
    
    @testset "Edge cases with missing values" begin
        # Test dimension without TextFormat (only has Enumeration)
        dim_no_format = findfirst("//structure:Dimension[@id='INDICATOR']", root_node)
        @test dim_no_format !== nothing
        
        result = SDMXer.extract_sdmx_element(SDMXer.DimensionElement, dim_no_format)
        @test result.dimension_id == "INDICATOR"
        @test !ismissing(result.codelist_id)
        @test ismissing(result.data_type)  # No TextFormat element
        
        # Test attribute with TextFormat instead of Enumeration
        text_attr = findfirst("//structure:Attribute[@id='DATA_SOURCE']", root_node)
        @test text_attr !== nothing
        
        result_text = SDMXer.extract_sdmx_element(SDMXer.AttributeElement, text_attr)
        @test result_text.attribute_id == "DATA_SOURCE"
        @test ismissing(result_text.codelist_id)  # Has TextFormat, not Enumeration
        
        # Test concept without description
        concept_no_desc = findfirst("//structure:Concept[not(.//common:Description)]", root_node)
        if concept_no_desc !== nothing
            result_no_desc = SDMXer.extract_sdmx_element(SDMXer.ConceptElement, concept_no_desc)
            @test !ismissing(result_no_desc.concept_id)
            @test ismissing(result_no_desc.description)
        end
    end
    
    @testset "Performance characteristics" begin
        # Test that multiple calls to the same type use the same compiled code
        dim_nodes = findall("//structure:Dimension", root_node)
        @test length(dim_nodes) > 5
        
        # Extract multiple dimensions - should use compiled specialization
        results = [SDMXer.extract_sdmx_element(SDMXer.DimensionElement, node) for node in dim_nodes[1:min(5, length(dim_nodes))]]
        @test all(r -> !ismissing(r.dimension_id), results)
        @test all(r -> !ismissing(r.position), results)
        
        # Test with different element types
        attr_nodes = findall("//structure:Attribute", root_node)
        @test length(attr_nodes) > 3
        
        attr_results = [SDMXer.extract_sdmx_element(SDMXer.AttributeElement, node) for node in attr_nodes[1:min(3, length(attr_nodes))]]
        @test all(r -> !ismissing(r.attribute_id), attr_results)
    end
end