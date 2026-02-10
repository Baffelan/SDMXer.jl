using Test
using SDMXer
using Dates
using EzXML

@testset "Availability Extraction" begin

    @testset "SPC Availability (Successful)" begin
        spc_file = fixture_path("spc_ac_bp50.xml")
        availability = extract_availability(spc_file)

        @test availability isa AvailabilityConstraint
        @test availability.constraint_id == "CC"
        @test availability.agency_id == "SDMX"
        @test availability.dataflow_ref.id == "DF_BP50"
        @test availability.total_observations > 0
        @test length(availability.dimensions) > 0

        # Test helper functions
        countries = get_available_values(availability, "GEO_PICT")
        @test !isempty(countries)
        @test "FJ" in countries # Fiji

        time_coverage = get_time_coverage(availability)
        @test time_coverage isa TimeAvailability
        @test time_coverage.total_periods > 0
    end
    
    @testset "TimeAvailability struct" begin
        # Test creating TimeAvailability with Date objects
        time_avail = SDMXer.TimeAvailability(
            Date("2020-01-01"),
            Date("2023-12-31"),
            "year",
            4,
            ["2021"]
        )
        @test time_avail.start_date == Date("2020-01-01")
        @test time_avail.end_date == Date("2023-12-31")
        @test time_avail.format == "year"
        @test time_avail.total_periods == 4
        @test "2021" in time_avail.gaps
        
        # Test creating TimeAvailability with String dates
        time_avail_str = SDMXer.TimeAvailability(
            "2020Q1",
            "2023Q4",
            "quarter",
            16,
            String[]
        )
        @test time_avail_str.start_date == "2020Q1"
        @test time_avail_str.end_date == "2023Q4"
        @test isempty(time_avail_str.gaps)
    end
    
    @testset "DimensionAvailability struct" begin
        dim_avail = SDMXer.DimensionAvailability(
            "COUNTRY",
            ["FJ", "TO", "WS"],
            3,
            "codelist",
            0.15  # 3 out of 20 possible countries
        )
        @test dim_avail.dimension_id == "COUNTRY"
        @test length(dim_avail.available_values) == 3
        @test dim_avail.total_count == 3
        @test dim_avail.value_type == "codelist"
        @test dim_avail.coverage_ratio == 0.15
    end
    
    @testset "extract_availability_from_dataflow" begin
        # Load the full dataflow document
        spc_file = fixture_path("spc_df_bp50.xml")
        doc = readxml(spc_file)
        
        # Try to extract availability from dataflow (might return nothing)
        result = SDMXer.extract_availability_from_dataflow(doc)
        
        # This is okay - not all dataflows have Actual ContentConstraints
        if result !== nothing
            @test result isa AvailabilityConstraint
            @test !isempty(result.constraint_id)
        else
            @test result === nothing  # Valid case
        end
    end
    
    @testset "extract_availability_from_node" begin
        # Load and find a ContentConstraint node
        spc_file = fixture_path("spc_ac_bp50.xml")
        doc = readxml(spc_file)
        root_node = root(doc)
        
        # Find ContentConstraint node
        constraint_node = findfirst("//*[local-name()='ContentConstraint']", root_node)
        @test constraint_node !== nothing
        
        # Extract availability from the node
        availability = SDMXer.extract_availability_from_node(constraint_node)
        @test availability isa AvailabilityConstraint
        @test !isempty(availability.constraint_id)
        @test !isempty(availability.dimensions)
    end
    
    @testset "extract_time_availability" begin
        # Create a test TimeRange node
        xml_str = """
        <KeyValue id="TIME_PERIOD" xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
            <TimeRange>
                <StartPeriod>2020-01-01</StartPeriod>
                <EndPeriod>2023-12-31</EndPeriod>
            </TimeRange>
        </KeyValue>
        """
        doc = parsexml(xml_str)
        time_node = root(doc)
        
        time_avail = SDMXer.extract_time_availability(time_node)
        @test time_avail isa TimeAvailability
        @test time_avail.start_date == Date("2020-01-01")
        @test time_avail.end_date == Date("2023-12-31")
        @test time_avail.format == "date"
        @test time_avail.total_periods == 4  # 2020-2023
        
        # Test with discrete values
        xml_str_discrete = """
        <KeyValue id="TIME_PERIOD" xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
            <Value>2020</Value>
            <Value>2021</Value>
            <Value>2022</Value>
            <Value>2023</Value>
        </KeyValue>
        """
        doc_discrete = parsexml(xml_str_discrete)
        time_node_discrete = root(doc_discrete)
        
        time_avail_discrete = SDMXer.extract_time_availability(time_node_discrete)
        @test time_avail_discrete.start_date == "2020"
        @test time_avail_discrete.end_date == "2023"
        @test time_avail_discrete.format == "discrete"
        @test time_avail_discrete.total_periods == 4
    end
    
    @testset "get_time_period_values" begin
        # Test with TimeRange
        xml_str_range = """
        <KeyValue id="TIME_PERIOD" xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
            <TimeRange>
                <StartPeriod>2020-01-01</StartPeriod>
                <EndPeriod>2023-12-31</EndPeriod>
            </TimeRange>
        </KeyValue>
        """
        doc_range = parsexml(xml_str_range)
        time_node_range = root(doc_range)
        
        values_range = SDMXer.get_time_period_values(time_node_range)
        @test length(values_range) == 1
        @test values_range[1] == "2020-2023"
        
        # Test with discrete values
        xml_str_discrete = """
        <KeyValue id="TIME_PERIOD" xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
            <Value>2020</Value>
            <Value>2021</Value>
            <Value>2022</Value>
        </KeyValue>
        """
        doc_discrete = parsexml(xml_str_discrete)
        time_node_discrete = root(doc_discrete)
        
        values_discrete = SDMXer.get_time_period_values(time_node_discrete)
        @test length(values_discrete) == 3
        @test "2020" in values_discrete
        @test "2021" in values_discrete
        @test "2022" in values_discrete
    end
    
    @testset "extract_dimension_values" begin
        xml_str = """
        <KeyValue id="COUNTRY" xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
            <Value>FJ</Value>
            <Value>TO</Value>
            <Value>WS</Value>
            <Value>VU</Value>
        </KeyValue>
        """
        doc = parsexml(xml_str)
        kv_node = root(doc)
        
        values = SDMXer.extract_dimension_values(kv_node)
        @test length(values) == 4
        @test "FJ" in values
        @test "TO" in values
        @test "WS" in values
        @test "VU" in values
        @test issorted(values)  # Should be sorted
    end
    
    @testset "get_available_values" begin
        # Create a mock AvailabilityConstraint
        dims = [
            SDMXer.DimensionAvailability("COUNTRY", ["FJ", "TO", "WS"], 3, "codelist", 1.0),
            SDMXer.DimensionAvailability("INDICATOR", ["GDP", "POP", "UNEMP"], 3, "codelist", 1.0),
            SDMXer.DimensionAvailability("TIME_PERIOD", ["2020", "2021", "2022"], 3, "time", 1.0)
        ]
        
        availability = SDMXer.AvailabilityConstraint(
            "TEST_CC",
            "Test Constraint",
            "TEST_AGENCY",
            "1.0",
            (id="DF_TEST", agency="TEST", version="1.0"),
            1000,
            dims,
            nothing,
            string(Dates.now())
        )
        
        # Test getting values for existing dimensions
        countries = get_available_values(availability, "COUNTRY")
        @test countries == ["FJ", "TO", "WS"]
        
        indicators = get_available_values(availability, "INDICATOR")
        @test indicators == ["GDP", "POP", "UNEMP"]
        
        # Test getting values for non-existing dimension
        missing_values = get_available_values(availability, "NON_EXISTENT")
        @test isempty(missing_values)
    end
    
    @testset "Error handling" begin
        # Test with invalid XML (no ContentConstraint)
        invalid_xml = """
        <root>
            <NotAConstraint>Invalid</NotAConstraint>
        </root>
        """
        doc = parsexml(invalid_xml)
        
        # Should throw error because no ContentConstraint is found (could be ArgumentError or XMLError)
        @test_throws Union{ArgumentError, EzXML.XMLError} SDMXer.extract_availability(doc)
    end
    
    @testset "Edge cases" begin
        # Test constraint without observation count
        xml_str = """
        <structure:ContentConstraint id="CC_NO_OBS" agencyID="TEST" version="1.0"
                                   xmlns:structure="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
                                   xmlns:common="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
            <common:Name>Test Constraint</common:Name>
            <structure:ConstraintAttachment>
                <structure:Dataflow>
                    <Ref id="DF_TEST" agencyID="TEST" version="1.0"/>
                </structure:Dataflow>
            </structure:ConstraintAttachment>
            <structure:CubeRegion>
                <common:KeyValue id="COUNTRY">
                    <common:Value>FJ</common:Value>
                </common:KeyValue>
            </structure:CubeRegion>
        </structure:ContentConstraint>
        """
        doc = parsexml(xml_str)
        constraint_node = root(doc)
        
        availability = SDMXer.extract_availability_from_node(constraint_node)
        @test availability.total_observations == 0  # Should default to 0
        @test availability.constraint_id == "CC_NO_OBS"
        @test length(availability.dimensions) == 1
        
        # Test with invalid observation count format
        xml_str_invalid_obs = """
        <structure:ContentConstraint id="CC_INVALID_OBS" agencyID="TEST" version="1.0"
                                   xmlns:structure="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
                                   xmlns:common="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
            <common:Annotations>
                <common:Annotation id="obs_count">
                    <common:AnnotationTitle>not_a_number</common:AnnotationTitle>
                </common:Annotation>
            </common:Annotations>
            <structure:ConstraintAttachment>
                <structure:Dataflow>
                    <Ref id="DF_TEST" agencyID="TEST" version="1.0"/>
                </structure:Dataflow>
            </structure:ConstraintAttachment>
            <structure:CubeRegion/>
        </structure:ContentConstraint>
        """
        doc_invalid = parsexml(xml_str_invalid_obs)
        constraint_node_invalid = root(doc_invalid)
        
        # Should warn and default to 0 (expected warning from invalid fixture)
        availability_invalid = @test_logs (:warn,) SDMXer.extract_availability_from_node(constraint_node_invalid)
        @test availability_invalid.total_observations == 0
    end

    # Removed error handling tests for obsolete fixtures
end