using Test
using SDMXer
using DataFrames

@testset "Codelist Extraction" begin

    @testset "SPC Codelists (DF_BP50)" begin
        spc_file = fixture_path("spc_df_bp50.xml")
        df = extract_all_codelists(spc_file)

        @test nrow(df) > 0
        @test "codelist_id" in names(df)
        @test "code_id" in names(df)
        @test "name" in names(df)

        # Replicate original test logic
        indicator_mask = [!ismissing(id) && occursin("INDICATOR", id) for id in df.codelist_id]
        indicator_codelists = unique(df[indicator_mask, :codelist_id])
        @test !isempty(indicator_codelists)
    end

    @testset "ABS Codelists (MIN_EXP)" begin
        abs_file = fixture_path("abs_df_min_exp.xml")
        df = extract_all_codelists(abs_file)
        @test nrow(df) > 0
        @test "CL_MINEX_MINERAL" in df.codelist_id
        # Test hierarchical codelist parsing
        @test "parent_code_id" in names(df)
        parent_codes = df[(!ismissing).(df.parent_code_id), :]
        @test nrow(parent_codes) > 0
        @test parent_codes[1, :parent_code_id] == "TOT"
    end

end

@testset "Codelist Mapping" begin
    @testset "map_codelist_to_dimension" begin
        @test map_codelist_to_dimension("CL_GEO_PICT") == "GEO_PICT"
        @test map_codelist_to_dimension("CL_INDICATOR") == "INDICATOR"
        @test map_codelist_to_dimension("CL_FREQ") == "FREQ"
        @test map_codelist_to_dimension("FREQ") == "FREQ"
        @test map_codelist_to_dimension("GEO_PICT") == "GEO_PICT"
        @test isnothing(map_codelist_to_dimension(missing))
    end
end

@testset "URL Construction" begin
    @testset "construct_availability_url" begin
        # Test with agency
        url1 = "https://data.api.abs.gov.au/rest/dataflow/ABS/MIN_EXP/1.0.0?references=all"
        @test construct_availability_url(url1) == "https://data.api.abs.gov.au/rest/availableconstraint/MIN_EXP/"

        # Test without agency
        url2 = "https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC/DF_BP50/1.0?references=all"
        @test construct_availability_url(url2) == "https://stats-sdmx-disseminate.pacificdata.org/rest/availableconstraint/DF_BP50/"

        # Test with different host
        url3 = "http://api.stats.govt.nz/rest/dataflow/STATSNZ/CEN23_ECI_041/1.0"
        @test construct_availability_url(url3) == "http://api.stats.govt.nz/rest/availableconstraint/CEN23_ECI_041/"
    end
end

@testset "Low-level XML Parsing" begin
    # Create a mock XML document for testing
    xml_string = """
    <structure:Structure xmlns:structure="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure" xmlns:common="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
        <structure:Codelists>
            <structure:Codelist id="CL_TEST">
                <structure:Code id="A">
                    <common:Name xml:lang="en">Code A</common:Name>
                </structure:Code>
                <structure:Code id="B">
                    <common:Name xml:lang="en">Code B</common:Name>
                    <structure:Parent>
                        <Ref id="A"/>
                    </structure:Parent>
                </structure:Code>
            </structure:Codelist>
        </structure:Codelists>
    </structure:Structure>
    """
    doc = SDMXer.EzXML.parsexml(xml_string)
    root_node = SDMXer.EzXML.root(doc)

    @testset "get_parent_id" begin
        code_a_node = findfirst("//structure:Code[@id='A']", root_node)
        code_b_node = findfirst("//structure:Code[@id='B']", root_node)
        @test ismissing(SDMXer.get_parent_id(code_a_node))
        @test SDMXer.get_parent_id(code_b_node) == "A"
    end

    @testset "process_code_node" begin
        code_a_node = findfirst("//structure:Code[@id='A']", root_node)
        processed_code = SDMXer.process_code_node(code_a_node)
        @test length(processed_code) == 1
        @test processed_code[1].code_id == "A"
        @test processed_code[1].name == "Code A"
        @test ismissing(processed_code[1].parent_code_id)
    end

    @testset "extract_codes_from_codelist_node" begin
        codelist_node = findfirst("//structure:Codelist", root_node)
        codes = SDMXer.extract_codes_from_codelist_node(codelist_node)
        @test length(codes) == 2
        @test codes[1].codelist_id == "CL_TEST"
        @test codes[2].parent_code_id == "A"
    end

    @testset "extract_all_codelists from EzXML.Document" begin
        df = extract_all_codelists(doc)
        @test nrow(df) == 2
        @test "CL_TEST" in df.codelist_id
    end
end

@testset "Availability Filtering" begin
    @testset "extract_all_codelists with filtering" begin
        # This test makes a live HTTP request
        abs_url = "https://data.api.abs.gov.au/rest/dataflow/ABS/MIN_EXP/1.0.0?references=all"

        # Test with boolean flag
        filtered_df = extract_all_codelists(abs_url, true)
        unfiltered_df = extract_all_codelists(abs_url, false)

        @test nrow(filtered_df) > 0
        @test nrow(filtered_df) < nrow(unfiltered_df)

        # Test with availability URL
        availability_url = construct_availability_url(abs_url)
        filtered_df_2 = extract_all_codelists(abs_url, availability_url)
        @test nrow(filtered_df_2) == nrow(filtered_df)
    end

    @testset "get_available_codelist_summary" begin
        # This test makes a live HTTP request
        abs_url = "https://data.api.abs.gov.au/rest/dataflow/ABS/MIN_EXP/1.0.0?references=all"
        summary = get_available_codelist_summary(abs_url)
        @test summary["dataflow_id"] == "MIN_EXP"
        @test summary["total_observations"] > 0
        @test haskey(summary["dimensions_with_data"], "MINERAL_TYPE")
    end
end
