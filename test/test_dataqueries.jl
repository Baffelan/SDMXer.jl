using Test
using SDMXer
using DataFrames

@testset "Data Query Construction" begin

    # Load a schema for testing key construction
    spc_schema_file = fixture_path("spc_df_bp50.xml")
    spc_schema = extract_dataflow_schema(spc_schema_file)

    @testset "construct_sdmx_key" begin
        # The get_dimension_order function is not exported, but it's used by construct_sdmx_key.
        # We can infer the order from the schema object.
        dim_order = spc_schema.dimensions.dimension_id

        # Test correct ordering
        filters = Dict("GEO_PICT" => "FJ", "FREQ" => "A", "INDICATOR" => "BP50_01")

        key = SDMXer.construct_sdmx_key(spc_schema, filters)

        key_parts = split(key, '.')
        freq_idx = findfirst(d -> d == "FREQ", dim_order)
        geo_idx = findfirst(d -> d == "GEO_PICT", dim_order)

        @test key_parts[freq_idx] == "A"
        @test key_parts[geo_idx] == "FJ"

        # Test with an invalid dimension
        invalid_filters = Dict("INVALID_DIM" => "value")
        @test_throws ArgumentError SDMXer.construct_sdmx_key(spc_schema, invalid_filters)
        
        # Test with empty filters
        empty_filters = Dict{String,String}()
        key_empty = SDMXer.construct_sdmx_key(spc_schema, empty_filters)
        @test occursin(".", key_empty)  # Should have dots for separators
        @test all(p -> p == "", split(key_empty, '.'))  # All parts should be empty
        
        # Test with partial filters
        partial_filters = Dict("FREQ" => "M")
        key_partial = SDMXer.construct_sdmx_key(spc_schema, partial_filters)
        key_parts_partial = split(key_partial, '.')
        @test key_parts_partial[freq_idx] == "M"
        # Other dimensions should be empty strings
        @test count(p -> p != "", key_parts_partial) == 1
        
        # Test with all dimensions specified (using actual dimension order)
        all_dims = Dict{String,String}()
        actual_dim_order = SDMXer.get_dimension_order(spc_schema)
        for dim in actual_dim_order
            all_dims[dim] = "TEST"
        end
        key_all = SDMXer.construct_sdmx_key(spc_schema, all_dims)
        @test !occursin("..", key_all)  # No double dots
        @test all(p -> p == "TEST", split(key_all, '.'))
    end

    @testset "construct_data_url" begin
        base_url = "https://example.com/rest"
        agency = "TEST"
        dataflow = "DF_TEST"
        version = "1.0"

        # Test with key
        url = construct_data_url(base_url, agency, dataflow, version, key="A.FR.USD")
        @test url == "https://example.com/rest/data/TEST,DF_TEST,1.0/A.FR.USD?dimensionAtObservation=AllDimensions"

        # Test with start/end period
        url = construct_data_url(base_url, agency, dataflow, version, start_period="2022", end_period="2023")
        @test occursin("startPeriod=2022", url)
        @test occursin("endPeriod=2023", url)
        @test occursin("dimensionAtObservation=AllDimensions", url)

        # Test with dimension_filters and schema
        filters = Dict("GEO_PICT" => "TV", "FREQ" => "Q")
        url = construct_data_url(base_url, agency, dataflow, version, schema=spc_schema, dimension_filters=filters)
        @test occursin("/Q.", url) || occursin(".Q", url)  # FREQ should be in the key
        @test occursin("TV", url)  # GEO_PICT value should be in the key

        # Test with dimension_filters but no schema (should warn)
        @test_logs (:warn, "Dimension filters provided without schema - key construction may be incorrect") construct_data_url(base_url, agency, dataflow, version, dimension_filters=filters)
        
        # Test with custom dimension_at_observation
        url_custom = construct_data_url(base_url, agency, dataflow, version, 
                                       dimension_at_observation="TIME_PERIOD")
        @test occursin("dimensionAtObservation=TIME_PERIOD", url_custom)
        
        # Test with all parameters
        url_full = construct_data_url(base_url, agency, dataflow, version,
                                     schema=spc_schema,
                                     dimension_filters=Dict("FREQ" => "A"),
                                     start_period="2020",
                                     end_period="2023",
                                     dimension_at_observation="AllDimensions")
        @test occursin("startPeriod=2020", url_full)
        @test occursin("endPeriod=2023", url_full)
        @test occursin("dimensionAtObservation=AllDimensions", url_full)
        @test occursin("/A.", url_full) || occursin(".A", url_full)
        
        # Test with empty key (get all data)
        url_all = construct_data_url(base_url, agency, dataflow, version)
        @test url_all == "https://example.com/rest/data/TEST,DF_TEST,1.0/?dimensionAtObservation=AllDimensions"
        
        # Test URL encoding (if special characters in parameters)
        url_dates = construct_data_url(base_url, agency, dataflow, version,
                                      start_period="2020-01-01",
                                      end_period="2023-12-31")
        @test occursin("2020-01-01", url_dates)
        @test occursin("2023-12-31", url_dates)
    end

    @testset "clean_sdmx_data" begin
        # Test basic cleaning
        dirty_df = DataFrame(
            TIME_PERIOD = [2022, 2023],
            OBS_VALUE = ["123.45", "67.8"],
            OTHER_COL = ["A", missing]
        )

        cleaned_df = SDMXer.clean_sdmx_data(dirty_df)

        @test eltype(cleaned_df.OBS_VALUE) <: Union{Missing, Float64}
        @test skipmissing(cleaned_df.OBS_VALUE) |> collect == [123.45, 67.8]
        @test cleaned_df.TIME_PERIOD isa Vector{String}
        @test cleaned_df.TIME_PERIOD == ["2022", "2023"]

        # Test with missing values
        dirty_df_missing = DataFrame(
            OBS_VALUE = ["10", missing, "", "20"],
            TIME_PERIOD = [2020, 2021, 2022, 2023]
        )
        cleaned_df_missing = SDMXer.clean_sdmx_data(dirty_df_missing)
        @test count(ismissing, cleaned_df_missing.OBS_VALUE) == 2  # missing and "" become missing
        @test skipmissing(cleaned_df_missing.OBS_VALUE) |> collect == [10.0, 20.0]
        
        # Test with non-numeric OBS_VALUE
        dirty_df_text = DataFrame(
            OBS_VALUE = ["100", "not_a_number", "200.5"],
            TIME_PERIOD = ["2020", "2021", "2022"]
        )
        cleaned_df_text = SDMXer.clean_sdmx_data(dirty_df_text)
        # Invalid number becomes nothing from tryparse, which then becomes missing
        @test cleaned_df_text.OBS_VALUE[2] === nothing || ismissing(cleaned_df_text.OBS_VALUE[2])
        @test cleaned_df_text.OBS_VALUE[1] == 100.0
        @test cleaned_df_text.OBS_VALUE[3] == 200.5
        
        # Test with already numeric OBS_VALUE
        already_clean = DataFrame(
            OBS_VALUE = [1.0, 2.0, 3.0],
            TIME_PERIOD = ["2020", "2021", "2022"]
        )
        cleaned_already = SDMXer.clean_sdmx_data(already_clean)
        @test cleaned_already.OBS_VALUE == [1.0, 2.0, 3.0]
        
        # Test with completely empty rows
        df_with_empty = DataFrame(
            OBS_VALUE = [100, missing, 200],
            TIME_PERIOD = ["2020", missing, "2022"],
            OTHER = ["A", missing, "C"]
        )
        cleaned_no_empty = SDMXer.clean_sdmx_data(df_with_empty)
        @test nrow(cleaned_no_empty) == 3  # Row 2 has some non-missing values
        
        # Test with DataFrame without standard columns
        non_standard = DataFrame(
            COL1 = [1, 2, 3],
            COL2 = ["A", "B", "C"]
        )
        cleaned_non_standard = SDMXer.clean_sdmx_data(non_standard)
        @test cleaned_non_standard == non_standard  # Should return unchanged
        
        # Test with empty DataFrame
        empty_df = DataFrame()
        cleaned_empty = SDMXer.clean_sdmx_data(empty_df)
        @test cleaned_empty == empty_df
        @test nrow(cleaned_empty) == 0
    end
    
    @testset "get_dimension_order" begin
        # Test that dimension order is extracted correctly from schema
        dim_order = SDMXer.get_dimension_order(spc_schema)
        @test dim_order isa Vector{String}
        @test !isempty(dim_order)
        @test "FREQ" in dim_order
        @test "GEO_PICT" in dim_order
        @test "TIME_PERIOD" in dim_order
        
        # Verify order matches the schema DataFrame order plus TIME_PERIOD
        # get_dimension_order includes TIME_PERIOD, dimensions doesn't
        expected_dims = vcat(spc_schema.dimensions.dimension_id, ["TIME_PERIOD"])
        @test dim_order == expected_dims
    end
    
    @testset "query_sdmx_data" begin
        # Note: This function makes actual HTTP requests, so we'll test the interface
        # but not make actual network calls in unit tests
        
        # Test that the function exists and has the expected signature
        @test hasmethod(SDMXer.query_sdmx_data, 
                       Tuple{String, String, String, String})
        @test hasmethod(SDMXer.query_sdmx_data, 
                       Tuple{String, String, String})  # version defaults to "latest"
        
        # Test TIME_PERIOD range parsing without actual HTTP calls
        # The function should extract start/end from TIME_PERIOD filter
        # We can't fully test without HTTP but can verify the logic works
    end
    
    @testset "summarize_data" begin
        # Test with a sample DataFrame
        test_data = DataFrame(
            TIME_PERIOD = ["2020", "2021", "2022", "2023"],
            OBS_VALUE = [100.0, 150.0, missing, 200.0],
            GEO_PICT = ["FJ", "FJ", "TO", "TO"],
            INDICATOR = ["GDP", "GDP", "CPI", "CPI"]
        )
        
        summary = SDMXer.summarize_data(test_data)
        
        @test summary["total_observations"] == 4
        @test summary["time_range"] == ("2020", "2023")
        @test haskey(summary, "obs_stats")
        @test summary["obs_stats"].min == 100.0
        @test summary["obs_stats"].max == 200.0
        @test summary["obs_stats"].mean == 150.0
        @test summary["obs_stats"].count == 3  # 3 non-missing values
        
        # Test unique dimension values (note: lowercase keys in summary)
        @test summary["geo_pict"] == ["FJ", "TO"]
        @test summary["indicator"] == ["CPI", "GDP"]
        
        # Test with empty DataFrame
        empty_summary = SDMXer.summarize_data(DataFrame())
        @test empty_summary["total_observations"] == 0
        
        # Test with DataFrame without standard SDMX columns
        non_standard_data = DataFrame(
            COL1 = [1, 2, 3],
            COL2 = ["A", "B", "C"]
        )
        non_standard_summary = SDMXer.summarize_data(non_standard_data)
        @test non_standard_summary["total_observations"] == 3
        @test !haskey(non_standard_summary, "time_range")
        @test !haskey(non_standard_summary, "obs_stats")
        # Non-SDMX columns won't be included in summary
        @test !haskey(non_standard_summary, "COL2")
        
        # Test with all missing OBS_VALUE
        all_missing_data = DataFrame(
            TIME_PERIOD = ["2020", "2021"],
            OBS_VALUE = [missing, missing]
        )
        all_missing_summary = SDMXer.summarize_data(all_missing_data)
        @test all_missing_summary["total_observations"] == 2
        @test !haskey(all_missing_summary, "obs_stats")  # No stats if all values are missing
        
        # Test with FREQ dimension (another common SDMX dimension)
        freq_data = DataFrame(
            TIME_PERIOD = ["2020", "2021"],
            OBS_VALUE = [100.0, 200.0],
            FREQ = ["A", "A"]
        )
        freq_summary = SDMXer.summarize_data(freq_data)
        @test freq_summary["freq"] == ["A"]
    end
    
    @testset "format_dimension_value" begin
        # Test the helper function for formatting dimension values
        @test SDMXer.format_dimension_value("TEST") == "TEST"
        @test SDMXer.format_dimension_value(["A", "B", "C"]) == "A+B+C"
        @test SDMXer.format_dimension_value(nothing) == ""
        @test SDMXer.format_dimension_value(["SINGLE"]) == "SINGLE"
    end
    
    @testset "fetch_sdmx_data error handling" begin
        # Test with invalid URL (should throw ArgumentError)
        @test_throws ArgumentError SDMXer.fetch_sdmx_data("not_a_url")
        
        # Test timeout parameter is passed correctly
        # Note: We can't test actual timeout without making network calls
        @test hasmethod(SDMXer.fetch_sdmx_data, Tuple{String})
    end
    
    @testset "Integration test - construct and validate URLs" begin
        # Test a complete workflow
        base_url = "https://stats-sdmx-disseminate.pacificdata.org/rest"
        agency = "SPC"
        dataflow = "DF_BP50"
        version = "1.0"
        
        # Create filters that match schema dimensions
        valid_filters = Dict(
            "FREQ" => "A",
            "INDICATOR" => "BP50_01",
            "GEO_PICT" => "FJ"
        )
        
        # Construct URL with schema validation
        url = construct_data_url(base_url, agency, dataflow, version,
                                schema=spc_schema,
                                dimension_filters=valid_filters,
                                start_period="2022")
        
        @test occursin(base_url, url)
        @test occursin("SPC,DF_BP50,1.0", url)
        @test occursin("startPeriod=2022", url)
        @test occursin("A", url)  # FREQ value
        @test occursin("BP50_01", url)  # INDICATOR value
        @test occursin("FJ", url)  # GEO_PICT value
        
        # Verify the key is properly constructed
        key = SDMXer.construct_sdmx_key(spc_schema, valid_filters)
        @test occursin(key, url) || occursin(replace(key, "." => "%2E"), url)  # URL encoding
    end
end