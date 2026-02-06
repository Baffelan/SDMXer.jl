using Test
using SDMXer
using DataFrames

# Note: fixture_path is defined in the main runtests.jl
# This makes it available to all included test files.

@testset "Dataflow Schema Extraction" begin

    @testset "SPC Dataflow (DF_BP50)" begin
        spc_file = fixture_path("spc_df_bp50.xml")
        @test isfile(spc_file)

        schema = extract_dataflow_schema(spc_file)

        # Test dataflow basic info
        @test schema.dataflow_info.id == "DF_BP50"
        @test schema.dataflow_info.agency == "SPC"
        @test !ismissing(schema.dataflow_info.name)
        @test schema.dataflow_info.dsd_id == "DSD_BP50"

        # Test dimensions
        @test nrow(schema.dimensions) > 0
        @test "FREQ" in schema.dimensions.dimension_id
        @test "INDICATOR" in schema.dimensions.dimension_id
        @test "GEO_PICT" in schema.dimensions.dimension_id

        # Test time dimension
        @test schema.time_dimension !== nothing
        @test schema.time_dimension.dimension_id == "TIME_PERIOD"

        # Test attributes
        @test nrow(schema.attributes) > 0
        @test "UNIT_MEASURE" in schema.attributes.attribute_id
        conditional_attrs = filter(row -> row.assignment_status == "Conditional", schema.attributes)
        @test nrow(conditional_attrs) > 0

        # Test measure
        @test nrow(schema.measures) == 1
        @test schema.measures[1, :measure_id] == "OBS_VALUE"
        @test schema.measures[1, :data_type] == "Double"

        # Test helper functions
        required_cols = get_required_columns(schema)
        optional_cols = get_optional_columns(schema)
        codelist_cols = get_codelist_columns(schema)

        @test "FREQ" in required_cols
        @test "TIME_PERIOD" in required_cols
        @test "OBS_VALUE" in required_cols
        @test length(optional_cols) > 0
        @test haskey(codelist_cols, "FREQ")
        @test haskey(codelist_cols, "INDICATOR")
    end

    # UNICEF, OECD, and Eurostat tests removed as fixtures are obsolete

end
