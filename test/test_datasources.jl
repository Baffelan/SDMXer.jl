using Test
using SDMXer
using DataFrames

@testset "SDMX Core Functionality" begin
    # Note: Data source reading and profiling functionality has been moved to SDMXLLM.jl
    # This file now focuses on core SDMX schema and validation tests
    
    @testset "Schema Extraction" begin
        spc_schema_file = fixture_path("spc_df_bp50.xml")
        schema = extract_dataflow_schema(spc_schema_file)
        
        @test schema isa DataflowSchema
        @test !isempty(schema.dimensions.dimension_id)
        @test !isempty(schema.measures.measure_id)
        @test schema.time_dimension !== nothing
    end
    
    @testset "Dataflow Components" begin
        spc_schema_file = fixture_path("spc_df_bp50.xml")
        schema = extract_dataflow_schema(spc_schema_file)
        
        # Test required columns
        required = get_required_columns(schema)
        @test "INDICATOR" in required
        @test "GEO_PICT" in required
        @test "TIME_PERIOD" in required
        
        # Test dimension order
        dim_order = get_dimension_order(schema)
        @test !isempty(dim_order)
        @test all(d -> d isa String, dim_order)
    end
end