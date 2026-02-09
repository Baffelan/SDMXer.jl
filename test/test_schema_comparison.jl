using Test
using SDMXer
using DataFrames

# Define fixture_path if not already defined (when run standalone)
if !@isdefined(fixture_path)
    fixture_path(filename) = joinpath(@__DIR__, "fixtures", filename)
end

@testset "Schema Comparison" begin
    @testset "codelist_overlap with vectors" begin
        # Partial overlap
        overlap = codelist_overlap(["FJ", "TV", "WS"], ["FJ", "TV", "PG"])
        @test Set(overlap.intersection) == Set(["FJ", "TV"])
        @test Set(overlap.only_in_a) == Set(["WS"])
        @test Set(overlap.only_in_b) == Set(["PG"])
        @test overlap.overlap_ratio ≈ 2 / 4  # |intersection| / |union|
        @test overlap.a_coverage ≈ 2 / 3
        @test overlap.b_coverage ≈ 2 / 3

        # Identical sets
        overlap_same = codelist_overlap(["A", "B"], ["A", "B"])
        @test length(overlap_same.intersection) == 2
        @test isempty(overlap_same.only_in_a)
        @test isempty(overlap_same.only_in_b)
        @test overlap_same.overlap_ratio ≈ 1.0

        # Disjoint sets
        overlap_none = codelist_overlap(["A", "B"], ["C", "D"])
        @test isempty(overlap_none.intersection)
        @test overlap_none.overlap_ratio ≈ 0.0

        # Empty sets
        overlap_empty = codelist_overlap(String[], String[])
        @test isempty(overlap_empty.intersection)
        @test overlap_empty.overlap_ratio ≈ 0.0
    end

    @testset "codelist_overlap with DataFrames" begin
        cl_a = DataFrame(
            codelist_id = ["CL_GEO", "CL_GEO", "CL_GEO"],
            code_id = ["FJ", "TV", "WS"],
            lang = ["en", "en", "en"],
            name = ["Fiji", "Tuvalu", "Samoa"]
        )
        cl_b = DataFrame(
            codelist_id = ["CL_GEO", "CL_GEO", "CL_GEO"],
            code_id = ["FJ", "TV", "PG"],
            lang = ["en", "en", "en"],
            name = ["Fiji", "Tuvalu", "PNG"]
        )

        overlap = codelist_overlap(cl_a, cl_b, "CL_GEO", "CL_GEO")
        @test Set(overlap.intersection) == Set(["FJ", "TV"])
        @test overlap.overlap_ratio > 0.0

        # Non-existent codelist
        overlap_miss = codelist_overlap(cl_a, cl_b, "CL_MISSING", "CL_GEO")
        @test isempty(overlap_miss.intersection)
    end

    @testset "compare_schemas with fixtures" begin
        # Load both fixture schemas (pass file path, not content, to avoid ENAMETOOLONG)
        bp50_path = fixture_path("spc_df_bp50.xml")
        abs_path = fixture_path("abs_df_min_exp.xml")

        schema_bp50 = extract_dataflow_schema(bp50_path)
        schema_abs = extract_dataflow_schema(abs_path)

        comparison = compare_schemas(schema_bp50, schema_abs)

        @test comparison isa SchemaComparison
        @test comparison.schema_a_info.id == schema_bp50.dataflow_info.id
        @test comparison.schema_b_info.id == schema_abs.dataflow_info.id

        # The two fixtures are from different agencies so may have
        # few or no shared codelists, but the comparison should still work
        @test comparison.joinability_score >= 0.0
        @test comparison.joinability_score <= 1.0

        # unique_to_a and unique_to_b should be vectors
        @test comparison.unique_to_a isa Vector{String}
        @test comparison.unique_to_b isa Vector{String}
    end

    @testset "compare_schemas identical" begin
        bp50_path = fixture_path("spc_df_bp50.xml")
        schema = extract_dataflow_schema(bp50_path)

        comparison = compare_schemas(schema, schema)

        # Identical schemas should have high joinability
        @test comparison.joinability_score > 0.5
        @test isempty(comparison.unique_to_a)
        @test isempty(comparison.unique_to_b)
        # All dimensions should be shared (same codelist_id)
        @test !isempty(comparison.shared_dimensions) || !isempty(comparison.recommended_join_dims)
    end
end
