using Test
using SDMXer
using DataFrames

@testset "Concept Extraction" begin

    @testset "SPC Concepts (DF_BP50)" begin
        spc_file = fixture_path("spc_df_bp50.xml")
        concepts_df = extract_concepts(spc_file)

        @test nrow(concepts_df) > 0
        @test "concept_id" in names(concepts_df)
        @test "role" in names(concepts_df)
        @test any(concepts_df.role .== "dimension")
        @test any(concepts_df.role .== "attribute")
        @test any(concepts_df.role .== "measure")
        @test any(concepts_df.role .== "time_dimension")
        @test "INDICATOR" in concepts_df.concept_id
    end

    # UNICEF, OECD, and Eurostat tests removed as fixtures are obsolete

end
