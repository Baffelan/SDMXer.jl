using Test
using SDMXer
using DataFrames

@testset "Cross-Dataflow Join" begin
    @testset "detect_join_columns" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ", "TV"],
            TIME_PERIOD = ["2020", "2020"],
            INDICATOR = ["IND1", "IND1"],
            OBS_VALUE = [100.0, 200.0],
            UNIT_MEASURE = ["USD", "USD"]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ", "TV"],
            TIME_PERIOD = ["2020", "2020"],
            OBS_VALUE = [50.0, 60.0],
            UNIT_MEASURE = ["NUM", "NUM"]
        )

        cols = detect_join_columns(df_a, df_b)
        @test "GEO_PICT" in cols
        @test "TIME_PERIOD" in cols
        # OBS_VALUE and UNIT_MEASURE should be excluded
        @test !("OBS_VALUE" in cols)
        @test !("UNIT_MEASURE" in cols)
    end

    @testset "detect_join_columns — no overlap" begin
        df_a = DataFrame(A = ["x", "y"], OBS_VALUE = [1.0, 2.0])
        df_b = DataFrame(B = ["x", "y"], OBS_VALUE = [3.0, 4.0])

        cols = detect_join_columns(df_a, df_b)
        # Only OBS_VALUE is common but excluded
        @test isempty(cols)
    end

    @testset "sdmx_join — inner join" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ", "TV", "WS"],
            TIME_PERIOD = ["2020", "2020", "2020"],
            OBS_VALUE = [100.0, 200.0, 300.0],
            UNIT_MEASURE = ["USD", "USD", "USD"],
            UNIT_MULT = [0, 0, 0]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ", "TV", "PG"],
            TIME_PERIOD = ["2020", "2020", "2020"],
            OBS_VALUE = [50.0, 60.0, 70.0],
            UNIT_MEASURE = ["NUM", "NUM", "NUM"],
            UNIT_MULT = [0, 0, 0]
        )

        result = sdmx_join(df_a, df_b)
        @test result isa JoinResult
        @test result.join_type === :inner
        @test !isempty(result.join_columns)
        @test "GEO_PICT" in result.join_columns
        @test "TIME_PERIOD" in result.join_columns

        # Inner join should have 2 rows (FJ, TV overlap)
        @test nrow(result.data) == 2

        # OBS_VALUE should be renamed with suffixes
        col_names = string.(names(result.data))
        @test any(c -> startswith(c, "OBS_VALUE"), col_names)
    end

    @testset "sdmx_join — outer join" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ", "TV"],
            TIME_PERIOD = ["2020", "2020"],
            OBS_VALUE = [100.0, 200.0]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ", "PG"],
            TIME_PERIOD = ["2020", "2020"],
            OBS_VALUE = [50.0, 70.0]
        )

        result = sdmx_join(df_a, df_b; join_type = :outer)
        @test result.join_type === :outer
        @test nrow(result.data) == 3  # FJ, TV, PG
    end

    @testset "sdmx_join — explicit columns" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ"],
            TIME_PERIOD = ["2020"],
            OBS_VALUE = [100.0]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ"],
            TIME_PERIOD = ["2020"],
            OBS_VALUE = [50.0]
        )

        result = sdmx_join(df_a, df_b; on = ["GEO_PICT"])
        @test result.join_columns == ["GEO_PICT"]
    end

    @testset "sdmx_join — suffix renaming" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ"],
            OBS_VALUE = [100.0],
            UNIT_MEASURE = ["USD"]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ"],
            OBS_VALUE = [50.0],
            UNIT_MEASURE = ["NUM"]
        )

        result = sdmx_join(df_a, df_b; suffix_a = "_trade", suffix_b = "_pop")
        col_names = string.(names(result.data))
        @test "OBS_VALUE_trade" in col_names
        @test "OBS_VALUE_pop" in col_names
        @test "UNIT_MEASURE_trade" in col_names
        @test "UNIT_MEASURE_pop" in col_names
    end

    @testset "sdmx_join — time range warnings" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ"],
            TIME_PERIOD = ["2020"],
            OBS_VALUE = [100.0]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ"],
            TIME_PERIOD = ["2025"],
            OBS_VALUE = [50.0]
        )

        result = sdmx_join(df_a, df_b; on = ["GEO_PICT", "TIME_PERIOD"], join_type = :outer)
        @test any(w -> occursin("time", lowercase(w)) || occursin("period", lowercase(w)), result.warnings)
    end

    @testset "sdmx_join — with exchange rates" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ"],
            TIME_PERIOD = ["2020"],
            OBS_VALUE = [100.0],
            UNIT_MEASURE = ["USD"],
            UNIT_MULT = [0]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ"],
            TIME_PERIOD = ["2020"],
            OBS_VALUE = [229.9],
            UNIT_MEASURE = ["FJD"],
            UNIT_MULT = [0]
        )

        rates = default_exchange_rates()
        result = sdmx_join(df_a, df_b;
            exchange_rates = rates,
            validate_units = true,
            harmonize = true)

        @test nrow(result.data) >= 1
        @test result.unit_report !== nothing
    end

    @testset "JoinResult metadata" begin
        df_a = DataFrame(GEO = ["FJ"], OBS_VALUE = [1.0])
        df_b = DataFrame(GEO = ["FJ"], OBS_VALUE = [2.0])

        result = sdmx_join(df_a, df_b)
        @test haskey(result.metadata, "rows_a")
        @test haskey(result.metadata, "rows_b")
        @test haskey(result.metadata, "rows_joined")
        @test result.metadata["rows_a"] == 1
        @test result.metadata["rows_b"] == 1
    end
end

@testset "sdmx_combine — Vertical Stacking" begin
    @testset "basic stacking" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ", "TV"],
            TIME_PERIOD = ["2020", "2020"],
            OBS_VALUE = [100.0, 200.0],
            UNIT_MEASURE = ["USD", "USD"],
            UNIT_MULT = [0, 0]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ", "TV"],
            TIME_PERIOD = ["2020", "2020"],
            OBS_VALUE = [50.0, 60.0],
            UNIT_MEASURE = ["NUM", "NUM"],
            UNIT_MULT = [0, 0]
        )

        result = sdmx_combine(df_a, df_b)
        @test result isa CombineResult
        @test nrow(result.data) == nrow(df_a) + nrow(df_b)
        @test "DATAFLOW" in names(result.data)
        @test result.source_col == "DATAFLOW"
        @test result.sources == ["A", "B"]
    end

    @testset "different columns — union fill" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ"],
            INDICATOR = ["GDP"],
            OBS_VALUE = [1000.0]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ"],
            COMMODITY = ["Kava"],
            OBS_VALUE = [50.0]
        )

        result = sdmx_combine(df_a, df_b; validate_units = false)
        @test nrow(result.data) == 2
        @test "INDICATOR" in names(result.data)
        @test "COMMODITY" in names(result.data)
        # Non-overlapping columns should have missing values
        @test ismissing(result.data[2, :INDICATOR])
        @test ismissing(result.data[1, :COMMODITY])
    end

    @testset "provenance labels" begin
        df_a = DataFrame(GEO = ["FJ"], OBS_VALUE = [1.0])
        df_b = DataFrame(GEO = ["TV"], OBS_VALUE = [2.0])

        result = sdmx_combine(df_a, df_b;
            source_col = "SOURCE",
            source_a = "Trade",
            source_b = "Population",
            validate_units = false)

        @test result.source_col == "SOURCE"
        @test result.sources == ["Trade", "Population"]
        @test "SOURCE" in names(result.data)
        source_vals = result.data[!, :SOURCE]
        @test "Trade" in source_vals
        @test "Population" in source_vals
    end

    @testset "unit warnings — informational" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ"],
            OBS_VALUE = [100.0],
            UNIT_MEASURE = ["USD"],
            UNIT_MULT = [0]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ"],
            OBS_VALUE = [50.0],
            UNIT_MEASURE = ["NUM"],
            UNIT_MULT = [0]
        )

        result = sdmx_combine(df_a, df_b; validate_units = true)
        @test !isempty(result.unit_reports)
        # Combine should succeed despite unit differences (not blocking)
        @test nrow(result.data) == 2
    end

    @testset "n-ary variant" begin
        df_a = DataFrame(GEO = ["FJ"], OBS_VALUE = [1.0])
        df_b = DataFrame(GEO = ["TV"], OBS_VALUE = [2.0])
        df_c = DataFrame(GEO = ["WS"], OBS_VALUE = [3.0])

        result = sdmx_combine([df_a, df_b, df_c];
            sources = ["Trade", "Pop", "GDP"],
            validate_units = false)

        @test result isa CombineResult
        @test nrow(result.data) == 3
        @test result.sources == ["Trade", "Pop", "GDP"]
        source_vals = unique(result.data[!, :DATAFLOW])
        @test length(source_vals) == 3
        @test "Trade" in source_vals
        @test "Pop" in source_vals
        @test "GDP" in source_vals
    end

    @testset "n-ary — auto labels" begin
        df_a = DataFrame(GEO = ["FJ"], OBS_VALUE = [1.0])
        df_b = DataFrame(GEO = ["TV"], OBS_VALUE = [2.0])

        result = sdmx_combine([df_a, df_b]; validate_units = false)
        @test result.sources == ["DF_1", "DF_2"]
    end

    @testset "harmonize UNIT_MULT" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ"],
            OBS_VALUE = [1.0],
            UNIT_MEASURE = ["USD"],
            UNIT_MULT = [3]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ"],
            OBS_VALUE = [500.0],
            UNIT_MEASURE = ["USD"],
            UNIT_MULT = [0]
        )

        result = sdmx_combine(df_a, df_b; harmonize = true, validate_units = false)
        @test nrow(result.data) == 2
        # After harmonization, UNIT_MULT=3 with OBS_VALUE=1.0 should become 1000.0
        vals = sort(result.data[!, :OBS_VALUE])
        @test vals[1] ≈ 500.0
        @test vals[2] ≈ 1000.0
    end
end

@testset "pivot_sdmx_wide" begin
    df = DataFrame(
        GEO_PICT = ["FJ", "FJ", "TV", "TV"],
        INDICATOR = ["GDP", "POP", "GDP", "POP"],
        OBS_VALUE = [1000.0, 50.0, 800.0, 30.0]
    )

    wide = pivot_sdmx_wide(df; indicator_col = :INDICATOR)
    @test nrow(wide) == 2
    @test "GDP" in names(wide)
    @test "POP" in names(wide)
    # Check values are correctly pivoted
    fj_row = filter(r -> r.GEO_PICT == "FJ", wide)
    @test fj_row[1, :GDP] ≈ 1000.0
    @test fj_row[1, :POP] ≈ 50.0
end
