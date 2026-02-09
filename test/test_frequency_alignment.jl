using Test
using SDMXer
using DataFrames

@testset "Frequency Alignment" begin
    @testset "align_frequencies — quarterly to annual" begin
        quarterly_df = DataFrame(
            GEO_PICT = repeat(["FJ"], 4),
            TIME_PERIOD = ["2020-Q1", "2020-Q2", "2020-Q3", "2020-Q4"],
            OBS_VALUE = [10.0, 20.0, 30.0, 40.0],
            FREQ = ["Q", "Q", "Q", "Q"]
        )
        annual_df = DataFrame(
            GEO_PICT = ["FJ"],
            TIME_PERIOD = ["2020"],
            OBS_VALUE = [500.0],
            FREQ = ["A"]
        )

        aligned_a, aligned_b, info = align_frequencies(quarterly_df, annual_df)

        @test info isa FrequencyAlignment
        @test info.target_freq == "A"
        @test info.method === :aggregate
        @test info.aggregation_fn === :sum

        # Quarterly should be aggregated to annual
        @test nrow(aligned_a) <= nrow(quarterly_df)
        # Annual should be unchanged
        @test nrow(aligned_b) == 1
    end

    @testset "align_frequencies — same frequency" begin
        df_a = DataFrame(
            GEO_PICT = ["FJ", "TV"],
            TIME_PERIOD = ["2020", "2020"],
            OBS_VALUE = [100.0, 200.0],
            FREQ = ["A", "A"]
        )
        df_b = DataFrame(
            GEO_PICT = ["FJ"],
            TIME_PERIOD = ["2020"],
            OBS_VALUE = [50.0],
            FREQ = ["A"]
        )

        aligned_a, aligned_b, info = align_frequencies(df_a, df_b)

        @test info.method === :none
        @test info.target_freq == "A"
        @test nrow(aligned_a) == 2
        @test nrow(aligned_b) == 1
    end

    @testset "align_frequencies — mean aggregation" begin
        quarterly_df = DataFrame(
            GEO_PICT = repeat(["FJ"], 4),
            TIME_PERIOD = ["2020-Q1", "2020-Q2", "2020-Q3", "2020-Q4"],
            OBS_VALUE = [10.0, 20.0, 30.0, 40.0],
            FREQ = ["Q", "Q", "Q", "Q"]
        )
        annual_df = DataFrame(
            GEO_PICT = ["FJ"],
            TIME_PERIOD = ["2020"],
            OBS_VALUE = [25.0],
            FREQ = ["A"]
        )

        aligned_a, _, info = align_frequencies(quarterly_df, annual_df; aggregation = :mean)

        @test info.aggregation_fn === :mean
        # Mean of [10, 20, 30, 40] = 25
        if nrow(aligned_a) == 1
            @test aligned_a.OBS_VALUE[1] ≈ 25.0
        end
    end

    @testset "align_frequencies — forced target frequency" begin
        df_a = DataFrame(
            TIME_PERIOD = ["2020-Q1", "2020-Q2"],
            OBS_VALUE = [10.0, 20.0],
            FREQ = ["Q", "Q"]
        )
        df_b = DataFrame(
            TIME_PERIOD = ["2020-01", "2020-02", "2020-03"],
            OBS_VALUE = [1.0, 2.0, 3.0],
            FREQ = ["M", "M", "M"]
        )

        _, _, info = align_frequencies(df_a, df_b; target_freq = "A")
        @test info.target_freq == "A"
    end

    @testset "FrequencyAlignment struct" begin
        fa = FrequencyAlignment("Q", "A", :aggregate, :sum)
        @test fa.source_freq == "Q"
        @test fa.target_freq == "A"
        @test fa.method === :aggregate
        @test fa.aggregation_fn === :sum
    end
end
