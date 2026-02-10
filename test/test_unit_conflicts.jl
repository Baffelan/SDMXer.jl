using Test
using SDMXer
using DataFrames

@testset "Unit Conflicts" begin
    @testset "detect_unit_conflicts — no conflicts" begin
        df_a = DataFrame(
            GEO = ["FJ", "TV"],
            OBS_VALUE = [100.0, 200.0],
            UNIT_MEASURE = ["KG", "KG"],
            UNIT_MULT = [0, 0]
        )
        df_b = DataFrame(
            GEO = ["FJ", "TV"],
            OBS_VALUE = [50.0, 60.0],
            UNIT_MEASURE = ["KG", "KG"],
            UNIT_MULT = [0, 0]
        )

        report = detect_unit_conflicts(df_a, df_b)
        @test report isa UnitConflictReport
        @test isempty(report.conflicts)
        @test !report.has_blocking_conflicts
        @test report.auto_resolvable_count == 0
    end

    @testset "detect_unit_conflicts — convertible units" begin
        df_a = DataFrame(
            GEO = ["FJ"],
            OBS_VALUE = [1000.0],
            UNIT_MEASURE = ["KG"]
        )
        df_b = DataFrame(
            GEO = ["FJ"],
            OBS_VALUE = [1.0],
            UNIT_MEASURE = ["T"]
        )

        report = detect_unit_conflicts(df_a, df_b)
        @test length(report.unit_measure_conflicts) == 1
        @test report.unit_measure_conflicts[1].is_convertible == true
        @test report.unit_measure_conflicts[1].severity === :warning
        @test !report.has_blocking_conflicts
        @test report.auto_resolvable_count == 1
    end

    @testset "detect_unit_conflicts — incompatible units" begin
        df_a = DataFrame(OBS_VALUE = [1.0], UNIT_MEASURE = ["KG"])
        df_b = DataFrame(OBS_VALUE = [1.0], UNIT_MEASURE = ["L"])

        report = detect_unit_conflicts(df_a, df_b)
        @test length(report.conflicts) == 1
        @test report.conflicts[1].is_convertible == false
        @test report.conflicts[1].severity === :error
        @test report.has_blocking_conflicts == true
    end

    @testset "detect_unit_conflicts — currency with exchange rates" begin
        df_a = DataFrame(OBS_VALUE = [100.0], UNIT_MEASURE = ["USD"])
        df_b = DataFrame(OBS_VALUE = [229.9], UNIT_MEASURE = ["FJD"])

        # Without exchange rates — error
        report_no_rates = detect_unit_conflicts(df_a, df_b)
        @test report_no_rates.has_blocking_conflicts == true

        # With exchange rates — warning
        rates = default_exchange_rates()
        report_with_rates = detect_unit_conflicts(df_a, df_b; exchange_rates = rates)
        @test !report_with_rates.has_blocking_conflicts
        @test report_with_rates.auto_resolvable_count > 0
    end

    @testset "detect_unit_conflicts — UNIT_MULT mismatch" begin
        df_a = DataFrame(OBS_VALUE = [1.0], UNIT_MULT = [0])
        df_b = DataFrame(OBS_VALUE = [1.0], UNIT_MULT = [3])

        report = detect_unit_conflicts(df_a, df_b)
        @test length(report.unit_mult_conflicts) == 1
        @test report.unit_mult_conflicts[1].is_convertible == true
    end

    @testset "detect_unit_conflicts — grouped: different indicators, different units" begin
        # Scenario: df_a has monetary (FJD) and mass (TON) indicators
        # df_b has count (NUM) indicators. Joining on GEO + TIME_PERIOD + INDICATOR.
        # Only indicator "IND1" appears in both — FJD vs NUM is a real conflict.
        # "IND2" (TON) only appears in df_a, so TON vs NUM is NOT a conflict.
        df_a = DataFrame(
            GEO = ["FJ", "FJ", "FJ"],
            TIME_PERIOD = ["2020", "2020", "2020"],
            INDICATOR = ["IND1", "IND2", "IND1"],
            OBS_VALUE = [100.0, 50.0, 200.0],
            UNIT_MEASURE = ["FJD", "TON", "FJD"]
        )
        df_b = DataFrame(
            GEO = ["FJ", "FJ"],
            TIME_PERIOD = ["2020", "2020"],
            INDICATOR = ["IND1", "IND3"],
            OBS_VALUE = [5000.0, 3000.0],
            UNIT_MEASURE = ["NUM", "NUM"]
        )

        # Grouped: only IND1 matches, so only FJD vs NUM should be flagged
        report_grouped = detect_unit_conflicts(df_a, df_b;
            join_dims = ["GEO", "TIME_PERIOD", "INDICATOR"])
        grouped_pairs = Set((c.value_a, c.value_b) for c in report_grouped.unit_measure_conflicts)
        @test ("FJD", "NUM") in grouped_pairs
        @test !(("TON", "NUM") in grouped_pairs)  # TON never matched — not a conflict

        # All-vs-all: both FJD-NUM and TON-NUM would be flagged
        report_allvsall = detect_unit_conflicts(df_a, df_b)
        allvsall_pairs = Set((c.value_a, c.value_b) for c in report_allvsall.unit_measure_conflicts)
        @test ("FJD", "NUM") in allvsall_pairs
        @test ("TON", "NUM") in allvsall_pairs  # false positive in all-vs-all
    end

    @testset "detect_unit_conflicts — grouped: same unit everywhere, no conflict" begin
        df_a = DataFrame(
            GEO = ["FJ", "TV"],
            OBS_VALUE = [100.0, 200.0],
            UNIT_MEASURE = ["USD", "USD"]
        )
        df_b = DataFrame(
            GEO = ["FJ", "TV"],
            OBS_VALUE = [50.0, 60.0],
            UNIT_MEASURE = ["USD", "USD"]
        )

        report = detect_unit_conflicts(df_a, df_b; join_dims = ["GEO"])
        @test isempty(report.conflicts)
    end

    @testset "detect_unit_conflicts — grouped: join dim not in both DFs falls back" begin
        df_a = DataFrame(
            COUNTRY = ["FJ"],
            OBS_VALUE = [100.0],
            UNIT_MEASURE = ["KG"]
        )
        df_b = DataFrame(
            REGION = ["Pacific"],
            OBS_VALUE = [1.0],
            UNIT_MEASURE = ["T"]
        )

        # join_dims don't exist in both → falls back to all-vs-all
        report = detect_unit_conflicts(df_a, df_b; join_dims = ["COUNTRY"])
        @test length(report.unit_measure_conflicts) == 1
        @test report.unit_measure_conflicts[1].value_a == "KG"
        @test report.unit_measure_conflicts[1].value_b == "T"
    end

    @testset "detect_unit_conflicts — grouped UNIT_MULT" begin
        df_a = DataFrame(
            GEO = ["FJ", "FJ"],
            INDICATOR = ["GDP", "POP"],
            OBS_VALUE = [1.0, 2.0],
            UNIT_MULT = [6, 0]
        )
        df_b = DataFrame(
            GEO = ["FJ"],
            INDICATOR = ["GDP"],
            OBS_VALUE = [3.0],
            UNIT_MULT = [3]
        )

        # Only GDP matches: UNIT_MULT 6 vs 3 is a real conflict
        report = detect_unit_conflicts(df_a, df_b; join_dims = ["GEO", "INDICATOR"])
        @test length(report.unit_mult_conflicts) == 1
        @test report.unit_mult_conflicts[1].value_a == "6"
        @test report.unit_mult_conflicts[1].value_b == "3"

        # All-vs-all would also flag 0 vs 3 (POP vs GDP) — a false positive
        report_all = detect_unit_conflicts(df_a, df_b)
        @test length(report_all.unit_mult_conflicts) == 2
    end

    @testset "normalize_units! — UNIT_MULT" begin
        df = DataFrame(
            OBS_VALUE = [5.0, 10.0],
            UNIT_MULT = [3, 6],
            UNIT_MEASURE = ["USD", "USD"]
        )

        normalize_units!(df)
        @test df.OBS_VALUE[1] ≈ 5000.0
        @test df.OBS_VALUE[2] ≈ 10.0e6
        @test df.UNIT_MULT[1] == 0
        @test df.UNIT_MULT[2] == 0
    end

    @testset "normalize_units! — target unit conversion" begin
        df = DataFrame(
            OBS_VALUE = [1000.0, 2000.0],
            UNIT_MEASURE = ["KG", "KG"],
            UNIT_MULT = [0, 0]
        )

        normalize_units!(df; target_unit = "T")
        @test df.OBS_VALUE[1] ≈ 1.0
        @test df.OBS_VALUE[2] ≈ 2.0
        @test df.UNIT_MEASURE[1] == "T"
        @test df.UNIT_MEASURE[2] == "T"
    end

    @testset "normalize_units! — currency conversion" begin
        df = DataFrame(
            OBS_VALUE = [100.0],
            UNIT_MEASURE = ["USD"],
            UNIT_MULT = [0]
        )

        rates = ExchangeRateTable()
        add_rate!(rates, "USD", "FJD", 2.299)

        normalize_units!(df; target_unit = "FJD", exchange_rates = rates)
        @test df.OBS_VALUE[1] ≈ 229.9
        @test df.UNIT_MEASURE[1] == "FJD"
    end

    @testset "harmonize_units — non-mutating" begin
        df_a = DataFrame(OBS_VALUE = [1000.0], UNIT_MEASURE = ["KG"], UNIT_MULT = [0])
        df_b = DataFrame(OBS_VALUE = [2.0], UNIT_MEASURE = ["T"], UNIT_MULT = [0])

        norm_a, norm_b = harmonize_units(df_a, df_b)

        # Originals unchanged
        @test df_a.OBS_VALUE[1] == 1000.0
        @test df_b.OBS_VALUE[1] == 2.0

        # Normalized copies should have UNIT_MULT applied (both are 0, so no change)
        @test norm_a.OBS_VALUE[1] == 1000.0
        @test norm_b.OBS_VALUE[1] == 2.0
    end
end
