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
