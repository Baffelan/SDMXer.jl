using Test
using SDMXer

@testset "SDMX Units" begin
    @testset "sdmx_to_unitful" begin
        # Known codes
        spec = sdmx_to_unitful("KG")
        @test spec isa SDMXUnitSpec
        @test spec.code == "KG"
        @test spec.category === :mass
        @test spec.description == "Kilogram"

        spec_t = sdmx_to_unitful("T")
        @test spec_t isa SDMXUnitSpec
        @test spec_t.category === :mass

        spec_usd = sdmx_to_unitful("USD")
        @test spec_usd isa SDMXUnitSpec
        @test spec_usd.category === :currency

        spec_idx = sdmx_to_unitful("IDX")
        @test spec_idx isa SDMXUnitSpec
        @test spec_idx.category === :dimensionless

        # Case insensitive
        @test sdmx_to_unitful("kg") isa SDMXUnitSpec

        # Unknown code
        @test sdmx_to_unitful("UNKNOWN") === nothing
        @test sdmx_to_unitful("") === nothing
    end

    @testset "unit_multiplier" begin
        @test unit_multiplier(0) == 1.0
        @test unit_multiplier(3) == 1000.0
        @test unit_multiplier(6) == 1.0e6
        @test unit_multiplier(9) == 1.0e9
        @test unit_multiplier("3") == 1000.0
        @test unit_multiplier("0") == 1.0
        @test unit_multiplier(nothing) == 1.0
        @test unit_multiplier(missing) == 1.0
    end

    @testset "are_units_convertible" begin
        # Same dimension — mass
        @test are_units_convertible("KG", "T") == true
        @test are_units_convertible("KG", "G") == true
        @test are_units_convertible("T", "MT") == true

        # Different dimensions
        @test are_units_convertible("KG", "L") == false
        @test are_units_convertible("KG", "KWH") == false
        @test are_units_convertible("HA", "L") == false

        # Currencies — blocked from auto-conversion
        @test are_units_convertible("USD", "EUR") == false
        @test are_units_convertible("FJD", "AUD") == false

        # Same currency is trivially convertible
        @test are_units_convertible("USD", "USD") == true

        # Unknown codes
        @test are_units_convertible("UNKNOWN", "KG") == false
        @test are_units_convertible("KG", "UNKNOWN") == false
    end

    @testset "conversion_factor" begin
        # Mass conversions
        factor_kg_t = conversion_factor("KG", "T")
        @test factor_kg_t !== nothing
        @test factor_kg_t ≈ 0.001

        factor_t_kg = conversion_factor("T", "KG")
        @test factor_t_kg !== nothing
        @test factor_t_kg ≈ 1000.0

        # Identity
        @test conversion_factor("KG", "KG") == 1.0

        # Currencies return nothing
        @test conversion_factor("USD", "EUR") === nothing
        @test conversion_factor("FJD", "AUD") === nothing

        # Incompatible units return nothing
        @test conversion_factor("KG", "L") === nothing
        @test conversion_factor("KWH", "HA") === nothing
    end

    @testset "ExchangeRateTable" begin
        # Empty table
        table = ExchangeRateTable()
        @test isempty(table.rates)

        # Add rate — should add inverse
        add_rate!(table, "USD", "FJD", 2.299)
        @test get_rate(table, "USD", "FJD") ≈ 2.299
        @test get_rate(table, "FJD", "USD") ≈ 1.0 / 2.299

        # Identity
        @test get_rate(table, "USD", "USD") == 1.0
        @test get_rate(table, "FJD", "FJD") == 1.0

        # Cross-rate via USD
        add_rate!(table, "USD", "AUD", 1.552)
        cross = get_rate(table, "FJD", "AUD")
        @test cross !== nothing
        @test cross ≈ (1.0 / 2.299) * 1.552

        # Unknown pair
        @test get_rate(table, "XXX", "YYY") === nothing
    end

    @testset "convert_currency" begin
        table = ExchangeRateTable()
        add_rate!(table, "USD", "FJD", 2.299)

        result = convert_currency(100.0, "USD", "FJD", table)
        @test result !== nothing
        @test result ≈ 229.9

        result_inv = convert_currency(229.9, "FJD", "USD", table)
        @test result_inv !== nothing
        @test result_inv ≈ 100.0 atol = 0.1

        # Unknown currency
        @test convert_currency(100.0, "USD", "XXX", table) === nothing
    end

    @testset "default_exchange_rates" begin
        table = default_exchange_rates()
        @test !isempty(table.rates)
        @test table.reference_date == "2025"

        # Check some known rates
        @test get_rate(table, "USD", "FJD") ≈ 2.299
        @test get_rate(table, "USD", "AUD") ≈ 1.552
        @test get_rate(table, "USD", "EUR") ≈ 0.885

        # Cross-rates should work
        fjd_aud = get_rate(table, "FJD", "AUD")
        @test fjd_aud !== nothing
        @test fjd_aud > 0.0
    end
end
