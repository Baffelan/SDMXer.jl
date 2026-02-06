using Test
using SDMXer
using Dates

@testset "TimeAvailability membership tests" begin
    @testset "Year format membership" begin
        # Create a TimeAvailability with year format
        time_avail = SDMXer.TimeAvailability(1970, 2030, "year", 61, String[])
        
        # Test integer year membership
        @test 1970 ∈ time_avail
        @test 2000 ∈ time_avail
        @test 2030 ∈ time_avail
        @test !(1969 ∈ time_avail)
        @test !(2031 ∈ time_avail)
        
        # Test string year membership
        @test "1970" ∈ time_avail
        @test "2020" ∈ time_avail
        @test "2030" ∈ time_avail
        @test !("1969" ∈ time_avail)
        @test !("2031" ∈ time_avail)
    end
    
    @testset "Date format membership" begin
        # Create a TimeAvailability with date format
        time_avail = SDMXer.TimeAvailability(
            Date(2020, 1, 1),
            Date(2023, 12, 31),
            "date",
            4,
            String[]
        )
        
        # Test integer year membership for date ranges
        @test 2020 ∈ time_avail
        @test 2021 ∈ time_avail
        @test 2023 ∈ time_avail
        @test !(2019 ∈ time_avail)
        @test !(2024 ∈ time_avail)
        
        # Test string year membership
        @test "2020" ∈ time_avail
        @test "2022" ∈ time_avail
        @test !("2019" ∈ time_avail)
        @test !("2024" ∈ time_avail)
    end
    
    @testset "Quarter format membership" begin
        # Create a TimeAvailability with quarter format
        time_avail = SDMXer.TimeAvailability(
            "2020-Q1",
            "2023-Q4",
            "quarter",
            16,
            String[]
        )
        
        # Test quarter string membership
        @test "2020-Q1" ∈ time_avail
        @test "2021-Q2" ∈ time_avail
        @test "2023-Q4" ∈ time_avail
        @test !("2019-Q4" ∈ time_avail)
        @test !("2024-Q1" ∈ time_avail)
    end
    
    @testset "Semester format membership" begin
        # Create a TimeAvailability with semester format
        time_avail = SDMXer.TimeAvailability(
            "2020-S1",
            "2023-S2",
            "semester",
            8,
            String[]
        )
        
        # Test semester string membership
        @test "2020-S1" ∈ time_avail
        @test "2021-S2" ∈ time_avail
        @test "2023-S2" ∈ time_avail
        @test !("2019-S2" ∈ time_avail)
        @test !("2024-S1" ∈ time_avail)
    end
end

@testset "find_data_gaps with TIME_PERIOD" begin
    @testset "No gaps when all periods are available" begin
        # Create mock availability with time coverage
        dimensions = [
            SDMXer.DimensionAvailability("GEO_PICT", ["FJ", "SB", "VU"], 3, "codelist", 1.0),
            SDMXer.DimensionAvailability("TIME_PERIOD", ["1970-2030"], 1, "time", 1.0)
        ]
        
        time_coverage = SDMXer.TimeAvailability(1970, 2030, "year", 61, String[])
        
        availability = SDMXer.AvailabilityConstraint(
            "CC", "Test constraint", "TEST", "1.0",
            (id="TEST", agency="TEST", version="1.0"),
            1000, dimensions, time_coverage, "2025-01-01"
        )
        
        # Test with expected values within range
        expected = Dict(
            "GEO_PICT" => ["FJ", "SB"],
            "TIME_PERIOD" => ["2020", "2021", "2022"]
        )
        
        gaps = SDMXer.find_data_gaps(availability, expected)
        @test !haskey(gaps, "TIME_PERIOD")  # No TIME_PERIOD gaps
        @test !haskey(gaps, "GEO_PICT")     # No GEO_PICT gaps
    end
    
    @testset "Detect gaps when periods are outside range" begin
        # Create mock availability with limited time coverage
        dimensions = [
            SDMXer.DimensionAvailability("GEO_PICT", ["FJ", "SB"], 2, "codelist", 0.67),
            SDMXer.DimensionAvailability("TIME_PERIOD", ["2020-2022"], 1, "time", 1.0)
        ]
        
        time_coverage = SDMXer.TimeAvailability(2020, 2022, "year", 3, String[])
        
        availability = SDMXer.AvailabilityConstraint(
            "CC", "Test constraint", "TEST", "1.0",
            (id="TEST", agency="TEST", version="1.0"),
            1000, dimensions, time_coverage, "2025-01-01"
        )
        
        # Test with expected values outside range
        expected = Dict(
            "GEO_PICT" => ["FJ", "VU"],  # VU is missing
            "TIME_PERIOD" => ["2019", "2020", "2023"]  # 2019 and 2023 are outside range
        )
        
        gaps = SDMXer.find_data_gaps(availability, expected)
        @test haskey(gaps, "TIME_PERIOD")
        @test gaps["TIME_PERIOD"] == ["2019", "2023"]
        @test haskey(gaps, "GEO_PICT")
        @test gaps["GEO_PICT"] == ["VU"]
    end
    
    @testset "Handle quarterly TIME_PERIOD" begin
        # Create mock availability with quarterly time coverage
        dimensions = [
            SDMXer.DimensionAvailability("INDICATOR", ["GDP", "CPI"], 2, "codelist", 1.0),
            SDMXer.DimensionAvailability("TIME_PERIOD", ["2020-Q1 to 2021-Q4"], 1, "time", 1.0)
        ]
        
        time_coverage = SDMXer.TimeAvailability("2020-Q1", "2021-Q4", "quarter", 8, String[])
        
        availability = SDMXer.AvailabilityConstraint(
            "CC", "Test constraint", "TEST", "1.0",
            (id="TEST", agency="TEST", version="1.0"),
            500, dimensions, time_coverage, "2025-01-01"
        )
        
        # Test with quarterly expected values
        expected = Dict(
            "INDICATOR" => ["GDP", "UNEMP"],  # UNEMP is missing
            "TIME_PERIOD" => ["2020-Q1", "2021-Q3", "2022-Q1"]  # 2022-Q1 is outside range
        )
        
        gaps = SDMXer.find_data_gaps(availability, expected)
        @test haskey(gaps, "TIME_PERIOD")
        @test gaps["TIME_PERIOD"] == ["2022-Q1"]
        @test haskey(gaps, "INDICATOR")
        @test gaps["INDICATOR"] == ["UNEMP"]
    end
end

@testset "get_time_period_range" begin
    @testset "Year range" begin
        time_coverage = SDMXer.TimeAvailability(2020, 2023, "year", 4, String[])
        range = SDMXer.get_time_period_range(time_coverage)
        @test range == 2020:2023
        @test length(range) == 4
    end
    
    @testset "Quarter sequence" begin
        time_coverage = SDMXer.TimeAvailability("2020-Q2", "2021-Q1", "quarter", 4, String[])
        periods = SDMXer.get_time_period_range(time_coverage)
        @test periods == ["2020-Q2", "2020-Q3", "2020-Q4", "2021-Q1"]
        @test length(periods) == 4
    end
    
    @testset "Semester sequence" begin
        time_coverage = SDMXer.TimeAvailability("2020-S1", "2022-S2", "semester", 6, String[])
        periods = SDMXer.get_time_period_range(time_coverage)
        @test periods == ["2020-S1", "2020-S2", "2021-S1", "2021-S2", "2022-S1", "2022-S2"]
        @test length(periods) == 6
    end
    
    @testset "Date range" begin
        time_coverage = SDMXer.TimeAvailability(
            Date(2020, 1, 1),
            Date(2020, 1, 5),
            "day",
            5,
            String[]
        )
        range = SDMXer.get_time_period_range(time_coverage)
        @test range == Date(2020, 1, 1):Day(1):Date(2020, 1, 5)
        @test length(range) == 5
    end
end