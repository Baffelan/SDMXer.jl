using Test
using SDMXer
using DataFrames

@testset "Validation System" begin

    # Setup
    spc_schema_file = fixture_path("spc_df_bp50.xml")
    schema = extract_dataflow_schema(spc_schema_file)
    validator = create_validator(schema)

    compliant_df = DataFrame(
        FREQ = "A",
        GEO_PICT = "FJ",
        INDICATOR = "BP50_01",
        TIME_PERIOD = "2022",
        OBS_VALUE = 1.0
    )

    @testset "Compliant Data" begin
        result = validator(compliant_df, "compliant_data")
        @test result.overall_score >= 0.8 # Should be high, but allow for INFO issues
        @test result.compliance_status == "compliant" || result.compliance_status == "minor_issues"
        # Some INFO level issues like outliers might appear even in good data
    end

    @testset "Rule: required_columns" begin
        df = select(compliant_df, Not(:FREQ))
        result = validator(df)
        @test any(issue -> issue.rule_id == "required_columns", result.issues)
        @test result.compliance_status in ["non_compliant", "major_issues", "minor_issues"]
    end

    @testset "Rule: column_types" begin
        df = copy(compliant_df)
        df.OBS_VALUE = string.(df.OBS_VALUE) # Wrong type
        result = validator(df)
        @test any(issue -> issue.rule_id == "column_types" && issue.severity == SDMXer.ERROR, result.issues)
    end

    @testset "Rule: codelist_compliance" begin
        df = copy(compliant_df)
        # The current implementation just checks for empty or long strings.
        df.GEO_PICT = [""] # Empty string should be caught
        result = validator(df)
        @test any(issue -> issue.rule_id == "codelist_compliance", result.issues)
    end

    @testset "Rule: time_format" begin
        df = copy(compliant_df)
        df.TIME_PERIOD = ["2022-13"] # Invalid month
        result = validator(df)
        @test any(issue -> issue.rule_id == "time_format", result.issues)
    end

    @testset "Rule: duplicates" begin
        df = vcat(compliant_df, compliant_df) # Create duplicate row
        result = validator(df)
        @test any(issue -> issue.rule_id == "duplicates", result.issues)
    end

    @testset "Report Generation" begin
        df = select(compliant_df, Not(:FREQ))
        result = validator(df, "bad_data")
        report = generate_validation_report(result, format="text")
        @test occursin("SDMX-CSV VALIDATION REPORT", report)
        @test occursin("Dataset: bad_data", report)
        @test occursin("Missing required columns", report)
    end

end
