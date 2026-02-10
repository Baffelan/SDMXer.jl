"""
SDMX-CSV Validation System for SDMXer.jl

This module provides comprehensive validation for SDMX-CSV files, ensuring they:
- Conform to SDMX-CSV structural requirements
- Meet specific dataflow schema constraints
- Pass data quality checks
- Include proper metadata and annotations
- Are compliant with statistical data standards

Features:
- Structure validation (required columns, data types)
- Content validation (codelists, value ranges, patterns)
- Quality assessment (completeness, consistency, accuracy)  
- Compliance reporting with actionable recommendations
- Performance-optimized for large datasets
"""

using DataFrames, Statistics, Dates, CSV
using EzXML, HTTP, JSON3


"""
    ValidationSeverity

Enumeration for validation issue severity levels in SDMX data validation.

This enum defines the severity levels used to classify validation issues, helping
users prioritize which problems to address first. Higher severity levels indicate
more critical issues that prevent successful SDMX data processing.

# Values
- `INFO = 1`: Informational messages about data characteristics or suggestions
- `WARNING = 2`: Potential issues that may cause problems but don't prevent processing
- `ERROR = 3`: Clear violations of SDMX standards that should be fixed
- `CRITICAL = 4`: Severe violations that prevent any further processing

# Examples
```julia
# Create validation issues with different severities
info_issue = ValidationIssue("Data completeness is 95%", INFO)
warning_issue = ValidationIssue("Column name doesn't follow convention", WARNING)
error_issue = ValidationIssue("Required column missing", ERROR)
critical_issue = ValidationIssue("Invalid data structure", CRITICAL)

# Filter issues by severity
critical_issues = filter(issue -> issue.severity == CRITICAL, all_issues)
```

# See also
[`ValidationRule`](@ref), [`ValidationResult`](@ref), [`SDMXValidator`](@ref)
"""
@enum ValidationSeverity begin
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4
end

"""
    ValidationRule

Defines a single validation rule with its criteria, evaluation logic, and optional auto-fix capabilities.

This struct encapsulates a complete validation rule that can be applied to SDMX data,
including the logic to evaluate the rule, determine severity of violations, and
optionally provide automatic fixes for common issues.

# Fields
- `rule_id::String`: Unique identifier for the validation rule
- `rule_name::String`: Human-readable name of the validation rule
- `description::String`: Detailed description of what the rule checks
- `severity::ValidationSeverity`: Severity level for violations of this rule
- `category::String`: Rule category ("structure", "content", "quality", "compliance")
- `evaluation_function::Function`: Function that evaluates the rule against data
- `auto_fix_available::Bool`: Whether automatic fixing is available for violations
- `auto_fix_function::Union{Function, Nothing}`: Function to automatically fix violations, if available

# Examples
```julia
# Create a structure validation rule
structure_rule = ValidationRule(
    "REQ_COLS_001",
    "Required Columns Present",
    "Checks that all required SDMX columns are present",
    ERROR,
    "structure",
    (data, schema) -> check_required_columns(data, schema),
    false,
    nothing
)

# Create a rule with auto-fix capability
format_rule = ValidationRule(
    "FMT_001",
    "Column Name Format",
    "Ensures column names follow SDMX conventions",
    WARNING,
    "content",
    (data, schema) -> check_column_format(data),
    true,
    (data) -> fix_column_names(data)
)

# Use in validator
validator = create_validator(schema)
add_custom_validation_rule(validator, structure_rule)
```

# See also
[`ValidationSeverity`](@ref), [`SDMXValidator`](@ref)
"""
struct ValidationRule
    rule_id::String
    rule_name::String
    description::String
    severity::ValidationSeverity
    category::String  # "structure", "content", "quality", "compliance"
    evaluation_function::Function
    auto_fix_available::Bool
    auto_fix_function::Union{Function, Nothing}
end

"""
    ValidationIssue

Represents a single validation issue found during SDMX data validation.

This struct captures detailed information about a specific validation problem,
including its severity, location, affected data, and potential solutions.

# Fields
- `rule_id::String`: Identifier of the validation rule that detected this issue
- `severity::ValidationSeverity`: Severity level of the issue (INFO, WARNING, ERROR, CRITICAL)
- `message::String`: Human-readable description of the validation issue
- `location::String`: Location where issue was found (column name, row range, etc.)
- `affected_rows::Vector{Int}`: Row numbers that contain the validation issue
- `suggested_fix::String`: Recommended action to resolve the issue
- `auto_fixable::Bool`: Whether the issue can be automatically corrected

# Examples
```julia
# Create a validation issue for missing required column
issue = ValidationIssue(
    "required_columns",
    ERROR,
    "Missing required column: TIME_PERIOD",
    "structure",
    Int[],
    "Add TIME_PERIOD column with appropriate time values",
    false
)

# Check if issue can be automatically fixed
if issue.auto_fixable
    println("This issue can be auto-fixed: ", issue.suggested_fix)
end
```

# See also
[`ValidationRule`](@ref), [`ValidationResult`](@ref), [`ValidationSeverity`](@ref)
"""
struct ValidationIssue
    rule_id::String
    severity::ValidationSeverity
    message::String
    location::String  # Column name, row range, etc.
    affected_rows::Vector{Int}
    suggested_fix::String
    auto_fixable::Bool
end

"""
    ValidationResult

Comprehensive validation results containing all validation issues, statistics, and actionable recommendations.

This struct represents the complete output of SDMX-CSV validation, providing
detailed analysis of data quality, compliance status, and performance metrics
along with specific recommendations for addressing any identified issues.

# Fields
- `dataset_name::String`: Name or identifier of the validated dataset
- `validation_timestamp::String`: ISO timestamp when validation was performed
- `target_schema::DataflowSchema`: SDMX dataflow schema used for validation
- `total_rows::Int`: Total number of rows in the validated dataset
- `total_columns::Int`: Total number of columns in the validated dataset
- `issues::Vector{ValidationIssue}`: All validation issues found, ordered by severity
- `statistics::Dict{String, Any}`: Detailed validation statistics and metrics
- `overall_score::Float64`: Overall compliance score (0.0-1.0, higher is better)
- `compliance_status::String`: Overall compliance status ("compliant", "minor_issues", "major_issues", "non_compliant")
- `recommendations::Vector{String}`: Prioritized list of actionable recommendations
- `performance_metrics::Dict{String, Float64}`: Validation performance timing and efficiency metrics

# Examples
```julia
# Use validation result
validator = create_validator(schema)
result = validate_sdmx_csv(validator, data, "my_dataset")

# Check overall compliance
println("Dataset score: ", result.overall_score * 100, "%")
println("Status: ", result.compliance_status)

# Show critical issues
critical_issues = filter(i -> i.severity == CRITICAL, result.issues)
println("Critical issues: ", length(critical_issues))

# Generate formatted report
report = generate_validation_report(result, format="text")
println(report)
```

# See also
[`validate_sdmx_csv`](@ref), [`generate_validation_report`](@ref)
"""
struct ValidationResult
    dataset_name::String
    validation_timestamp::String
    target_schema::DataflowSchema
    total_rows::Int
    total_columns::Int
    issues::Vector{ValidationIssue}
    statistics::Dict{String, Any}
    overall_score::Float64
    compliance_status::String  # "compliant", "minor_issues", "major_issues", "non_compliant"
    recommendations::Vector{String}
    performance_metrics::Dict{String, Float64}
end

"""
    create_validation_result(dataset_name::String, target_schema::DataflowSchema, 
                           total_rows::Int, total_columns::Int, issues::Vector{ValidationIssue},
                           statistics::Dict{String, Any}, overall_score::Float64,
                           compliance_status::String, recommendations::Vector{String},
                           performance_metrics::Dict{String, Float64}) -> ValidationResult

Creates a ValidationResult with comprehensive parameter validation and timestamp generation.

This constructor function validates all input parameters for correctness before
creating the ValidationResult struct, ensuring data integrity and preventing
invalid validation results from being created.

# Arguments
- `dataset_name::String`: Name identifier for the validated dataset
- `target_schema::DataflowSchema`: SDMX schema used as validation target
- `total_rows::Int`: Total number of rows (must be non-negative)
- `total_columns::Int`: Total number of columns (must be non-negative)
- `issues::Vector{ValidationIssue}`: Collection of all validation issues found
- `statistics::Dict{String, Any}`: Validation statistics and metrics
- `overall_score::Float64`: Overall compliance score (must be 0.0-1.0)
- `compliance_status::String`: Must be one of: "compliant", "minor_issues", "major_issues", "non_compliant"
- `recommendations::Vector{String}`: List of actionable recommendations
- `performance_metrics::Dict{String, Float64}`: Performance timing and efficiency data

# Returns
- `ValidationResult`: Fully validated result struct with auto-generated timestamp

# Examples
```julia
# Create validation result with proper validation
result = create_validation_result(
    "test_dataset",
    schema,
    1000,  # rows
    15,    # columns
    issues_vector,
    Dict("memory_usage_mb" => 12.5),
    0.85,  # 85% compliance score
    "minor_issues",
    ["Fix missing TIME_PERIOD values"],
    Dict("total_validation_time_ms" => 150.0)
)
```

# Throws
- `ArgumentError`: If overall_score is outside 0.0-1.0 range
- `ArgumentError`: If compliance_status is not a valid status
- `ArgumentError`: If total_rows or total_columns are negative

# See also
[`ValidationResult`](@ref), [`validate_score_range`](@ref), [`validate_compliance_status`](@ref)
"""
function create_validation_result(dataset_name::String, target_schema::DataflowSchema, 
                                 total_rows::Int, total_columns::Int, issues::Vector{ValidationIssue},
                                 statistics::Dict{String, Any}, overall_score::Float64,
                                 compliance_status::String, recommendations::Vector{String},
                                 performance_metrics::Dict{String, Float64})
    # Functional validation
    validate_score_range(overall_score)
    validate_compliance_status(compliance_status)
    validate_row_column_counts(total_rows, total_columns)
    
    return ValidationResult(
        dataset_name, 
        string(now()),  # Generate timestamp
        target_schema,
        total_rows, 
        total_columns,
        issues, 
        statistics, 
        overall_score,
        compliance_status, 
        recommendations, 
        performance_metrics
    )
end

"""
    validate_score_range(score::Float64) -> Nothing

Validates that the overall compliance score is within the valid range of 0.0 to 1.0.

This validation function ensures that compliance scores are meaningful and can be
interpreted as percentages (0% to 100% compliance). Throws an error for invalid scores.

# Arguments
- `score::Float64`: The compliance score to validate

# Returns
- `Nothing`: Function returns nothing on successful validation

# Examples
```julia
# Valid scores pass silently
validate_score_range(0.75)  # OK - 75% compliance
validate_score_range(1.0)   # OK - 100% compliance
validate_score_range(0.0)   # OK - 0% compliance

# Invalid scores throw errors
try
    validate_score_range(1.5)  # Error: > 1.0
catch e
    println("Invalid score: ", e)
end
```

# Throws
- `ArgumentError`: If score is less than 0.0 or greater than 1.0

# See also
[`create_validation_result`](@ref), [`validate_compliance_status`](@ref)
"""
function validate_score_range(score::Float64)
    0.0 <= score <= 1.0 || throw(ArgumentError("overall_score must be between 0.0 and 1.0, got $score"))
end

"""
    validate_compliance_status(status::String) -> Nothing

Validates that the compliance status string is one of the four allowed SDMX compliance levels.

Ensures that only valid compliance status values are used, maintaining consistency
across all validation results and enabling proper categorization of datasets.

# Arguments
- `status::String`: The compliance status to validate

# Returns
- `Nothing`: Function returns nothing on successful validation

# Examples
```julia
# Valid statuses pass silently
validate_compliance_status("compliant")      # OK - no issues
validate_compliance_status("minor_issues")   # OK - few warnings
validate_compliance_status("major_issues")   # OK - many errors
validate_compliance_status("non_compliant")  # OK - critical failures

# Invalid statuses throw errors
try
    validate_compliance_status("partially_ok")  # Error: not allowed
catch e
    println("Invalid status: ", e)
end
```

# Throws
- `ArgumentError`: If status is not one of: "compliant", "minor_issues", "major_issues", "non_compliant"

# See also
[`create_validation_result`](@ref), [`validate_score_range`](@ref)
"""
function validate_compliance_status(status::String)
    allowed_statuses = ["compliant", "minor_issues", "major_issues", "non_compliant"]
    status in allowed_statuses || throw(ArgumentError("Invalid compliance_status: $status. Must be one of: $(join(allowed_statuses, ", "))"))
end

"""
    validate_row_column_counts(rows::Int, columns::Int) -> Nothing

Validates that dataset row and column counts are non-negative integers.

Ensures that dataset dimensions are logical and meaningful, preventing
negative counts that would indicate data processing errors or invalid inputs.

# Arguments
- `rows::Int`: Number of rows in the dataset
- `columns::Int`: Number of columns in the dataset

# Returns
- `Nothing`: Function returns nothing on successful validation

# Examples
```julia
# Valid counts pass silently
validate_row_column_counts(1000, 15)  # OK - normal dataset
validate_row_column_counts(0, 0)      # OK - empty dataset
validate_row_column_counts(1, 1)      # OK - minimal dataset

# Invalid counts throw errors
try
    validate_row_column_counts(-1, 5)  # Error: negative rows
catch e
    println("Invalid row count: ", e)
end
```

# Throws
- `ArgumentError`: If rows is negative
- `ArgumentError`: If columns is negative

# See also
[`create_validation_result`](@ref), [`ValidationResult`](@ref)
"""
function validate_row_column_counts(rows::Int, columns::Int)
    rows >= 0 || throw(ArgumentError("total_rows must be non-negative, got $rows"))
    columns >= 0 || throw(ArgumentError("total_columns must be non-negative, got $columns"))
end


"""
    SDMXValidator

Main validation engine with configurable rules and settings for comprehensive SDMX data validation.

This mutable struct serves as the central validation engine, containing all the rules,
configuration settings, and schema information needed to validate SDMX-CSV datasets.
It supports both strict and performance modes, custom thresholds, and auto-fixing capabilities.

# Fields
- `schema::DataflowSchema`: The target SDMX dataflow schema for validation
- `validation_rules::Dict{String, ValidationRule}`: Collection of validation rules indexed by rule ID
- `strict_mode::Bool`: If true, applies stricter validation criteria with lower tolerance for issues
- `performance_mode::Bool`: If true, skips expensive validation checks for large datasets
- `custom_thresholds::Dict{String, Float64}`: Custom threshold values for validation rules
- `auto_fix_enabled::Bool`: Whether automatic issue fixing is enabled

# Examples
```julia
# Create validator with default settings
validator = create_validator(schema)

# Validate a dataset
result = validator(data, "my_dataset")

# Create validator with custom settings
validator = create_validator(
    schema, 
    strict_mode=true, 
    performance_mode=false,
    auto_fix_enabled=true
)

# Add custom validation rules
custom_rule = ValidationRule(
    "custom_001", "Custom Check", "My custom validation",
    WARNING, "quality", my_validation_function, false, nothing
)
add_custom_validation_rule(validator, custom_rule)
```

# See also
[`create_validator`](@ref), [`validate_sdmx_csv`](@ref), [`ValidationRule`](@ref)
"""
mutable struct SDMXValidator
    schema::DataflowSchema
    validation_rules::Dict{String, ValidationRule}
    strict_mode::Bool
    performance_mode::Bool  # Skip expensive checks for large datasets
    custom_thresholds::Dict{String, Float64}
    auto_fix_enabled::Bool
end

# =================== CALLABLE STRUCT INTERFACE ===================

"""
    (validator::SDMXValidator)(data::DataFrame, dataset_name::String="dataset") -> ValidationResult
    (validator::SDMXValidator)(data::AbstractDataFrame, dataset_name::String="dataset") -> ValidationResult

Make SDMXValidator callable as a function. This allows for intuitive usage:

# Examples
```julia
validator = create_validator(schema)
result = validator(my_data, "my_dataset")  # Instead of validate_sdmx_csv(validator, my_data, "my_dataset")

# Works with any AbstractDataFrame
result = validator(my_dataframe)  # Uses default name "dataset"
```
"""
function (validator::SDMXValidator)(data::AbstractDataFrame, dataset_name::String="dataset")
    return validate_sdmx_csv(validator, DataFrame(data), dataset_name)
end

# Specialized method for DataFrame (no conversion needed)
function (validator::SDMXValidator)(data::DataFrame, dataset_name::String="dataset")
    return validate_sdmx_csv(validator, data, dataset_name)
end

# Note: DataSource method moved to SDMXLLM.jl package

"""
    create_validator(schema::DataflowSchema; 
                    strict_mode=false, 
                    performance_mode=false,
                    auto_fix_enabled=true) -> SDMXValidator

Creates a comprehensive SDMX validator with default validation rules and configurable behavior.

This constructor function creates a fully configured SDMXValidator instance loaded with
all standard SDMX validation rules. The validator can be customized with different
modes and settings to suit specific validation requirements.

# Arguments
- `schema::DataflowSchema`: The target SDMX dataflow schema that defines validation requirements

# Keyword Arguments  
- `strict_mode=false`: Enable strict validation with lower tolerance for issues
- `performance_mode=false`: Enable performance mode that skips expensive checks for large datasets
- `auto_fix_enabled=true`: Enable automatic fixing of correctable validation issues

# Returns
- `SDMXValidator`: Configured validator ready for dataset validation

# Examples
```julia
# Create basic validator
validator = create_validator(schema)
result = validator(data)

# Create strict validator for critical data
strict_validator = create_validator(
    schema,
    strict_mode=true,
    performance_mode=false,
    auto_fix_enabled=false
)

# Create performance-optimized validator for large datasets
fast_validator = create_validator(
    schema,
    strict_mode=false,
    performance_mode=true,
    auto_fix_enabled=true
)
```

# See also
[`SDMXValidator`](@ref), [`validate_sdmx_csv`](@ref)
"""
function create_validator(schema::DataflowSchema; 
                         strict_mode=false, 
                         performance_mode=false,
                         auto_fix_enabled=true)
    
    validator = SDMXValidator(
        schema,
        Dict{String, ValidationRule}(),
        strict_mode,
        performance_mode,
        Dict{String, Float64}(
            "missing_value_threshold" => 0.05,  # 5% missing values threshold
            "outlier_threshold" => 3.0,         # 3 standard deviations
            "uniqueness_threshold" => 0.95      # 95% uniqueness for categorical
        ),
        auto_fix_enabled
    )
    
    # Load default validation rules
    load_default_validation_rules!(validator)
    
    return validator
end

"""
    load_default_validation_rules!(validator::SDMXValidator) -> Nothing

Loads comprehensive default validation rules into the validator for complete SDMX compliance checking.

This function populates the validator with all standard SDMX validation rules covering
structure, content, quality, and compliance validation. Rules are categorized by type
and include both mandatory requirements and best practices.

# Arguments
- `validator::SDMXValidator`: The validator instance to populate with rules

# Returns
- `Nothing`: Function modifies the validator in-place

# Examples
```julia
# Create empty validator and load default rules
validator = SDMXValidator(schema, Dict(), false, false, Dict(), true)
load_default_validation_rules!(validator)

# Validator now contains all standard rules
println("Loaded ", length(validator.validation_rules), " validation rules")

# Standard rule categories loaded:
# - Structure: required columns, data types, naming conventions
# - Content: codelist compliance, time formats, observation values
# - Quality: missing values, duplicates, outlier detection
# - Compliance: SDMX-CSV format requirements
```

# Rule Categories
The function loads rules in these categories:
- **Structure**: Column presence, data types, naming conventions
- **Content**: Codelist compliance, time period formats, observation value validity
- **Quality**: Missing value analysis, duplicate detection, outlier identification
- **Compliance**: SDMX-CSV format adherence

# See also
[`create_validator`](@ref), [`ValidationRule`](@ref)
"""
function load_default_validation_rules!(validator::SDMXValidator)
    schema = validator.schema
    
    # === STRUCTURE VALIDATION RULES ===
    
    # Required columns validation
    validator.validation_rules["required_columns"] = ValidationRule(
        "required_columns",
        "Required Columns Present",
        "All required SDMX columns must be present in the dataset",
        ERROR,
        "structure",
        data -> validate_required_columns(data, schema),
        false,
        nothing
    )
    
    # Column data types validation
    validator.validation_rules["column_types"] = ValidationRule(
        "column_types",
        "Column Data Types",
        "Columns must have appropriate data types for their SDMX role",
        ERROR,
        "structure",
        data -> validate_column_types(data, schema),
        true,
        data -> fix_column_types(data, schema)
    )
    
    # Column naming validation
    validator.validation_rules["column_names"] = ValidationRule(
        "column_names",
        "SDMX Column Naming",
        "Column names must follow SDMX conventions",
        WARNING,
        "structure",
        data -> validate_column_names(data, schema),
        true,
        data -> fix_column_names(data, schema)
    )
    
    # === CONTENT VALIDATION RULES ===
    
    # Codelist compliance
    validator.validation_rules["codelist_compliance"] = ValidationRule(
        "codelist_compliance",
        "Codelist Value Compliance",
        "Values must match valid codelist codes where applicable",
        ERROR,
        "content",
        data -> validate_codelist_compliance(data, schema),
        true,
        data -> fix_codelist_values(data, schema)
    )
    
    # Time period format
    validator.validation_rules["time_format"] = ValidationRule(
        "time_format",
        "Time Period Format",
        "TIME_PERIOD values must follow ISO 8601 or SDMX time format",
        ERROR,
        "content",
        data -> validate_time_format(data, schema),
        true,
        data -> fix_time_format(data, schema)
    )
    
    # Observation value validation
    validator.validation_rules["obs_value"] = ValidationRule(
        "obs_value",
        "Observation Value Validity",
        "OBS_VALUE must be numeric and within reasonable ranges",
        ERROR,
        "content",
        data -> validate_obs_values(data, schema),
        true,
        data -> fix_obs_values(data, schema)
    )
    
    # === QUALITY VALIDATION RULES ===
    
    # Missing value assessment
    validator.validation_rules["missing_values"] = ValidationRule(
        "missing_values",
        "Missing Value Analysis",
        "Excessive missing values may indicate data quality issues",
        WARNING,
        "quality",
        data -> validate_missing_values(data, validator.custom_thresholds["missing_value_threshold"]),
        false,
        nothing
    )
    
    # Duplicate row detection
    validator.validation_rules["duplicates"] = ValidationRule(
        "duplicates",
        "Duplicate Row Detection",
        "Dataset should not contain duplicate observations",
        WARNING,
        "quality",
        data -> validate_duplicates(data, schema),
        true,
        data -> remove_duplicates(data, schema)
    )
    
    # Outlier detection
    validator.validation_rules["outliers"] = ValidationRule(
        "outliers",
        "Statistical Outlier Detection",
        "Identifies potential outliers in numeric observations",
        INFO,
        "quality",
        data -> detect_outliers(data, validator.custom_thresholds["outlier_threshold"]),
        false,
        nothing
    )
    
    # === COMPLIANCE VALIDATION RULES ===
    
    # SDMX-CSV format compliance
    validator.validation_rules["sdmx_csv_format"] = ValidationRule(
        "sdmx_csv_format",
        "SDMX-CSV Format Compliance",
        "Dataset must conform to SDMX-CSV format requirements",
        CRITICAL,
        "compliance",
        data -> validate_sdmx_csv_format(data, schema),
        false,
        nothing
    )
end

"""
    validate_sdmx_csv(validator::SDMXValidator, 
                      data::DataFrame,
                      dataset_name::String = "unknown") -> ValidationResult

Performs comprehensive validation of SDMX-CSV data against all configured validation rules.

This is the main validation function that executes all validation rules in the validator,
collects issues, calculates compliance metrics, generates recommendations, and produces
a complete validation report with performance statistics.

# Arguments
- `validator::SDMXValidator`: Configured validator containing rules and schema
- `data::DataFrame`: The SDMX-CSV dataset to validate
- `dataset_name::String = "unknown"`: Name identifier for the dataset in reports

# Returns
- `ValidationResult`: Comprehensive validation results including issues, scores, and recommendations

# Examples
```julia
# Basic validation
validator = create_validator(schema)
result = validate_sdmx_csv(validator, data, "my_dataset")

# Check results
println("Overall score: ", result.overall_score * 100, "%")
println("Status: ", result.compliance_status)
println("Issues found: ", length(result.issues))

# Generate report
report = generate_validation_report(result)
println(report)

# Handle validation issues
critical_issues = filter(i -> i.severity == CRITICAL, result.issues)
if !isempty(critical_issues)
    println("CRITICAL: Dataset cannot be published")
end
```

# Performance Notes
- Validation time scales with dataset size and number of enabled rules
- Performance mode skips expensive checks for large datasets
- Rule execution is timed individually for performance analysis

# See also
[`SDMXValidator`](@ref), [`ValidationResult`](@ref), [`generate_validation_report`](@ref)
"""
function validate_sdmx_csv(validator::SDMXValidator, 
                          data::DataFrame,
                          dataset_name::String = "unknown")
    
    start_time = time()
    issues = Vector{ValidationIssue}()
    statistics = Dict{String, Any}()
    
    # Basic dataset statistics
    statistics["row_count"] = nrow(data)
    statistics["column_count"] = ncol(data)
    statistics["memory_usage_mb"] = Base.summarysize(data) / (1024^2)
    
    # Run all validation rules
    for (rule_id, rule) in validator.validation_rules
        rule_start = time()
        
        try
            rule_issues = rule.evaluation_function(data)
            append!(issues, rule_issues)
            
            # Track rule performance
            rule_time = time() - rule_start
            statistics["$(rule_id)_time_ms"] = round(rule_time * 1000, digits=2)
            
        catch e
            # Create an issue for failed validation rules
            push!(issues, ValidationIssue(
                rule_id,
                ERROR,
                "Validation rule failed: $e",
                "system",
                Int[],
                "Review validation rule implementation",
                false
            ))
        end
    end
    
    # Calculate overall score and compliance status
    overall_score, compliance_status = calculate_compliance_metrics(issues, nrow(data))
    
    # Generate recommendations
    recommendations = generate_recommendations(issues, validator.schema)
    
    # Performance metrics
    total_time = time() - start_time
    performance_metrics = Dict{String, Float64}(
        "total_validation_time_ms" => round(total_time * 1000, digits=2),
        "rows_per_second" => nrow(data) / total_time,
        "issues_per_1000_rows" => (length(issues) / nrow(data)) * 1000
    )
    
    return create_validation_result(
        dataset_name,
        validator.schema,
        nrow(data),
        ncol(data),
        issues,
        statistics,
        overall_score,
        compliance_status,
        recommendations,
        performance_metrics
    )
end

# === SPECIFIC VALIDATION FUNCTIONS ===

"""
    validate_required_columns(data::DataFrame, schema::DataflowSchema) -> Vector{ValidationIssue}

Validates that all required columns are present.
"""
function validate_required_columns(data::DataFrame, schema::DataflowSchema)
    issues = Vector{ValidationIssue}()
    required_cols = get_required_columns(schema)
    data_cols = Set(names(data))
    
    missing_cols = setdiff(Set(required_cols), data_cols)
    
    if !isempty(missing_cols)
        separator = ", "
        push!(issues, ValidationIssue(
            "required_columns",
            ERROR,
            "Missing required columns: $(join(missing_cols, separator))",
            "structure",
            Int[],
            "Add missing columns: $(join(missing_cols, separator))",
            false
        ))
    end
    
    return issues
end

"""
    validate_column_types(data::DataFrame, schema::DataflowSchema) -> Vector{ValidationIssue}

Validates column data types match expected types.
"""
function validate_column_types(data::DataFrame, schema::DataflowSchema)
    issues = Vector{ValidationIssue}()
    
    # Check TIME_PERIOD should be string or date-like
    if hasproperty(data, :TIME_PERIOD)
        if !(eltype(data.TIME_PERIOD) <: Union{String, Missing, Date, DateTime})
            push!(issues, ValidationIssue(
                "column_types",
                ERROR,
                "TIME_PERIOD column should be String, Date, or DateTime type",
                "TIME_PERIOD",
                Int[],
                "Convert TIME_PERIOD to appropriate time format",
                true
            ))
        end
    end
    
    # Check OBS_VALUE should be numeric
    if hasproperty(data, :OBS_VALUE)
        if !(eltype(data.OBS_VALUE) <: Union{Number, Missing})
            push!(issues, ValidationIssue(
                "column_types",
                ERROR,
                "OBS_VALUE column must be numeric",
                "OBS_VALUE",
                Int[],
                "Convert OBS_VALUE to Float64 type",
                true
            ))
        end
    end
    
    return issues
end

"""
    validate_codelist_compliance(data::DataFrame, schema::DataflowSchema) -> Vector{ValidationIssue}

Validates values against codelist constraints.
"""
function validate_codelist_compliance(data::DataFrame, schema::DataflowSchema)
    issues = Vector{ValidationIssue}()
    codelist_cols = get_codelist_columns(schema)
    
    for (col_name, codelist_id) in codelist_cols
        if hasproperty(data, Symbol(col_name))
            col_data = data[!, col_name]
            unique_values = unique(skipmissing(col_data))
            
            # For now, we'll just check if values look like valid codes
            # In a full implementation, we'd fetch the actual codelist
            invalid_values = String[]
            for val in unique_values
                if isa(val, String)
                    # Basic validation - codes shouldn't be empty or too long
                    if isempty(val) || length(val) > 50
                        push!(invalid_values, val)
                    end
                else
                    push!(invalid_values, string(val))
                end
            end
            
            if !isempty(invalid_values)
                invalid_rows = findall(in(invalid_values), col_data)
                separator = ", "
                sample_values = invalid_values[1:min(5, length(invalid_values))]
                push!(issues, ValidationIssue(
                    "codelist_compliance",
                    ERROR,
                    "Invalid values in $col_name: $(join(sample_values, separator))",
                    col_name,
                    invalid_rows,
                    "Check values against codelist $codelist_id",
                    true
                ))
            end
        end
    end
    
    return issues
end

"""
    validate_time_format(data::DataFrame, schema::DataflowSchema) -> Vector{ValidationIssue}

Validates TIME_PERIOD format compliance.
"""
function validate_time_format(data::DataFrame, schema::DataflowSchema)
    issues = Vector{ValidationIssue}()
    
    if !hasproperty(data, :TIME_PERIOD)
        return issues
    end
    
    time_col = data.TIME_PERIOD
    invalid_rows = Int[]
    
    for (i, val) in enumerate(time_col)
        if ismissing(val)
            continue
        end
        
        val_str = string(val)
        
        # Check for common time formats
        if !is_valid_time_format(val_str)
            push!(invalid_rows, i)
        end
    end
    
    if !isempty(invalid_rows)
        push!(issues, ValidationIssue(
            "time_format",
            ERROR,
            "Invalid TIME_PERIOD format in $(length(invalid_rows)) rows",
            "TIME_PERIOD",
            invalid_rows[1:min(10, length(invalid_rows))],  # Show first 10 rows
            "Use format YYYY, YYYY-MM, YYYY-QN, etc.",
            true
        ))
    end
    
    return issues
end

"""
    is_valid_time_format(time_str::String) -> Bool

Checks if a time string follows valid SDMX time formats.
"""
function is_valid_time_format(time_str::String)
    # Check for year only format (YYYY)
    if occursin(r"^\d{4}$", time_str)
        year = tryparse(Int, time_str)
        return year !== nothing && year >= 1900 && year <= 2100  # Reasonable year range
    end
    
    # Check for year-month format (YYYY-MM) using Julia's Date parsing
    if occursin(r"^\d{4}-\d{2}$", time_str)
        return tryparse(Date, time_str, DateFormat("yyyy-mm")) !== nothing
    end
    
    # Check for year-quarter format (YYYY-Q[1-4])
    if occursin(r"^\d{4}-Q[1-4]$", time_str)
        return true  # Regex already validates Q1-Q4
    end
    
    # Check for year-month-day format (YYYY-MM-DD) using Julia's Date parsing
    # This properly handles leap years and month-specific day counts
    if occursin(r"^\d{4}-\d{2}-\d{2}$", time_str)
        return tryparse(Date, time_str, DateFormat("yyyy-mm-dd")) !== nothing
    end
    
    # Check for year-week format (YYYY-Www)
    if occursin(r"^\d{4}-W\d{2}$", time_str)
        week_str = time_str[7:8]
        week = tryparse(Int, week_str)
        return week !== nothing && week >= 1 && week <= 53
    end
    
    # Check for datetime formats (YYYY-MM-DDTHH:MM:SS)
    if occursin(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", time_str)
        # Try parsing as DateTime
        return tryparse(DateTime, time_str[1:19], DateFormat("yyyy-mm-ddTHH:MM:SS")) !== nothing
    end
    
    return false
end

"""
    validate_obs_values(data::DataFrame, schema::DataflowSchema) -> Vector{ValidationIssue}

Validates observation values for reasonableness.
"""
function validate_obs_values(data::DataFrame, schema::DataflowSchema)
    issues = Vector{ValidationIssue}()
    
    if !hasproperty(data, :OBS_VALUE)
        return issues
    end
    
    obs_values = data.OBS_VALUE
    numeric_values = filter(!ismissing, obs_values)
    
    if isempty(numeric_values)
        push!(issues, ValidationIssue(
            "obs_value",
            WARNING,
            "No valid numeric observations found",
            "OBS_VALUE",
            Int[],
            "Ensure OBS_VALUE contains numeric data",
            false
        ))
        return issues
    end
    
    # Check for extreme values
    if any(isinf.(numeric_values)) || any(isnan.(numeric_values))
        inf_rows = findall(x -> !ismissing(x) && (isinf(x) || isnan(x)), obs_values)
        push!(issues, ValidationIssue(
            "obs_value",
            ERROR,
            "Invalid numeric values (Inf/NaN) found",
            "OBS_VALUE",
            inf_rows,
            "Replace Inf/NaN values with missing or valid numbers",
            true
        ))
    end
    
    return issues
end

"""
    validate_missing_values(data::DataFrame, threshold::Float64) -> Vector{ValidationIssue}

Validates missing value patterns and thresholds.
"""
function validate_missing_values(data::DataFrame, threshold::Float64)
    issues = Vector{ValidationIssue}()
    
    for col_name in names(data)
        col_data = data[!, col_name]
        missing_count = sum(ismissing.(col_data))
        missing_rate = missing_count / length(col_data)
        
        if missing_rate > threshold
            push!(issues, ValidationIssue(
                "missing_values",
                WARNING,
                "$col_name has $(round(missing_rate*100, digits=1))% missing values",
                col_name,
                Int[],
                "Consider data imputation or investigate data collection",
                false
            ))
        end
    end
    
    return issues
end

"""
    validate_duplicates(data::DataFrame, schema::DataflowSchema) -> Vector{ValidationIssue}

Detects duplicate rows in the dataset.
"""
function validate_duplicates(data::DataFrame, schema::DataflowSchema)
    issues = Vector{ValidationIssue}()
    
    # Get key columns for duplicate detection
    key_cols = get_required_columns(schema)
    available_key_cols = intersect(key_cols, names(data))
    
    if length(available_key_cols) < 2
        return issues  # Can't meaningfully check for duplicates
    end
    
    # Check for duplicates based on key columns
    grouped = groupby(data, available_key_cols)
    duplicate_groups = filter(g -> nrow(g) > 1, grouped)
    
    if !isempty(duplicate_groups)
        total_duplicates = sum(nrow(g) - 1 for g in duplicate_groups)
        duplicate_rows = vcat([collect(g.id)[2:end] for g in duplicate_groups]...)  # Skip first occurrence
        
        push!(issues, ValidationIssue(
            "duplicates",
            WARNING,
            "$total_duplicates duplicate rows detected",
            "dataset",
            duplicate_rows[1:min(10, length(duplicate_rows))],
            "Remove or consolidate duplicate observations",
            true
        ))
    end
    
    return issues
end

"""
    calculate_compliance_metrics(issues::Vector{ValidationIssue}, row_count::Int) -> Tuple{Float64, String}

Calculates overall compliance score and status.
"""
function calculate_compliance_metrics(issues::Vector{ValidationIssue}, row_count::Int)
    if isempty(issues)
        return 1.0, "compliant"
    end
    
    # Weight issues by severity
    severity_weights = Dict(
        CRITICAL => 10.0,
        ERROR => 5.0,
        WARNING => 2.0,
        INFO => 0.5
    )
    
    total_weight = sum(severity_weights[issue.severity] for issue in issues)
    
    # Normalize by dataset size (more forgiving for larger datasets)
    size_factor = log10(max(100, row_count)) / 2  # Logarithmic scaling
    penalty = total_weight / size_factor
    
    # Calculate score (0-1, higher is better)
    score = max(0.0, 1.0 - (penalty / 100.0))
    
    # Determine compliance status
    critical_count = sum(issue.severity == CRITICAL for issue in issues)
    error_count = sum(issue.severity == ERROR for issue in issues)
    warning_count = sum(issue.severity == WARNING for issue in issues)
    
    status = if critical_count > 0
        "non_compliant"
    elseif error_count > 5
        "major_issues"
    elseif error_count > 0 || warning_count > 10
        "minor_issues"
    else
        "compliant"
    end
    
    return score, status
end

"""
    generate_recommendations(issues::Vector{ValidationIssue}, schema::DataflowSchema) -> Vector{String}

Generates actionable recommendations based on validation issues.
"""
function generate_recommendations(issues::Vector{ValidationIssue}, schema::DataflowSchema)
    recommendations = String[]
    
    if isempty(issues)
        push!(recommendations, "Dataset is fully compliant with SDMX-CSV requirements")
        return recommendations
    end
    
    # Group issues by category
    issue_categories = Dict{String, Int}()
    for issue in issues
        category = get_rule_category(issue.rule_id)
        issue_categories[category] = get(issue_categories, category, 0) + 1
    end
    
    # Priority recommendations based on issue patterns
    if get(issue_categories, "structure", 0) > 0
        push!(recommendations, "Address structural issues first - ensure all required columns are present with correct data types")
    end
    
    if get(issue_categories, "content", 0) > 0
        push!(recommendations, "Validate data content against SDMX codelists and format requirements")
    end
    
    critical_count = sum(issue.severity == CRITICAL for issue in issues)
    if critical_count > 0
        push!(recommendations, "URGENT: $critical_count critical issues must be resolved before data can be published")
    end
    
    auto_fixable = sum(issue.auto_fixable for issue in issues)
    if auto_fixable > 0
        push!(recommendations, "$auto_fixable issues can be automatically fixed - consider using auto-fix functionality")
    end
    
    # Data quality recommendations
    if get(issue_categories, "quality", 0) > 3
        push!(recommendations, "Consider implementing data quality checks in your data collection process")
    end
    
    return recommendations
end

"""
    get_rule_category(rule_id::String) -> String

Maps rule IDs to their categories.
"""
function get_rule_category(rule_id::String)
    category_map = Dict(
        "required_columns" => "structure",
        "column_types" => "structure", 
        "column_names" => "structure",
        "codelist_compliance" => "content",
        "time_format" => "content",
        "obs_value" => "content",
        "missing_values" => "quality",
        "duplicates" => "quality",
        "outliers" => "quality",
        "sdmx_csv_format" => "compliance"
    )
    
    return get(category_map, rule_id, "unknown")
end

"""
    generate_validation_report(result::ValidationResult;
                              format::String = "text",
                              include_details::Bool = true) -> String

Generates a formatted validation report.

# See also
[`validate_sdmx_csv`](@ref), [`ValidationResult`](@ref)
"""
function generate_validation_report(result::ValidationResult; 
                                   format::String = "text",
                                   include_details::Bool = true)
    
    if format == "text"
        return generate_text_report(result, include_details)
    elseif format == "json"
        return generate_json_report(result)
    elseif format == "html"
        return generate_html_report(result, include_details)
    else
        error("Unsupported report format: $format")
    end
end

"""
    generate_text_report(result::ValidationResult, include_details::Bool) -> String

Generates a text-based validation report.
"""
function generate_text_report(result::ValidationResult, include_details::Bool)
    report = """
═══════════════════════════════════════════════════════════════
                    SDMX-CSV VALIDATION REPORT
═══════════════════════════════════════════════════════════════

Dataset: $(result.dataset_name)
Validation Date: $(result.validation_timestamp)
Target Schema: $(result.target_schema.dataflow_info.agency):$(result.target_schema.dataflow_info.id)

═══ SUMMARY ═══
Rows: $(result.total_rows)
Columns: $(result.total_columns)
Overall Score: $(round(result.overall_score * 100, digits=1))%
Compliance Status: $(uppercase(replace(result.compliance_status, "_" => " ")))

═══ ISSUES SUMMARY ═══
Total Issues: $(length(result.issues))
"""
    
    # Count issues by severity
    severity_counts = Dict{ValidationSeverity, Int}()
    for issue in result.issues
        severity_counts[issue.severity] = get(severity_counts, issue.severity, 0) + 1
    end
    
    for severity in [CRITICAL, ERROR, WARNING, INFO]
        count = get(severity_counts, severity, 0)
        if count > 0
            report *= "$(severity): $count\n"
        end
    end
    
    # Performance metrics
    report *= "\n═══ PERFORMANCE ═══\n"
    report *= "Validation Time: $(result.performance_metrics["total_validation_time_ms"]) ms\n"
    report *= "Processing Speed: $(round(result.performance_metrics["rows_per_second"], digits=1)) rows/sec\n"
    
    # Recommendations
    if !isempty(result.recommendations)
        report *= "\n═══ RECOMMENDATIONS ═══\n"
        for (i, rec) in enumerate(result.recommendations)
            report *= "$i. $rec\n"
        end
    end
    
    # Detailed issues
    if include_details && !isempty(result.issues)
        report *= "\n═══ DETAILED ISSUES ═══\n"
        
        for (i, issue) in enumerate(result.issues)
            severity_symbol = issue.severity == CRITICAL ? "🔴" :
                            issue.severity == ERROR ? "⚠️" :
                            issue.severity == WARNING ? "⚡" : "ℹ️"
            
            report *= "\n$i. $severity_symbol $(issue.severity) - $(issue.message)\n"
            report *= "   Location: $(issue.location)\n"
            
            if !isempty(issue.affected_rows)
                row_sample = issue.affected_rows[1:min(5, length(issue.affected_rows))]
                separator = ", "
                report *= "   Affected Rows: $(join(row_sample, separator))"
                if length(issue.affected_rows) > 5
                    report *= " (and $(length(issue.affected_rows) - 5) more)"
                end
                report *= "\n"
            end
            
            if !isempty(issue.suggested_fix)
                report *= "   Suggested Fix: $(issue.suggested_fix)\n"
            end
            
            if issue.auto_fixable
                report *= "   ✅ Auto-fix available\n"
            end
        end
    end
    
    report *= "\n═══════════════════════════════════════════════════════════════\n"
    
    return report
end

"""
    preview_validation_output(result::ValidationResult; max_issues::Int = 10) -> String

Creates a concise preview of validation results.
"""
function preview_validation_output(result::ValidationResult; max_issues::Int = 10)
    preview = """
=== VALIDATION PREVIEW ===
Dataset: $(result.dataset_name)
Score: $(round(result.overall_score * 100, digits=1))% | Status: $(result.compliance_status)
Issues: $(length(result.issues)) total
"""
    
    if !isempty(result.issues)
        preview *= "\nTop Issues:\n"
        sorted_issues = sort(result.issues, by = x -> (x.severity, length(x.affected_rows)), rev=true)
        
        for (i, issue) in enumerate(sorted_issues[1:min(max_issues, length(sorted_issues))])
            severity_symbol = issue.severity == CRITICAL ? "🔴" : 
                            issue.severity == ERROR ? "⚠️" : 
                            issue.severity == WARNING ? "⚡" : "ℹ️"
            
            preview *= "$i. $severity_symbol $(issue.message) ($(issue.location))\n"
        end
        
        if length(result.issues) > max_issues
            preview *= "... and $(length(result.issues) - max_issues) more issues\n"
        end
    else
        preview *= "\n✅ No issues found - dataset is fully compliant!\n"
    end
    
    return preview
end