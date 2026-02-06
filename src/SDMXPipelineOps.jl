"""
Julia-idiomatic Pipeline Operators for SDMXer.jl

This module defines custom operators and pipeline functions for smooth workflow chaining,
leveraging Julia's operator overloading and method dispatch capabilities.
"""

using DataFrames
using Base.Threads


# =================== CUSTOM OPERATORS ===================

import Base: ⊆, |>

"""
    ⊆(data::DataFrame, schema::DataflowSchema) -> Bool

Schema compliance operator using subset symbol.

Returns true if data structure is a subset/compliant with schema requirements.
This operator provides an intuitive way to check if a DataFrame conforms to
the structure defined by an SDMX dataflow schema.

# Arguments
- `data::DataFrame`: The DataFrame to validate against the schema
- `schema::DataflowSchema`: The SDMX dataflow schema to check compliance against

# Returns
- `Bool`: true if data structure complies with schema requirements, false otherwise

# Examples
```julia
is_compliant = my_data ⊆ schema
if my_data ⊆ schema
    println("Data is schema compliant!")
end
```

# See also
[`⇒`](@ref), [`validate_with`](@ref)
"""
function ⊆(data::DataFrame, schema::DataflowSchema)
    required_cols = get_required_columns(schema)
    return all(col -> col in names(data), required_cols)
end

"""
    ⇒(data::DataFrame, validator::SDMXValidator) -> ValidationResult

Data flow operator for direct DataFrame validation.

Provides a pipeline-friendly operator for applying SDMX validation to DataFrames.
The arrow symbol suggests the flow of data through the validation process.

# Arguments
- `data::DataFrame`: The DataFrame to validate
- `validator::SDMXValidator`: The validator to apply to the data

# Returns
- `ValidationResult`: The result of the validation process

# Examples
```julia
result = my_data ⇒ validator
if (my_data ⇒ validator).is_valid
    println("Validation passed!")
end
```

# See also
[`⊆`](@ref), [`validate_with`](@ref)
"""
function ⇒(data::DataFrame, validator::SDMXValidator)
    return validator(data)
end

# =================== PIPELINE FUNCTIONS ===================

"""
    validate_with(schema::DataflowSchema; kwargs...) -> Function

Creates a validation function for use in data processing pipelines.

This function returns a closure that can be used with Julia's pipe operator
to validate DataFrames against SDMX schemas in a functional programming style.

# Arguments
- `schema::DataflowSchema`: The SDMX dataflow schema to validate against
- `kwargs...`: Additional keyword arguments passed to the validator

# Returns
- `Function`: A function that takes a DataFrame and returns a ValidationResult

# Examples
```julia
result = my_data |> validate_with(schema; strict_mode=true)
validator_func = validate_with(schema; performance_mode=true)
result = validator_func(my_data)
```

# See also
[`⇒`](@ref), [`create_validator`](@ref)
"""
function validate_with(schema::DataflowSchema; kwargs...)
    validator = create_validator(schema; kwargs...)
    return data -> validator(data)
end

# Note: profile_with function moved to SDMXLLM.jl package
# Use SDMXLLM.profile_source_data for data profiling functionality

# =================== WORKFLOW CHAINING FUNCTIONS ===================

"""
    chain(operations...) -> Function

Creates a composable chain of operations for data processing workflows.

This function creates a single function that applies a sequence of operations
in order, passing the result of each operation to the next. It's useful for
building complex data transformation pipelines.

# Arguments
- `operations...`: Variable number of functions to chain together

# Returns
- `Function`: A function that applies all operations sequentially to input data

# Examples
```julia
processor = chain(
    validate_with(schema),
    profile_with("my_data.csv"),
    data -> transform_data(data)
)

result = my_data |> processor

# More complex example
analysis_pipeline = chain(
    validate_with(schema; strict_mode=true),
    profile_with("dataset"),
    data -> (data, infer_mappings(data, schema))
)
```

# See also
[`pipeline`](@ref), [`SDMXPipeline`](@ref)
"""
function chain(operations...)
    return data -> foldl((result, op) -> op(result), operations, init=data)
end

# =================== EXTENDED PIPELINE OPERATORS ===================
# Note: DataSource-related pipeline operators moved to SDMXLLM.jl package

# =================== COMPREHENSIVE WORKFLOW PIPELINE ===================

"""
    SDMXPipeline{T}

A composable pipeline structure for complete SDMX data processing workflows.

This struct wraps a collection of operations that can be applied sequentially
to data, providing a reusable and composable approach to SDMX data processing.

# Fields
- `operations::T`: Tuple or collection of operations to be applied in sequence

# Examples
```julia
my_pipeline = pipeline(
    validate_with(schema),
    profile_with("dataset.csv")
)

result = my_data |> my_pipeline
```

# See also
[`pipeline`](@ref), [`chain`](@ref)
"""
struct SDMXPipeline{T}
    operations::T
end

"""
    pipeline(operations...) -> SDMXPipeline

Create an SDMX processing pipeline with chainable operations.

This function constructs an SDMXPipeline that can be reused and applied
to different datasets using Julia's pipe operator syntax.

# Arguments
- `operations...`: Variable number of functions to include in the pipeline

# Returns
- `SDMXPipeline`: A pipeline object that can be applied to data

# Examples
```julia
my_pipeline = pipeline(
    validate_with(schema; strict_mode=true),
    profile_with("dataset.csv")
)

# Execute the pipeline
result = my_data |> my_pipeline

# Reusable pipeline for multiple datasets
standard_pipeline = pipeline(
    validate_with(schema),
    profile_with("data")
)

results = [dataset1, dataset2] .|> Ref(standard_pipeline)
```

# See also
[`SDMXPipeline`](@ref), [`chain`](@ref)
"""
function pipeline(operations...)
    return SDMXPipeline(operations)
end

"""
    |>(data, pipeline::SDMXPipeline) -> Any

Execute an SDMXPipeline on data using Julia's pipe operator.

This method enables the pipe operator syntax for applying SDMXPipeline objects
to data, providing a clean and intuitive workflow syntax.

# Arguments
- `data`: The input data to process through the pipeline
- `pipeline::SDMXPipeline`: The pipeline to execute

# Returns
- `Any`: The result after applying all pipeline operations

# Examples
```julia
result = my_data |> my_pipeline
processed_data = raw_dataset |> validation_pipeline
```

# See also
[`SDMXPipeline`](@ref), [`pipeline`](@ref)
"""
function |>(data, pipeline::SDMXPipeline)
    return foldl((result, op) -> op(result), pipeline.operations, init=data)
end

# =================== SPECIALIZED DATA FLOW OPERATORS ===================

# This function was moved above to use ⇒ instead

# =================== UTILITY PIPELINE FUNCTIONS ===================

"""
    tap(f::Function) -> Function

Creates a "tap" function for side effects in pipelines without modifying data flow.

This function allows you to perform side effects (like logging, printing, or
debugging) at any point in a pipeline without affecting the data being passed through.
The original data is always returned unchanged.

# Arguments
- `f::Function`: Function to call for side effects, receives the data as input

# Returns
- `Function`: A function that applies the side effect and returns the original data

# Examples
```julia
result = my_data |>
    tap(d -> println("Processing \$(nrow(d)) rows")) |>
    validate_with(schema) |>
    tap(r -> println("Validation score: \$(r.overall_score)"))

# Debugging pipeline
debug_pipeline = pipeline(
    tap(data -> @info "Input data size: \$(size(data))"),
    validate_with(schema),
    tap(result -> @info "Validation result: \$(result.is_valid)")
)
```

# See also
[`chain`](@ref), [`pipeline`](@ref)
"""
function tap(f::Function)
    return data -> begin
        f(data)
        return data
    end
end

"""
    branch(condition::Function, true_path::Function, false_path::Function=identity) -> Function

Creates conditional branching in data processing pipelines.

This function allows pipelines to take different processing paths based on
runtime conditions, enabling adaptive data processing workflows.

# Arguments
- `condition::Function`: Function that takes data and returns a boolean
- `true_path::Function`: Function to apply when condition is true
- `false_path::Function=identity`: Function to apply when condition is false (default: identity)

# Returns
- `Function`: A function that applies conditional logic to input data

# Examples
```julia
result = my_data |>
    branch(
        data -> nrow(data) > 1000,
        validate_with(schema; performance_mode=true),  # Large dataset path
        validate_with(schema; strict_mode=true)        # Small dataset path
    )

# Handle missing data differently
processor = branch(
    data -> any(ismissing, eachcol(data)),
    data -> impute_missing(data),  # Has missing values
    identity                       # No missing values
)
```

# See also
[`tap`](@ref), [`chain`](@ref)
"""
function branch(condition::Function, true_path::Function, false_path::Function=identity)
    return data -> condition(data) ? true_path(data) : false_path(data)
end

"""
    parallel_map(f::Function) -> Function

Creates a parallel mapping function for concurrent data processing.

This function creates a parallel version of map that can process multiple
datasets or collections concurrently, improving performance for CPU-intensive
SDMX operations.

# Arguments
- `f::Function`: Function to apply to each element in parallel

# Returns
- `Function`: A function that applies f to collections in parallel using threading

# Examples
```julia
# Process multiple datasets in parallel
results = datasets |> parallel_map(validate_with(schema))

# Parallel profiling of multiple files
profiles = data_files |> parallel_map(profile_with("batch_analysis"))

# Apply transformation to multiple dataframes
transformed = dataframes |> parallel_map(data -> transform(data, :col => :new_col))
```

# Throws
- `BoundsError`: If collections are empty

# See also
[`chain`](@ref)
"""
function parallel_map(f::Function)
    return collections -> begin
        results = Vector{Any}(undef, length(collections))
        @threads for i in 1:length(collections)
            results[i] = f(collections[i])
        end
        return results
    end
end