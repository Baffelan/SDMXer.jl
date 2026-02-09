"""
Cross-Dataflow Join for SDMXer.jl

Provides intelligent joining of SDMX DataFrames from different dataflows:
- Auto-detects join columns from shared dimensions
- Validates unit compatibility before joining
- Renames conflicting OBS_VALUE columns with suffixes
- Warns about time range mismatches
- Integrates with schema comparison, unit conflict detection, and frequency alignment
"""

using DataFrames

# =================== TYPES ===================

"""
    JoinResult

Result of joining two SDMX DataFrames.

# Fields
- `data::DataFrame`: The joined DataFrame
- `join_columns::Vector{String}`: Columns used for the join
- `join_type::Symbol`: Type of join performed (:inner, :outer, :left, :right)
- `rows_matched::Int`: Number of rows in the result
- `unit_report::Union{UnitConflictReport, Nothing}`: Unit conflict analysis
- `warnings::Vector{String}`: Any warnings generated during the join
- `metadata::Dict{String, Any}`: Additional metadata about the join
"""
struct JoinResult
    data::DataFrame
    join_columns::Vector{String}
    join_type::Symbol
    rows_matched::Int
    unit_report::Union{UnitConflictReport, Nothing}
    warnings::Vector{String}
    metadata::Dict{String, Any}
end

# =================== SDMX METADATA COLUMNS ===================

const SDMX_METADATA_COLS = Set([
    "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "OBS_STATUS",
    "DECIMALS", "DATAFLOW", "STRUCTURE", "STRUCTURE_ID",
    "STRUCTURE_NAME", "ACTION", "OBS_CONF", "CONF_STATUS",
    "BASE_PER", "TIME_FORMAT"
])

# =================== DETECT JOIN COLUMNS ===================

"""
    detect_join_columns(df_a::DataFrame, df_b::DataFrame;
                       schema_a::Union{DataflowSchema, Nothing}=nothing,
                       schema_b::Union{DataflowSchema, Nothing}=nothing) -> Vector{String}

Detect appropriate columns for joining two SDMX DataFrames.

Priority:
1. If schemas provided, prefer columns with matching codelist_ids
2. Otherwise, find columns present in both DataFrames
3. Exclude SDMX metadata columns (OBS_VALUE, UNIT_MEASURE, etc.)
4. Validate that candidate columns have overlapping values

# Examples
```julia
join_cols = detect_join_columns(trade_df, pop_df)
join_cols = detect_join_columns(trade_df, pop_df; schema_a=schema_trade, schema_b=schema_pop)
```
"""
function detect_join_columns(
        df_a::DataFrame, df_b::DataFrame;
        schema_a::Union{DataflowSchema, Nothing} = nothing,
        schema_b::Union{DataflowSchema, Nothing} = nothing
)
    names_a = Set(string.(names(df_a)))
    names_b = Set(string.(names(df_b)))
    common = intersect(names_a, names_b)

    # Exclude SDMX metadata columns
    candidates = sort(collect(setdiff(common, SDMX_METADATA_COLS)))

    if !isnothing(schema_a) && !isnothing(schema_b)
        comparison = compare_schemas(schema_a, schema_b)
        # Prioritize columns that appear in recommended_join_dims
        recommended = Set(comparison.recommended_join_dims)
        prioritized = filter(c -> c in recommended, candidates)
        secondary = filter(c -> !(c in recommended), candidates)
        candidates = vcat(prioritized, secondary)
    end

    # Filter to columns with actual value overlap
    valid = String[]
    for col in candidates
        vals_a = Set(skipmissing(df_a[!, Symbol(col)]))
        vals_b = Set(skipmissing(df_b[!, Symbol(col)]))
        if !isempty(intersect(vals_a, vals_b))
            push!(valid, col)
        end
    end

    return valid
end

# =================== SDMX JOIN ===================

"""
    sdmx_join(df_a::DataFrame, df_b::DataFrame;
             on::Union{Symbol, Vector{String}}=:auto,
             join_type::Symbol=:inner,
             validate_units::Bool=true,
             harmonize::Bool=true,
             exchange_rates::Union{ExchangeRateTable, Nothing}=nothing,
             schema_a::Union{DataflowSchema, Nothing}=nothing,
             schema_b::Union{DataflowSchema, Nothing}=nothing,
             suffix_a::String="_a",
             suffix_b::String="_b") -> JoinResult

Join two SDMX DataFrames with unit validation, harmonization, and automatic
join column detection.

# Arguments
- `df_a`, `df_b`: DataFrames to join
- `on`: Join columns. `:auto` to auto-detect, or explicit column names.
- `join_type`: `:inner`, `:outer`, `:left`, or `:right`
- `validate_units`: Check for unit conflicts before joining
- `harmonize`: Normalize UNIT_MULT into OBS_VALUE before joining
- `exchange_rates`: Exchange rate table for currency conversion
- `schema_a`, `schema_b`: Optional schemas for smarter column detection
- `suffix_a`, `suffix_b`: Suffixes for conflicting column names (e.g., OBS_VALUE_a)

# Returns
- `JoinResult`: Contains the joined DataFrame, join metadata, and any warnings

# Examples
```julia
result = sdmx_join(trade_df, pop_df; join_type=:inner)
result.data              # joined DataFrame
result.join_columns      # columns used
result.warnings          # any issues

# With explicit columns and exchange rates
result = sdmx_join(trade_df, gdp_df;
    on=["GEO_PICT", "TIME_PERIOD"],
    exchange_rates=default_exchange_rates())
```
"""
function sdmx_join(
        df_a::DataFrame, df_b::DataFrame;
        on::Union{Symbol, Vector{String}} = :auto,
        join_type::Symbol = :inner,
        validate_units::Bool = true,
        harmonize::Bool = true,
        exchange_rates::Union{ExchangeRateTable, Nothing} = nothing,
        schema_a::Union{DataflowSchema, Nothing} = nothing,
        schema_b::Union{DataflowSchema, Nothing} = nothing,
        suffix_a::String = "_a",
        suffix_b::String = "_b"
)
    warnings = String[]

    # Step 1: Detect or validate join columns
    join_cols = if on === :auto
        detected = detect_join_columns(df_a, df_b; schema_a = schema_a, schema_b = schema_b)
        if isempty(detected)
            push!(warnings, "No common join columns detected — returning cross join")
        end
        detected
    else
        on
    end

    # Step 2: Unit conflict detection
    unit_report = nothing
    if validate_units
        unit_report = detect_unit_conflicts(df_a, df_b;
            join_dims = join_cols,
            exchange_rates = exchange_rates)
        if unit_report.has_blocking_conflicts
            push!(warnings, "Blocking unit conflicts detected: " * unit_report.summary)
        end
        for conflict in unit_report.conflicts
            push!(warnings, conflict.description)
        end
    end

    # Step 3: Harmonize units if requested
    work_a, work_b = if harmonize
        harmonize_units(df_a, df_b; exchange_rates = exchange_rates)
    else
        (copy(df_a), copy(df_b))
    end

    # Step 4: Rename conflicting columns with suffixes
    names_a = Set(string.(names(work_a)))
    names_b = Set(string.(names(work_b)))
    join_set = Set(join_cols)
    conflicting = setdiff(intersect(names_a, names_b), join_set)

    rename_a = Dict{String, String}()
    rename_b = Dict{String, String}()
    for col in conflicting
        rename_a[col] = col * suffix_a
        rename_b[col] = col * suffix_b
    end

    if !isempty(rename_a)
        rename!(work_a, [Symbol(k) => Symbol(v) for (k, v) in rename_a]...)
        rename!(work_b, [Symbol(k) => Symbol(v) for (k, v) in rename_b]...)
    end

    # Step 5: Perform the join
    joined = if isempty(join_cols)
        crossjoin(work_a, work_b; makeunique = true)
    else
        join_syms = Symbol.(join_cols)
        if join_type === :inner
            innerjoin(work_a, work_b; on = join_syms, makeunique = true)
        elseif join_type === :outer
            outerjoin(work_a, work_b; on = join_syms, makeunique = true)
        elseif join_type === :left
            leftjoin(work_a, work_b; on = join_syms, makeunique = true)
        elseif join_type === :right
            rightjoin(work_a, work_b; on = join_syms, makeunique = true)
        else
            error("Unknown join type: " * string(join_type))
        end
    end

    # Step 6: Check for time range warnings
    if "TIME_PERIOD" in join_cols
        _check_time_range_overlap!(warnings, df_a, df_b)
    end

    metadata = Dict{String, Any}(
        "renamed_columns_a" => rename_a,
        "renamed_columns_b" => rename_b,
        "rows_a" => nrow(df_a),
        "rows_b" => nrow(df_b),
        "rows_joined" => nrow(joined)
    )

    return JoinResult(
        joined, join_cols, join_type, nrow(joined),
        unit_report, warnings, metadata
    )
end

# =================== HELPERS ===================

function _check_time_range_overlap!(warnings::Vector{String}, df_a::DataFrame, df_b::DataFrame)
    if hasproperty(df_a, :TIME_PERIOD) && hasproperty(df_b, :TIME_PERIOD)
        periods_a = sort(unique(string.(skipmissing(df_a.TIME_PERIOD))))
        periods_b = sort(unique(string.(skipmissing(df_b.TIME_PERIOD))))
        if !isempty(periods_a) && !isempty(periods_b)
            overlap = intersect(Set(periods_a), Set(periods_b))
            if isempty(overlap)
                push!(warnings,
                    "No overlapping time periods: A=[" * first(periods_a) * ".." * last(periods_a) *
                    "], B=[" * first(periods_b) * ".." * last(periods_b) * "]")
            else
                only_a = length(setdiff(Set(periods_a), Set(periods_b)))
                only_b = length(setdiff(Set(periods_b), Set(periods_a)))
                if only_a > 0 || only_b > 0
                    push!(warnings,
                        "Partial time overlap: " * string(length(overlap)) * " shared periods, " *
                        string(only_a) * " only in A, " * string(only_b) * " only in B")
                end
            end
        end
    end
end
