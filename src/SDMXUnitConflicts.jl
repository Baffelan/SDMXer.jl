"""
Unit Conflict Detection and Harmonization for SDMXer.jl

Detects and resolves unit mismatches when joining SDMX data:
- Identifies UNIT_MEASURE and UNIT_MULT differences
- Classifies conflicts by severity (:none, :warning, :error)
- Normalizes UNIT_MULT into OBS_VALUE
- Harmonizes convertible units (e.g., KG ↔ T)
- Flags currency conflicts requiring exchange rates
"""

using DataFrames

# =================== TYPES ===================

"""
    UnitConflict

A single unit conflict between two DataFrames being joined.

# Fields
- `dimension::String`: The column/dimension where the conflict occurs ("UNIT_MEASURE" or "UNIT_MULT")
- `value_a::String`: Value in the first DataFrame
- `value_b::String`: Value in the second DataFrame
- `is_convertible::Bool`: Whether automatic conversion is possible
- `conversion_factor::Union{Float64, Nothing}`: Factor to convert A→B, or nothing
- `severity::Symbol`: :none, :warning, or :error
- `description::String`: Human-readable conflict description

# See also
- [`UnitConflictReport`](@ref): aggregates multiple conflicts
- [`detect_unit_conflicts`](@ref): produces these from two DataFrames
"""
struct UnitConflict
    dimension::String
    value_a::String
    value_b::String
    is_convertible::Bool
    conversion_factor::Union{Float64, Nothing}
    severity::Symbol
    description::String
end

"""
    UnitConflictReport

Summary of all unit conflicts between two DataFrames.

# Fields
- `conflicts::Vector{UnitConflict}`: All detected conflicts
- `unit_measure_conflicts::Vector{UnitConflict}`: UNIT_MEASURE-specific conflicts
- `unit_mult_conflicts::Vector{UnitConflict}`: UNIT_MULT-specific conflicts
- `currency_conflicts::Vector{UnitConflict}`: Currency-related conflicts
- `has_blocking_conflicts::Bool`: True if any conflict has :error severity
- `auto_resolvable_count::Int`: Number of conflicts that can be automatically resolved
- `summary::String`: Human-readable summary

# See also
- [`detect_unit_conflicts`](@ref): produces this report
- [`harmonize_units`](@ref): resolves convertible conflicts
- [`sdmx_join`](@ref): uses this report during join validation
"""
struct UnitConflictReport
    conflicts::Vector{UnitConflict}
    unit_measure_conflicts::Vector{UnitConflict}
    unit_mult_conflicts::Vector{UnitConflict}
    currency_conflicts::Vector{UnitConflict}
    has_blocking_conflicts::Bool
    auto_resolvable_count::Int
    summary::String
end

# =================== CONFLICT DETECTION ===================

"""
    detect_unit_conflicts(df_a::DataFrame, df_b::DataFrame;
                         join_dims::Vector{String}=String[],
                         exchange_rates::Union{ExchangeRateTable, Nothing}=nothing) -> UnitConflictReport

Compare UNIT_MEASURE and UNIT_MULT columns between two DataFrames and report conflicts.

When `join_dims` is provided, only compares unit pairs that would actually appear
together in joined rows (grouped comparison). This avoids false positives from
cross-dimensional pairs (e.g., FJD from a monetary indicator vs NUM from a count
indicator in the same dataset).

When `join_dims` is empty, falls back to comparing all unique unit values
(all-vs-all), which may produce false positives for multi-indicator datasets.

# Examples
```julia
# Grouped comparison — only flags conflicts for rows that will be joined
report = detect_unit_conflicts(trade_df, gdp_df; join_dims=["GEO_PICT", "TIME_PERIOD"])

# All-vs-all fallback
report = detect_unit_conflicts(trade_df, gdp_df)
```

# See also
[`UnitConflictReport`](@ref), [`harmonize_units`](@ref), [`sdmx_join`](@ref)
"""
function detect_unit_conflicts(
        df_a::DataFrame, df_b::DataFrame;
        join_dims::Vector{String} = String[],
        exchange_rates::Union{ExchangeRateTable, Nothing} = nothing
)
    conflicts = UnitConflict[]

    # Check UNIT_MEASURE
    if hasproperty(df_a, :UNIT_MEASURE) && hasproperty(df_b, :UNIT_MEASURE)
        unit_pairs = _collect_unit_pairs(df_a, df_b, :UNIT_MEASURE, join_dims)
        for (ua_str, ub_str) in unit_pairs
            push!(conflicts, _classify_unit_pair(ua_str, ub_str, exchange_rates))
        end
    end

    # Check UNIT_MULT
    if hasproperty(df_a, :UNIT_MULT) && hasproperty(df_b, :UNIT_MULT)
        mult_pairs = _collect_unit_pairs(df_a, df_b, :UNIT_MULT, join_dims)
        for (ma_str, mb_str) in mult_pairs
            push!(conflicts, UnitConflict(
                "UNIT_MULT", ma_str, mb_str,
                true, unit_multiplier(ma_str) / unit_multiplier(mb_str),
                :warning,
                "UNIT_MULT mismatch " * ma_str * " vs " * mb_str * " — auto-resolvable by normalizing"
            ))
        end
    end

    # Categorize
    unit_measure_conflicts = filter(c -> c.dimension == "UNIT_MEASURE", conflicts)
    unit_mult_conflicts = filter(c -> c.dimension == "UNIT_MULT", conflicts)
    currency_conflicts = filter(c -> c.dimension == "UNIT_MEASURE" &&
                                     occursin("urrency", c.description), conflicts)

    has_blocking = any(c -> c.severity === :error, conflicts)
    auto_resolvable = count(c -> c.is_convertible, conflicts)

    n_total = length(conflicts)
    summary = if n_total == 0
        "No unit conflicts detected"
    else
        "Found " * string(n_total) * " unit conflict(s): " *
        string(auto_resolvable) * " auto-resolvable, " *
        string(n_total - auto_resolvable) * " require manual intervention"
    end

    return UnitConflictReport(
        conflicts, unit_measure_conflicts, unit_mult_conflicts,
        currency_conflicts, has_blocking, auto_resolvable, summary
    )
end

# =================== CONFLICT DETECTION HELPERS ===================

"""
    _collect_unit_pairs(df_a, df_b, unit_col, join_dims) -> Set{Tuple{String,String}}

Collect the distinct (unit_a, unit_b) pairs that would co-occur in joined rows.

When `join_dims` is non-empty and the columns exist in both DFs, performs a grouped
comparison: builds unique (join_key..., unit) summaries from each DF, inner-joins them
on the join_key, and extracts distinct mismatched pairs.

When `join_dims` is empty or no join columns are present, falls back to the Cartesian
product of unique unit values from each DF.
"""
function _collect_unit_pairs(
        df_a::DataFrame, df_b::DataFrame,
        unit_col::Symbol, join_dims::Vector{String}
)
    # Filter join_dims to columns that actually exist in BOTH DataFrames
    usable_dims = filter(d -> hasproperty(df_a, Symbol(d)) && hasproperty(df_b, Symbol(d)), join_dims)

    if !isempty(usable_dims)
        return _collect_grouped_pairs(df_a, df_b, unit_col, usable_dims)
    else
        return _collect_allvsall_pairs(df_a, df_b, unit_col)
    end
end

function _collect_allvsall_pairs(
        df_a::DataFrame, df_b::DataFrame, unit_col::Symbol)
    vals_a = unique(string.(collect(skipmissing(df_a[!, unit_col]))))
    vals_b = unique(string.(collect(skipmissing(df_b[!, unit_col]))))
    pairs = Set{Tuple{String, String}}()
    for ua in vals_a
        for ub in vals_b
            ua == ub && continue
            push!(pairs, (ua, ub))
        end
    end
    return pairs
end

function _collect_grouped_pairs(
        df_a::DataFrame, df_b::DataFrame,
        unit_col::Symbol, join_dims::Vector{String}
)
    dim_syms = Symbol.(join_dims)

    # Build unique (join_key..., unit) from each DF, dropping missing units
    cols_a = vcat(dim_syms, [unit_col])
    cols_b = vcat(dim_syms, [unit_col])
    summary_a = dropmissing(unique(select(df_a, cols_a)), unit_col)
    summary_b = dropmissing(unique(select(df_b, cols_b)), unit_col)

    # Rename unit columns so we can distinguish them after the join
    rename!(summary_a, unit_col => :_unit_a)
    rename!(summary_b, unit_col => :_unit_b)

    # Inner join on the join dimensions
    joined = innerjoin(summary_a, summary_b; on = dim_syms)

    # Extract distinct mismatched pairs
    pairs = Set{Tuple{String, String}}()
    for row in eachrow(joined)
        ua = string(row._unit_a)
        ub = string(row._unit_b)
        ua == ub && continue
        push!(pairs, (ua, ub))
    end
    return pairs
end

"""
    _classify_unit_pair(ua::String, ub::String, exchange_rates) -> UnitConflict

Classify a single UNIT_MEASURE pair as a UnitConflict with appropriate severity.
"""
function _classify_unit_pair(
        ua::String, ub::String,
        exchange_rates::Union{ExchangeRateTable, Nothing}
)
    spec_a = sdmx_to_unitful(ua)
    spec_b = sdmx_to_unitful(ub)

    is_currency = (!isnothing(spec_a) && spec_a.category === :currency) ||
                  (!isnothing(spec_b) && spec_b.category === :currency)

    if is_currency
        has_rate = !isnothing(exchange_rates) &&
                   !isnothing(get_rate(exchange_rates, ua, ub))
        return UnitConflict(
            "UNIT_MEASURE", ua, ub,
            has_rate, nothing,
            has_rate ? :warning : :error,
            has_rate ?
            "Currency mismatch " * ua * " vs " * ub * " — convertible via exchange rates" :
            "Currency mismatch " * ua * " vs " * ub * " — no exchange rate available"
        )
    elseif are_units_convertible(ua, ub)
        factor = conversion_factor(ua, ub)
        return UnitConflict(
            "UNIT_MEASURE", ua, ub,
            true, factor,
            :warning,
            "Unit mismatch " * ua * " vs " * ub * " — auto-convertible (factor=" * string(factor) * ")"
        )
    else
        return UnitConflict(
            "UNIT_MEASURE", ua, ub,
            false, nothing,
            :error,
            "Incompatible units " * ua * " vs " * ub * " — different dimensions"
        )
    end
end

# =================== NORMALIZATION ===================

"""
    normalize_units!(df::DataFrame;
                    target_unit::Union{String, Nothing}=nothing,
                    exchange_rates::Union{ExchangeRateTable, Nothing}=nothing) -> DataFrame

Normalize a DataFrame's OBS_VALUE by applying UNIT_MULT and optionally converting UNIT_MEASURE.

Operations (in order):
1. If UNIT_MULT column exists, multiply OBS_VALUE by 10^UNIT_MULT, then set UNIT_MULT to 0
2. If target_unit is specified and UNIT_MEASURE exists, convert values to target_unit

Mutates `df` in place and returns it.

# Examples
```julia
# Normalize UNIT_MULT only
normalize_units!(df)

# Normalize and convert to tonnes
normalize_units!(df; target_unit="T")

# Normalize with currency conversion
normalize_units!(df; target_unit="USD", exchange_rates=default_exchange_rates())
```
"""
function normalize_units!(
        df::DataFrame;
        target_unit::Union{String, Nothing} = nothing,
        exchange_rates::Union{ExchangeRateTable, Nothing} = nothing
)
    # Step 1: Apply UNIT_MULT
    if hasproperty(df, :UNIT_MULT) && hasproperty(df, :OBS_VALUE)
        for i in 1:nrow(df)
            mult = df.UNIT_MULT[i]
            if !ismissing(mult) && !ismissing(df.OBS_VALUE[i])
                multiplier = unit_multiplier(mult)
                if multiplier != 1.0
                    df.OBS_VALUE[i] = df.OBS_VALUE[i] * multiplier
                    df.UNIT_MULT[i] = 0
                end
            end
        end
    end

    # Step 2: Convert UNIT_MEASURE if target specified
    if !isnothing(target_unit) && hasproperty(df, :UNIT_MEASURE) && hasproperty(df, :OBS_VALUE)
        for i in 1:nrow(df)
            current_unit = df.UNIT_MEASURE[i]
            ismissing(current_unit) && continue
            current_str = string(current_unit)
            current_str == target_unit && continue
            ismissing(df.OBS_VALUE[i]) && continue

            # Try deterministic conversion first
            factor = conversion_factor(current_str, target_unit)
            if !isnothing(factor)
                df.OBS_VALUE[i] = df.OBS_VALUE[i] * factor
                df.UNIT_MEASURE[i] = target_unit
                continue
            end

            # Try currency conversion
            if !isnothing(exchange_rates)
                converted = convert_currency(df.OBS_VALUE[i], current_str, target_unit, exchange_rates)
                if !isnothing(converted)
                    df.OBS_VALUE[i] = converted
                    df.UNIT_MEASURE[i] = target_unit
                end
            end
        end
    end

    return df
end

"""
    harmonize_units(df_a::DataFrame, df_b::DataFrame;
                   target_unit::Union{String, Nothing}=nothing,
                   exchange_rates::Union{ExchangeRateTable, Nothing}=nothing) -> Tuple{DataFrame, DataFrame}

Non-mutating version: copies both DataFrames then normalizes them.

Returns `(normalized_a, normalized_b)`.

# Examples
```julia
norm_a, norm_b = harmonize_units(trade_df, gdp_df; target_unit="USD",
                                  exchange_rates=default_exchange_rates())
```

# See also
[`detect_unit_conflicts`](@ref), [`normalize_units!`](@ref), [`sdmx_join`](@ref)
"""
function harmonize_units(
        df_a::DataFrame, df_b::DataFrame;
        target_unit::Union{String, Nothing} = nothing,
        exchange_rates::Union{ExchangeRateTable, Nothing} = nothing
)
    copy_a = copy(df_a)
    copy_b = copy(df_b)
    normalize_units!(copy_a; target_unit = target_unit, exchange_rates = exchange_rates)
    normalize_units!(copy_b; target_unit = target_unit, exchange_rates = exchange_rates)
    return (copy_a, copy_b)
end
