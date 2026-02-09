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

Checks unique values in each DataFrame's unit columns. For UNIT_MEASURE, uses
`are_units_convertible` and `conversion_factor`. For currencies, checks the
exchange_rates table.

# Examples
```julia
report = detect_unit_conflicts(trade_df, gdp_df)
report.has_blocking_conflicts  # true if incompatible units
report.auto_resolvable_count   # conflicts fixable by conversion
```
"""
function detect_unit_conflicts(
        df_a::DataFrame, df_b::DataFrame;
        join_dims::Vector{String} = String[],
        exchange_rates::Union{ExchangeRateTable, Nothing} = nothing
)
    conflicts = UnitConflict[]

    # Check UNIT_MEASURE
    if hasproperty(df_a, :UNIT_MEASURE) && hasproperty(df_b, :UNIT_MEASURE)
        units_a = unique(skipmissing(df_a.UNIT_MEASURE))
        units_b = unique(skipmissing(df_b.UNIT_MEASURE))
        for ua in units_a
            for ub in units_b
                ua_str = string(ua)
                ub_str = string(ub)
                ua_str == ub_str && continue
                spec_a = sdmx_to_unitful(ua_str)
                spec_b = sdmx_to_unitful(ub_str)
                # Determine if currency conflict
                is_currency = (!isnothing(spec_a) && spec_a.category === :currency) ||
                              (!isnothing(spec_b) && spec_b.category === :currency)
                if is_currency
                    # Check exchange rate availability
                    has_rate = !isnothing(exchange_rates) &&
                              !isnothing(get_rate(exchange_rates, ua_str, ub_str))
                    push!(conflicts, UnitConflict(
                        "UNIT_MEASURE", ua_str, ub_str,
                        has_rate, nothing,
                        has_rate ? :warning : :error,
                        has_rate ?
                        "Currency mismatch " * ua_str * " vs " * ub_str * " — convertible via exchange rates" :
                        "Currency mismatch " * ua_str * " vs " * ub_str * " — no exchange rate available"
                    ))
                elseif are_units_convertible(ua_str, ub_str)
                    factor = conversion_factor(ua_str, ub_str)
                    push!(conflicts, UnitConflict(
                        "UNIT_MEASURE", ua_str, ub_str,
                        true, factor,
                        :warning,
                        "Unit mismatch " * ua_str * " vs " * ub_str * " — auto-convertible (factor=" * string(factor) * ")"
                    ))
                else
                    push!(conflicts, UnitConflict(
                        "UNIT_MEASURE", ua_str, ub_str,
                        false, nothing,
                        :error,
                        "Incompatible units " * ua_str * " vs " * ub_str * " — different dimensions"
                    ))
                end
            end
        end
    end

    # Check UNIT_MULT
    if hasproperty(df_a, :UNIT_MULT) && hasproperty(df_b, :UNIT_MULT)
        mults_a = unique(skipmissing(df_a.UNIT_MULT))
        mults_b = unique(skipmissing(df_b.UNIT_MULT))
        for ma in mults_a
            for mb in mults_b
                ma_str = string(ma)
                mb_str = string(mb)
                ma_str == mb_str && continue
                push!(conflicts, UnitConflict(
                    "UNIT_MULT", ma_str, mb_str,
                    true, unit_multiplier(ma) / unit_multiplier(mb),
                    :warning,
                    "UNIT_MULT mismatch " * ma_str * " vs " * mb_str * " — auto-resolvable by normalizing"
                ))
            end
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
