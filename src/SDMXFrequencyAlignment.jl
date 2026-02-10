"""
Frequency Alignment for SDMXer.jl

Aligns time-series data from different SDMX dataflows that may use different
frequencies (annual, quarterly, monthly). Aggregates higher-frequency data to
match lower-frequency data for joining.
"""

using DataFrames, Statistics, Dates

# =================== TYPES ===================

"""
    FrequencyAlignment

Describes the frequency alignment applied to a pair of DataFrames.

# Fields
- `source_freq::String`: Original frequency of the higher-freq DataFrame (e.g., "Q", "M")
- `target_freq::String`: Target frequency after alignment (e.g., "A")
- `method::Symbol`: Alignment method used (:aggregate, :none, :passthrough)
- `aggregation_fn::Symbol`: Aggregation function used (:sum, :mean, :last, :first, :max, :min)

# See also
- [`align_frequencies`](@ref): produces this result when aligning two DataFrames
"""
struct FrequencyAlignment
    source_freq::String
    target_freq::String
    method::Symbol
    aggregation_fn::Symbol
end

# =================== FREQUENCY HIERARCHY ===================

const FREQ_RANK = Dict{String, Int}(
    "A" => 1,    # Annual
    "S" => 2,    # Semi-annual
    "Q" => 4,    # Quarterly
    "M" => 12,   # Monthly
    "W" => 52,   # Weekly
    "D" => 365,  # Daily
    "B" => 260   # Business daily
)

# =================== CORE FUNCTIONS ===================

"""
    align_frequencies(df_a::DataFrame, df_b::DataFrame;
                     time_col::String="TIME_PERIOD",
                     freq_col::String="FREQ",
                     value_col::String="OBS_VALUE",
                     target_freq::Union{String, Nothing}=nothing,
                     aggregation::Symbol=:sum,
                     group_cols::Union{Vector{String}, Nothing}=nothing) -> Tuple{DataFrame, DataFrame, FrequencyAlignment}

Align two DataFrames to a common time frequency by aggregating higher-frequency data.

If `target_freq` is not specified, uses the lower frequency of the two (e.g., if one
is quarterly and the other annual, aggregates the quarterly data to annual).

# Arguments
- `df_a`, `df_b`: DataFrames to align
- `time_col`: Name of the time period column (default "TIME_PERIOD")
- `freq_col`: Name of the frequency column (default "FREQ")
- `value_col`: Name of the observation value column (default "OBS_VALUE")
- `target_freq`: Force alignment to this frequency (default: auto-detect)
- `aggregation`: Aggregation function (:sum, :mean, :last, :first, :max, :min)
- `group_cols`: Additional columns to group by during aggregation (auto-detected if nothing)

# Returns
- `(aligned_a, aligned_b, alignment_info)`: Aligned DataFrames and alignment metadata

# Examples
```julia
aligned_a, aligned_b, info = align_frequencies(quarterly_df, annual_df)
info.source_freq   # "Q"
info.target_freq   # "A"
info.method        # :aggregate
```
"""
function align_frequencies(
        df_a::DataFrame, df_b::DataFrame;
        time_col::String = "TIME_PERIOD",
        freq_col::String = "FREQ",
        value_col::String = "OBS_VALUE",
        target_freq::Union{String, Nothing} = nothing,
        aggregation::Symbol = :sum,
        group_cols::Union{Vector{String}, Nothing} = nothing
)
    freq_a = _detect_frequency(df_a, freq_col, time_col)
    freq_b = _detect_frequency(df_b, freq_col, time_col)

    # Determine target frequency
    if isnothing(target_freq)
        rank_a = get(FREQ_RANK, freq_a, 1)
        rank_b = get(FREQ_RANK, freq_b, 1)
        target_freq = rank_a <= rank_b ? freq_a : freq_b
    end

    rank_target = get(FREQ_RANK, target_freq, 1)
    rank_a = get(FREQ_RANK, freq_a, 1)
    rank_b = get(FREQ_RANK, freq_b, 1)

    # Auto-detect group columns (non-time, non-value, non-freq columns)
    if isnothing(group_cols)
        group_cols = _auto_group_cols(df_a, time_col, freq_col, value_col)
    end

    # Align each DataFrame if needed
    aligned_a = if rank_a > rank_target
        _aggregate_to_frequency(df_a, freq_a, target_freq, time_col, freq_col, value_col, aggregation, group_cols)
    else
        copy(df_a)
    end

    aligned_b = if rank_b > rank_target
        _aggregate_to_frequency(df_b, freq_b, target_freq, time_col, freq_col, value_col, aggregation, group_cols)
    else
        copy(df_b)
    end

    source_freq = rank_a > rank_b ? freq_a : freq_b
    method = (freq_a == freq_b && freq_a == target_freq) ? :none : :aggregate

    alignment = FrequencyAlignment(source_freq, target_freq, method, aggregation)

    return (aligned_a, aligned_b, alignment)
end

# =================== HELPERS ===================

function _detect_frequency(df::DataFrame, freq_col::String, time_col::String)
    # Try explicit FREQ column first
    if hasproperty(df, Symbol(freq_col))
        freqs = unique(skipmissing(df[!, Symbol(freq_col)]))
        if length(freqs) == 1
            return string(first(freqs))
        elseif length(freqs) > 1
            # Multiple frequencies — return the most common
            freq_counts = combine(groupby(df, Symbol(freq_col)), nrow => :count)
            sort!(freq_counts, :count, rev = true)
            return string(first(freq_counts[!, Symbol(freq_col)]))
        end
    end

    # Infer from time period format
    if hasproperty(df, Symbol(time_col))
        periods = unique(skipmissing(df[!, Symbol(time_col)]))
        if !isempty(periods)
            sample = string(first(periods))
            return _infer_frequency_from_period(sample)
        end
    end

    return "A"  # Default to annual
end

function _infer_frequency_from_period(period::String)
    # Year only: "2020"
    if occursin(r"^\d{4}$", period)
        return "A"
    end
    # Quarter: "2020-Q1" or "2020Q1"
    if occursin(r"^\d{4}-?Q\d$", period)
        return "Q"
    end
    # Month: "2020-01" or "2020M01"
    if occursin(r"^\d{4}-\d{2}$", period) || occursin(r"^\d{4}M\d{2}$", period)
        return "M"
    end
    # Week: "2020-W01"
    if occursin(r"^\d{4}-?W\d{2}$", period)
        return "W"
    end
    # Day: "2020-01-15"
    if occursin(r"^\d{4}-\d{2}-\d{2}$", period)
        return "D"
    end
    return "A"
end

function _extract_year(period::String)
    m = match(r"^(\d{4})", period)
    return isnothing(m) ? period : m[1]
end

function _auto_group_cols(df::DataFrame, time_col::String, freq_col::String, value_col::String)
    exclude = Set([time_col, freq_col, value_col,
        "OBS_STATUS", "DECIMALS", "DATAFLOW", "STRUCTURE",
        "UNIT_MULT", "OBS_VALUE"])
    return [string(c) for c in names(df) if !(string(c) in exclude) && eltype(df[!, c]) <: Union{Missing, AbstractString, Symbol}]
end

function _aggregate_to_frequency(
        df::DataFrame, source_freq::String, target_freq::String,
        time_col::String, freq_col::String, value_col::String,
        aggregation::Symbol, group_cols::Vector{String}
)
    result = copy(df)

    # Add a target period column
    if target_freq == "A"
        result[!, :_target_period] = [_extract_year(string(p)) for p in result[!, Symbol(time_col)]]
    else
        # For other target frequencies, we'd need more complex period mapping
        # For now, just extract the year as a reasonable default
        result[!, :_target_period] = [_extract_year(string(p)) for p in result[!, Symbol(time_col)]]
    end

    # Build grouping columns
    grp_syms = vcat(Symbol.(group_cols), [:_target_period])
    # Filter to columns that actually exist
    grp_syms = filter(s -> hasproperty(result, s), grp_syms)

    agg_fn = _get_agg_function(aggregation)

    # Group and aggregate
    grouped = groupby(result, grp_syms)
    aggregated = combine(grouped, Symbol(value_col) => agg_fn => Symbol(value_col))

    # Restore time column from target period
    if hasproperty(aggregated, :_target_period)
        aggregated[!, Symbol(time_col)] = aggregated._target_period
        select!(aggregated, Not(:_target_period))
    end

    # Update FREQ column if it exists
    if hasproperty(aggregated, Symbol(freq_col))
        aggregated[!, Symbol(freq_col)] .= target_freq
    end

    return aggregated
end

function _get_agg_function(aggregation::Symbol)
    aggregation === :sum && return (x -> sum(skipmissing(x)))
    aggregation === :mean && return (x -> mean(skipmissing(x)))
    aggregation === :last && return (x -> last(collect(skipmissing(x))))
    aggregation === :first && return (x -> first(collect(skipmissing(x))))
    aggregation === :max && return (x -> maximum(skipmissing(x)))
    aggregation === :min && return (x -> minimum(skipmissing(x)))
    error("Unknown aggregation function: " * string(aggregation))
end
