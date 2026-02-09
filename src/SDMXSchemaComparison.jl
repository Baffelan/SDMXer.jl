"""
Schema Comparison for SDMXer.jl

Provides tools for comparing two DataflowSchema objects to identify:
- Shared dimensions (same codelist_id)
- Compatible dimensions (different codelist but overlapping codes)
- Unique dimensions in each schema
- Time period overlap
- Joinability scoring and recommended join dimensions
"""

using DataFrames

# =================== TYPES ===================

"""
    CodelistOverlap

Result of comparing two sets of codelist codes.

# Fields
- `intersection::Vector{String}`: Codes present in both sets
- `only_in_a::Vector{String}`: Codes only in the first set
- `only_in_b::Vector{String}`: Codes only in the second set
- `overlap_ratio::Float64`: |intersection| / |union|, 0.0–1.0
- `a_coverage::Float64`: Fraction of A's codes found in B
- `b_coverage::Float64`: Fraction of B's codes found in A
"""
struct CodelistOverlap
    intersection::Vector{String}
    only_in_a::Vector{String}
    only_in_b::Vector{String}
    overlap_ratio::Float64
    a_coverage::Float64
    b_coverage::Float64
end

"""
    SchemaComparison

Result of comparing two DataflowSchema objects.

# Fields
- `schema_a_info::NamedTuple`: dataflow_info from schema A
- `schema_b_info::NamedTuple`: dataflow_info from schema B
- `shared_dimensions::DataFrame`: Dimensions with the same codelist_id in both schemas
- `compatible_dimensions::DataFrame`: Dimensions with different codelist_id but overlapping codes
- `unique_to_a::Vector{String}`: Dimension IDs only in schema A
- `unique_to_b::Vector{String}`: Dimension IDs only in schema B
- `shared_attributes::Vector{String}`: Attribute IDs present in both schemas
- `time_overlap::Union{NamedTuple, Nothing}`: Overlapping time range, or nothing
- `joinability_score::Float64`: Overall joinability score 0.0–1.0
- `recommended_join_dims::Vector{String}`: Recommended dimension IDs for joining
"""
struct SchemaComparison
    schema_a_info::NamedTuple
    schema_b_info::NamedTuple
    shared_dimensions::DataFrame
    compatible_dimensions::DataFrame
    unique_to_a::Vector{String}
    unique_to_b::Vector{String}
    shared_attributes::Vector{String}
    time_overlap::Union{NamedTuple, Nothing}
    joinability_score::Float64
    recommended_join_dims::Vector{String}
end

# =================== CODELIST OVERLAP ===================

"""
    codelist_overlap(codes_a::Vector{String}, codes_b::Vector{String}) -> CodelistOverlap

Compute set overlap between two vectors of code strings.

# Examples
```julia
overlap = codelist_overlap(["FJ", "TV", "WS"], ["FJ", "TV", "PG"])
overlap.intersection  # ["FJ", "TV"]
overlap.overlap_ratio # 0.5
```
"""
function codelist_overlap(codes_a::Vector{String}, codes_b::Vector{String})
    set_a = Set(codes_a)
    set_b = Set(codes_b)
    inter = sort(collect(intersect(set_a, set_b)))
    only_a = sort(collect(setdiff(set_a, set_b)))
    only_b = sort(collect(setdiff(set_b, set_a)))
    union_size = length(union(set_a, set_b))
    overlap_ratio = union_size > 0 ? length(inter) / union_size : 0.0
    a_cov = length(set_a) > 0 ? length(inter) / length(set_a) : 0.0
    b_cov = length(set_b) > 0 ? length(inter) / length(set_b) : 0.0
    return CodelistOverlap(inter, only_a, only_b, overlap_ratio, a_cov, b_cov)
end

"""
    codelist_overlap(codelists_df_a::DataFrame, codelists_df_b::DataFrame, id_a::String, id_b::String) -> CodelistOverlap

Compute overlap between two codelists identified by codelist_id within their
respective codelists DataFrames.

Expects the standard codelist DataFrame format from `extract_all_codelists`
with columns: codelist_id, code_id, lang, name, parent_code_id, order.

# Examples
```julia
codelists_a = extract_all_codelists(url_a)
codelists_b = extract_all_codelists(url_b)
overlap = codelist_overlap(codelists_a, codelists_b, "CL_GEO_PICT", "CL_GEO_PICT")
```
"""
function codelist_overlap(
        codelists_df_a::DataFrame,
        codelists_df_b::DataFrame,
        id_a::String,
        id_b::String
)
    codes_a = _extract_codes_for_id(codelists_df_a, id_a)
    codes_b = _extract_codes_for_id(codelists_df_b, id_b)
    return codelist_overlap(codes_a, codes_b)
end

function _extract_codes_for_id(codelists_df::DataFrame, codelist_id::String)
    filtered = filter(row -> row.codelist_id == codelist_id, codelists_df)
    isempty(filtered) && return String[]
    return unique(string.(filtered.code_id))
end

# =================== SCHEMA COMPARISON ===================

"""
    compare_schemas(schema_a::DataflowSchema, schema_b::DataflowSchema) -> SchemaComparison

Compare two DataflowSchema objects to identify shared and compatible dimensions,
time overlap, and recommend join dimensions.

Shared dimensions have the same codelist_id. Compatible dimensions have different
codelist_ids but must be checked for code overlap externally (this function marks
them as candidates). The joinability_score reflects how many dimensions overlap.

# Examples
```julia
schema_a = extract_dataflow_schema(url_a)
schema_b = extract_dataflow_schema(url_b)
comparison = compare_schemas(schema_a, schema_b)
comparison.shared_dimensions       # DataFrame of shared dims
comparison.recommended_join_dims   # suggested join columns
comparison.joinability_score       # 0.0–1.0
```
"""
function compare_schemas(schema_a::DataflowSchema, schema_b::DataflowSchema)
    dims_a = schema_a.dimensions
    dims_b = schema_b.dimensions

    # Build lookup maps: codelist_id → dimension row(s)
    cl_to_dims_a = _codelist_dim_map(dims_a)
    cl_to_dims_b = _codelist_dim_map(dims_b)

    dim_ids_a = Set(dims_a.dimension_id)
    dim_ids_b = Set(dims_b.dimension_id)

    # Shared dimensions: same codelist_id in both schemas
    shared_cls = intersect(keys(cl_to_dims_a), keys(cl_to_dims_b))
    shared_rows = NamedTuple[]
    shared_dim_ids = Set{String}()
    for cl_id in shared_cls
        for dim_a in cl_to_dims_a[cl_id]
            for dim_b in cl_to_dims_b[cl_id]
                push!(shared_rows, (
                    codelist_id = cl_id,
                    dim_id_a = dim_a.dimension_id,
                    dim_id_b = dim_b.dimension_id,
                    concept_a = dim_a.concept_id,
                    concept_b = dim_b.concept_id
                ))
                push!(shared_dim_ids, dim_a.dimension_id)
                push!(shared_dim_ids, dim_b.dimension_id)
            end
        end
    end
    shared_df = isempty(shared_rows) ? DataFrame() : DataFrame(shared_rows)

    # Compatible dimensions: same dimension_id in both but different codelist
    compatible_rows = NamedTuple[]
    common_ids = intersect(dim_ids_a, dim_ids_b)
    for dim_id in common_ids
        dim_id in shared_dim_ids && continue
        row_a = first(filter(r -> r.dimension_id == dim_id, eachrow(dims_a)))
        row_b = first(filter(r -> r.dimension_id == dim_id, eachrow(dims_b)))
        cl_a = ismissing(row_a.codelist_id) ? "" : row_a.codelist_id
        cl_b = ismissing(row_b.codelist_id) ? "" : row_b.codelist_id
        if cl_a != cl_b
            push!(compatible_rows, (
                dimension_id = dim_id,
                codelist_a = cl_a,
                codelist_b = cl_b,
                concept_a = row_a.concept_id,
                concept_b = row_b.concept_id
            ))
        end
    end
    compatible_df = isempty(compatible_rows) ? DataFrame() : DataFrame(compatible_rows)

    # Unique dimensions
    used_a = Set{String}()
    used_b = Set{String}()
    for row in shared_rows
        push!(used_a, row.dim_id_a)
        push!(used_b, row.dim_id_b)
    end
    for row in compatible_rows
        push!(used_a, row.dimension_id)
        push!(used_b, row.dimension_id)
    end
    unique_a = sort(collect(setdiff(dim_ids_a, used_a)))
    unique_b = sort(collect(setdiff(dim_ids_b, used_b)))

    # Shared attributes
    attr_ids_a = Set(schema_a.attributes.attribute_id)
    attr_ids_b = Set(schema_b.attributes.attribute_id)
    shared_attrs = sort(collect(intersect(attr_ids_a, attr_ids_b)))

    # Time overlap
    time_overlap = _compute_time_overlap(schema_a, schema_b)

    # Recommended join dimensions — prefer shared codelist dims that use same dimension_id
    recommended = String[]
    for row in shared_rows
        if row.dim_id_a == row.dim_id_b
            push!(recommended, row.dim_id_a)
        end
    end
    # Also add compatible dimensions (same id, different codelist) as secondary candidates
    for row in compatible_rows
        push!(recommended, row.dimension_id)
    end
    # Add time dimension if both schemas have one
    if !isnothing(schema_a.time_dimension) && !isnothing(schema_b.time_dimension)
        td_a = schema_a.time_dimension.dimension_id
        td_b = schema_b.time_dimension.dimension_id
        if td_a == td_b && !(td_a in recommended)
            push!(recommended, td_a)
        end
    end
    recommended = unique(recommended)

    # Joinability score: fraction of dimension slots that are shared/compatible
    total_dims = length(union(dim_ids_a, dim_ids_b))
    matched_dims = length(Set(vcat(
        [row.dim_id_a for row in shared_rows],
        [row.dim_id_b for row in shared_rows],
        [row.dimension_id for row in compatible_rows]
    )))
    joinability = total_dims > 0 ? matched_dims / total_dims : 0.0

    return SchemaComparison(
        schema_a.dataflow_info,
        schema_b.dataflow_info,
        shared_df,
        compatible_df,
        unique_a,
        unique_b,
        shared_attrs,
        time_overlap,
        joinability,
        recommended
    )
end

# =================== HELPERS ===================

function _codelist_dim_map(dims_df::DataFrame)
    result = Dict{String, Vector{DataFrameRow}}()
    for row in eachrow(dims_df)
        ismissing(row.codelist_id) && continue
        cl_id = row.codelist_id
        if !haskey(result, cl_id)
            result[cl_id] = DataFrameRow[]
        end
        push!(result[cl_id], row)
    end
    return result
end

function _compute_time_overlap(schema_a::DataflowSchema, schema_b::DataflowSchema)
    td_a = schema_a.time_dimension
    td_b = schema_b.time_dimension
    (isnothing(td_a) || isnothing(td_b)) && return nothing
    # Return basic info — actual time range comparison requires availability data
    return (
        dim_id_a = td_a.dimension_id,
        dim_id_b = td_b.dimension_id,
        same_id = td_a.dimension_id == td_b.dimension_id,
        note = "Time range comparison requires availability constraint data"
    )
end
