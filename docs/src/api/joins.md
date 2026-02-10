# Cross-Dataflow Joins

Functions for comparing, harmonizing, and joining SDMX DataFrames across different dataflows.

## Schema Comparison

```@docs
SDMXer.SchemaComparison
SDMXer.CodelistOverlap
SDMXer.compare_schemas
SDMXer.codelist_overlap
```

## Unit Conflicts

```@docs
SDMXer.UnitConflict
SDMXer.UnitConflictReport
SDMXer.detect_unit_conflicts
SDMXer.normalize_units!
SDMXer.harmonize_units
```

## Frequency Alignment

```@docs
SDMXer.FrequencyAlignment
SDMXer.align_frequencies
```

## Horizontal Join

```@docs
SDMXer.JoinResult
SDMXer.detect_join_columns
SDMXer.sdmx_join
```

## Vertical Combine

```@docs
SDMXer.CombineResult
SDMXer.sdmx_combine
SDMXer.pivot_sdmx_wide
```
