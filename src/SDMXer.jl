"""
SDMXer.jl - A Julia package for reading and parsing SDMX structural metadata into handy structures (e.g., DataFrames).

This package provides utilities to extract codelists and codes from SDMX-ML XML documents, making it easy to work with SDMX metadata in Julia.
"""
module SDMXer

using EzXML, DataFrames, HTTP, CSV, Statistics, Dates, JSON3, StatsBase, Unitful

include("SDMXElementTypes.jl")
include("SDMXGeneratedParsing.jl")
include("SDMXGeneratedIntegration.jl")
include("SDMXCodelists.jl")
include("SDMXConcepts.jl")
include("SDMXDataflows.jl")
include("SDMXAvailability.jl")
include("SDMXValidation.jl")
include("SDMXPipelineOps.jl")
include("SDMXDataQueries.jl")
include("SDMXHelpers.jl")
include("SDMXUnits.jl")
include("SDMXSchemaComparison.jl")
include("SDMXUnitConflicts.jl")
include("SDMXFrequencyAlignment.jl")
include("SDMXJoin.jl")

# Note: Data source abstractions moved to SDMXLLM.jl package

# === CORE DATA STRUCTURES ===
# Primary types for SDMX schema, data profiling, validation, and availability analysis
export DataflowSchema
export AvailabilityConstraint, DimensionAvailability, TimeAvailability
export ValidationResult, ValidationRule, ValidationSeverity, SDMXValidator

# === GENERATED FUNCTION TYPES & PARSING ===
# Type-specialized parsing system using @generated functions for compile-time optimization
export SDMXElement, DimensionElement, AttributeElement, MeasureElement, ConceptElement, CodelistElement, AvailabilityElement, TimeElement
export extract_sdmx_element, get_xpath_patterns, extract_code_info, extract_generic_element
export demonstrate_generated_parsing, migration_guide, create_benchmark_xml

# === SDMX SCHEMA & METADATA EXTRACTION ===
# Functions for extracting and analyzing SDMX schema structures and concepts
export extract_concepts, extract_dataflow_schema
export get_required_columns, get_optional_columns, get_codelist_columns, get_dimension_order

# === CODELIST PROCESSING ===
# Functions for extracting, processing, and mapping SDMX codelists
export get_parent_id, process_code_node, extract_codes_from_codelist_node
export extract_all_codelists, filter_codelists_by_availability, get_available_codelist_summary
export construct_availability_url, map_codelist_to_dimension

# === DATA AVAILABILITY ANALYSIS ===
# Functions for analyzing data availability constraints and coverage
export extract_availability, extract_availability_from_dataflow, extract_availability_from_node, get_available_values, get_time_coverage, get_time_period_range
export extract_time_availability, get_time_period_values, extract_dimension_values
export compare_schema_availability, get_data_coverage_summary, find_data_gaps, print_availability_summary

# Note: Data source processing moved to SDMXLLM.jl package

# === DATA VALIDATION SYSTEM ===
# Comprehensive validation framework for SDMX data quality and compliance
export create_validator, validate_sdmx_csv
export generate_validation_report, preview_validation_output, is_valid_time_format

# === DATA QUERY & RETRIEVAL ===
# Functions for constructing queries and retrieving SDMX data from APIs
export construct_data_url, fetch_sdmx_data, query_sdmx_data, construct_sdmx_key, clean_sdmx_data, summarize_data

# === PIPELINE OPERATIONS & WORKFLOW ===
# Functional programming interface for chaining SDMX operations
export validate_with, chain, pipeline
export tap, branch, parallel_map, SDMXPipeline

# === PIPELINE OPERATORS (Unicode) ===
# Custom operators for expressive SDMX data pipeline construction
export ⊆, ⇒

# === UTILITY FUNCTIONS ===
# Helper functions for URL handling and XML processing
export is_url, normalize_sdmx_url, fetch_sdmx_xml

# === UNITS & CONVERSION ===
# Unitful integration, SDMX unit code mappings, and exchange rates
export SDMXUnitSpec, ExchangeRateTable, SDMX_UNIT_MAP
export sdmx_to_unitful, are_units_convertible, conversion_factor, unit_multiplier
export add_rate!, get_rate, convert_currency, default_exchange_rates

# === SCHEMA COMPARISON ===
# Comparing DataflowSchema objects for cross-dataflow analysis
export CodelistOverlap, SchemaComparison
export codelist_overlap, compare_schemas

# === UNIT CONFLICTS & HARMONIZATION ===
# Detecting and resolving unit mismatches across dataflows
export UnitConflict, UnitConflictReport
export detect_unit_conflicts, normalize_units!, harmonize_units

# === FREQUENCY ALIGNMENT ===
# Aligning time-series data across different frequencies
export FrequencyAlignment
export align_frequencies

# === CROSS-DATAFLOW JOIN ===
# Intelligent joining of SDMX DataFrames from different dataflows
export JoinResult
export detect_join_columns, sdmx_join

end # module SDMXer
