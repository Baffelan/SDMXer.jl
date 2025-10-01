# Utility Functions

Helper functions for working with SDMX data and URLs.

## URL Utilities

```@docs
SDMX.is_url
SDMX.normalize_sdmx_url
SDMX.fetch_sdmx_xml
```

## Time Utilities

```@docs
SDMX.is_valid_time_format
SDMX.get_time_period_values
SDMX.get_time_period_range
```

## Node Processing

```@docs
SDMX.process_code_node
SDMX.get_parent_id
SDMX.extract_codes_from_codelist_node
SDMX.extract_availability_from_node
```

## Data Extraction

```@docs
SDMX.extract_dimension_values
SDMX.extract_time_availability
```