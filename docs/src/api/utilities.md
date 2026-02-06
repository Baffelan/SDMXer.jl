# Utility Functions

Helper functions for working with SDMX data and URLs.

## URL Utilities

```@docs
SDMXer.is_url
SDMXer.normalize_sdmx_url
SDMXer.fetch_sdmx_xml
```

## Time Utilities

```@docs
SDMXer.is_valid_time_format
SDMXer.get_time_period_values
SDMXer.get_time_period_range
```

## Node Processing

```@docs
SDMXer.process_code_node
SDMXer.get_parent_id
SDMXer.extract_codes_from_codelist_node
SDMXer.extract_availability_from_node
```

## Data Extraction

```@docs
SDMXer.extract_dimension_values
SDMXer.extract_time_availability
```
