# SDMX Element Types

Type-specialized parsing system using @generated functions for compile-time optimization.

## Element Types

```@docs
SDMX.SDMXElement
SDMX.DimensionElement
SDMX.AttributeElement
SDMX.MeasureElement
SDMX.ConceptElement
SDMX.CodelistElement
SDMX.AvailabilityElement
SDMX.TimeElement
```

## Extraction Functions

```@docs
SDMX.extract_sdmx_element
SDMX.extract_generic_element
SDMX.extract_code_info
SDMX.get_xpath_patterns
```

## Demonstrations

```@docs
SDMX.demonstrate_generated_parsing
SDMX.create_benchmark_xml
SDMX.migration_guide
```