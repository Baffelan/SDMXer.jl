"""
SDMX Unit System for SDMXer.jl

Provides Unitful.jl integration with SDMX unit code mappings:
- Maps SDMX UNIT_MEASURE codes to Unitful quantities
- Custom units for SDMX-specific measures (tonne, barrel)
- Currency dimension for compound unit algebra (USD/kg, FJD/tonne)
- Exchange rate table for cross-currency conversion
- UNIT_MULT power-of-10 handling

Note on currencies: All currencies are defined with a 1:1 factor to USD so that
Unitful's dimensional algebra works for compound units (e.g., USD/kg to USD/tonne).
Cross-currency conversion must go through ExchangeRateTable — never raw uconvert.
"""

using Unitful

# =================== CUSTOM UNITS ===================

# Tonne: Mg (megagram) exists in Unitful but "t" is conventional in SDMX
@unit sdmx_t "t" MetricTon 1000u"kg" false

# Barrel (oil): ~158.987 litres
@unit sdmx_bbl "bbl" Barrel (158987 // 1000) * u"L" false

# Hectare: already in Unitful as u"ha" — no custom definition needed

# =================== CURRENCY DIMENSION ===================

@dimension 𝐂 "𝐂" Currency
@refunit USD "USD" USDollar 𝐂 false
@unit EUR "EUR" Euro 1USD false
@unit FJD "FJD" FijiDollar 1USD false
@unit AUD "AUD" AustralianDollar 1USD false
@unit NZD "NZD" NewZealandDollar 1USD false
@unit GBP "GBP" BritishPound 1USD false
@unit JPY "JPY" JapaneseYen 1USD false
@unit PGK "PGK" PapuaNewGuineaKina 1USD false
@unit SBD "SBD" SolomonIslandsDollar 1USD false
@unit TOP "TOP" TonganPaanga 1USD false
@unit VUV "VUV" VanuatuVatu 1USD false
@unit WST "WST" SamoanTala 1USD false
@unit XPF "XPF" CFPFranc 1USD false
@unit CNY "CNY" ChineseYuan 1USD false
@unit INR "INR" IndianRupee 1USD false
@unit KRW "KRW" SouthKoreanWon 1USD false

# =================== LOCAL PROMOTION ===================

const localpromotion = Unitful.promote_unit(
    unit(1.0USD), unit(1.0EUR), unit(1.0FJD), unit(1.0AUD),
    unit(1.0NZD), unit(1.0GBP), unit(1.0JPY), unit(1.0PGK),
    unit(1.0SBD), unit(1.0TOP), unit(1.0VUV), unit(1.0WST),
    unit(1.0XPF), unit(1.0CNY), unit(1.0INR), unit(1.0KRW)
)

function __init__()
    Unitful.register(SDMXer)
end

# =================== TYPES ===================

"""
    SDMXUnitSpec

Maps an SDMX unit code to a Unitful unit with metadata.

# Fields
- `code::String`: The SDMX UNIT_MEASURE code (e.g., "KG", "T", "USD")
- `unit::Unitful.Units`: The corresponding Unitful unit
- `category::Symbol`: Unit category (:mass, :volume, :currency, :energy, :area, :dimensionless, :other)
- `description::String`: Human-readable description
"""
struct SDMXUnitSpec
    code::String
    unit::Unitful.Units
    category::Symbol
    description::String
end

"""
    ExchangeRateTable

Stores exchange rates for cross-currency conversion.

All rates are stored as domestic currency per 1 USD. Cross-rates are derived
automatically. Users can override rates with `add_rate!`.

# Fields
- `rates::Dict{Tuple{String,String}, Float64}`: Map from (from, to) currency pair to rate
- `reference_date::String`: Reference date for the rates
- `source::String`: Source description for the rates

# Examples
```julia
table = ExchangeRateTable()
add_rate!(table, "USD", "FJD", 2.299)
add_rate!(table, "USD", "AUD", 1.552)
rate = get_rate(table, "FJD", "AUD")
```
"""
mutable struct ExchangeRateTable
    rates::Dict{Tuple{String, String}, Float64}
    reference_date::String
    source::String
end

ExchangeRateTable() = ExchangeRateTable(
    Dict{Tuple{String, String}, Float64}(), "", ""
)

ExchangeRateTable(reference_date::String, source::String) = ExchangeRateTable(
    Dict{Tuple{String, String}, Float64}(), reference_date, source
)

# =================== SDMX UNIT MAP ===================

"""
    SDMX_UNIT_MAP

Maps ~30 common SDMX UNIT_MEASURE codes to SDMXUnitSpec.
Covers SPC Pacific Data Hub codes and common international codes.
"""
const SDMX_UNIT_MAP = Dict{String, SDMXUnitSpec}(
    # Mass
    "KG" => SDMXUnitSpec("KG", u"kg", :mass, "Kilogram"),
    "T" => SDMXUnitSpec("T", sdmx_t, :mass, "Metric tonne"),
    "MT" => SDMXUnitSpec("MT", sdmx_t, :mass, "Metric tonne (alternative code)"),
    "G" => SDMXUnitSpec("G", u"g", :mass, "Gram"),
    "LB" => SDMXUnitSpec("LB", u"lb", :mass, "Pound"),
    # Volume
    "L" => SDMXUnitSpec("L", u"L", :volume, "Litre"),
    "ML" => SDMXUnitSpec("ML", u"mL", :volume, "Millilitre"),
    "BBL" => SDMXUnitSpec("BBL", sdmx_bbl, :volume, "Barrel (oil)"),
    "M3" => SDMXUnitSpec("M3", u"m^3", :volume, "Cubic metre"),
    # Area
    "HA" => SDMXUnitSpec("HA", u"ha", :area, "Hectare"),
    "KM2" => SDMXUnitSpec("KM2", u"km^2", :area, "Square kilometre"),
    # Energy
    "KWH" => SDMXUnitSpec("KWH", u"kW*hr", :energy, "Kilowatt-hour"),
    "MWH" => SDMXUnitSpec("MWH", u"MW*hr", :energy, "Megawatt-hour"),
    "GJ" => SDMXUnitSpec("GJ", u"GJ", :energy, "Gigajoule"),
    # Currencies
    "USD" => SDMXUnitSpec("USD", USD, :currency, "US Dollar"),
    "EUR" => SDMXUnitSpec("EUR", EUR, :currency, "Euro"),
    "FJD" => SDMXUnitSpec("FJD", FJD, :currency, "Fiji Dollar"),
    "AUD" => SDMXUnitSpec("AUD", AUD, :currency, "Australian Dollar"),
    "NZD" => SDMXUnitSpec("NZD", NZD, :currency, "New Zealand Dollar"),
    "GBP" => SDMXUnitSpec("GBP", GBP, :currency, "British Pound"),
    "JPY" => SDMXUnitSpec("JPY", JPY, :currency, "Japanese Yen"),
    "PGK" => SDMXUnitSpec("PGK", PGK, :currency, "Papua New Guinea Kina"),
    "SBD" => SDMXUnitSpec("SBD", SBD, :currency, "Solomon Islands Dollar"),
    "TOP" => SDMXUnitSpec("TOP", TOP, :currency, "Tongan Paanga"),
    "VUV" => SDMXUnitSpec("VUV", VUV, :currency, "Vanuatu Vatu"),
    "WST" => SDMXUnitSpec("WST", WST, :currency, "Samoan Tala"),
    "XPF" => SDMXUnitSpec("XPF", XPF, :currency, "CFP Franc"),
    # Dimensionless / index
    "PT" => SDMXUnitSpec("PT", Unitful.NoUnits, :dimensionless, "Percentage point"),
    "PC" => SDMXUnitSpec("PC", Unitful.NoUnits, :dimensionless, "Percentage"),
    "NUM" => SDMXUnitSpec("NUM", Unitful.NoUnits, :dimensionless, "Number (count)"),
    "IDX" => SDMXUnitSpec("IDX", Unitful.NoUnits, :dimensionless, "Index"),
    "PS" => SDMXUnitSpec("PS", Unitful.NoUnits, :dimensionless, "Persons"),
)

# =================== CORE FUNCTIONS ===================

"""
    sdmx_to_unitful(code::String) -> Union{SDMXUnitSpec, Nothing}

Look up an SDMX UNIT_MEASURE code and return its SDMXUnitSpec, or `nothing` if unknown.

# Examples
```julia
spec = sdmx_to_unitful("KG")
spec.unit  # kg
spec.category  # :mass

sdmx_to_unitful("UNKNOWN")  # nothing
```
"""
function sdmx_to_unitful(code::String)
    return get(SDMX_UNIT_MAP, uppercase(code), nothing)
end

"""
    are_units_convertible(code_a::String, code_b::String) -> Bool

Check whether two SDMX unit codes are convertible via Unitful dimensional analysis.
Returns `false` for cross-currency pairs (same dimension but conversion requires rates).

# Examples
```julia
are_units_convertible("KG", "T")   # true  — same mass dimension
are_units_convertible("KG", "L")   # false — mass vs volume
are_units_convertible("USD", "EUR") # false — currencies need exchange rates
```
"""
function are_units_convertible(code_a::String, code_b::String)
    spec_a = sdmx_to_unitful(code_a)
    spec_b = sdmx_to_unitful(code_b)
    isnothing(spec_a) && return false
    isnothing(spec_b) && return false
    # Block cross-currency conversion — must go through ExchangeRateTable
    if spec_a.category === :currency || spec_b.category === :currency
        return spec_a.code == spec_b.code
    end
    # Use Unitful dimension check
    return Unitful.dimension(spec_a.unit) == Unitful.dimension(spec_b.unit)
end

"""
    conversion_factor(from::String, to::String) -> Union{Float64, Nothing}

Return the deterministic conversion factor from one SDMX unit to another.
Returns `nothing` for currencies (use ExchangeRateTable instead) or incompatible units.

# Examples
```julia
conversion_factor("KG", "T")   # 0.001
conversion_factor("T", "KG")   # 1000.0
conversion_factor("USD", "EUR") # nothing — use exchange rates
conversion_factor("KG", "L")   # nothing — incompatible dimensions
```
"""
function conversion_factor(from::String, to::String)
    from == to && return 1.0
    are_units_convertible(from, to) || return nothing
    spec_from = sdmx_to_unitful(from)
    spec_to = sdmx_to_unitful(to)
    # Both are known and convertible (and not cross-currency)
    return Float64(Unitful.ustrip(Unitful.uconvert(spec_to.unit, 1.0 * spec_from.unit)))
end

"""
    unit_multiplier(mult_code) -> Float64

Convert an SDMX UNIT_MULT code (power-of-10 exponent) to its numeric multiplier.

Common values: 0 → 1, 3 → 1000, 6 → 1e6, 9 → 1e9

# Examples
```julia
unit_multiplier(0)    # 1.0
unit_multiplier(3)    # 1000.0
unit_multiplier(6)    # 1.0e6
unit_multiplier("3")  # 1000.0
unit_multiplier(nothing)  # 1.0
unit_multiplier(missing)  # 1.0
```
"""
function unit_multiplier(mult_code)
    (ismissing(mult_code) || isnothing(mult_code)) && return 1.0
    exp_val = mult_code isa AbstractString ? parse(Int, mult_code) : Int(mult_code)
    return 10.0^exp_val
end

# =================== EXCHANGE RATE TABLE ===================

"""
    add_rate!(table::ExchangeRateTable, from::String, to::String, rate::Float64)

Add an exchange rate to the table. Automatically adds the inverse rate.

# Examples
```julia
table = ExchangeRateTable()
add_rate!(table, "USD", "FJD", 2.299)
# Now table has both USD→FJD (2.299) and FJD→USD (1/2.299)
```
"""
function add_rate!(table::ExchangeRateTable, from::String, to::String, rate::Float64)
    table.rates[(from, to)] = rate
    table.rates[(to, from)] = 1.0 / rate
    return table
end

"""
    get_rate(table::ExchangeRateTable, from::String, to::String) -> Union{Float64, Nothing}

Get the exchange rate from one currency to another. Tries direct lookup first,
then derives cross-rate via USD if both USD-based rates exist.

# Examples
```julia
table = default_exchange_rates()
get_rate(table, "USD", "FJD")  # 2.299
get_rate(table, "FJD", "AUD")  # derived cross-rate
get_rate(table, "XXX", "YYY")  # nothing
```
"""
function get_rate(table::ExchangeRateTable, from::String, to::String)
    from == to && return 1.0
    # Direct lookup
    direct = get(table.rates, (from, to), nothing)
    !isnothing(direct) && return direct
    # Try cross-rate via USD
    from_usd = get(table.rates, (from, "USD"), nothing)
    usd_to = get(table.rates, ("USD", to), nothing)
    if !isnothing(from_usd) && !isnothing(usd_to)
        return from_usd * usd_to
    end
    return nothing
end

"""
    convert_currency(value::Real, from::String, to::String, table::ExchangeRateTable) -> Union{Float64, Nothing}

Convert a monetary value from one currency to another using the exchange rate table.
Returns `nothing` if no rate is available.

# Examples
```julia
table = default_exchange_rates()
convert_currency(100.0, "USD", "FJD", table)  # ≈ 229.9
convert_currency(100.0, "FJD", "AUD", table)  # derived via cross-rate
```
"""
function convert_currency(value::Real, from::String, to::String, table::ExchangeRateTable)
    rate = get_rate(table, from, to)
    isnothing(rate) && return nothing
    return Float64(value * rate)
end

"""
    default_exchange_rates() -> ExchangeRateTable

Return an ExchangeRateTable pre-populated with IMF 2025 annual period-average rates
(domestic currency per 1 USD).

These are approximate default rates intended for quick analysis. For production use,
supply your own rates via `add_rate!` or build a fresh table.

| Currency | Per 1 USD | Year |
|----------|-----------|------|
| AUD      | 1.5520    | 2025 |
| EUR      | 0.8850    | 2025 |
| FJD      | 2.2990    | 2025 |
| GBP      | 0.7595    | 2025 |
| JPY      | 149.66    | 2025 |
| NZD      | 1.7201    | 2025 |
| PGK      | 4.1227    | 2025 |
| SBD      | 8.3268    | 2025 |
| TOP      | 2.3730    | 2024 |
| VUV      | 119.17    | 2024 |
| WST      | 2.7915    | 2025 |
"""
function default_exchange_rates()
    table = ExchangeRateTable("2025", "IMF period-average (approximate defaults — may be stale)")
    add_rate!(table, "USD", "AUD", 1.5520)
    add_rate!(table, "USD", "EUR", 0.8850)
    add_rate!(table, "USD", "FJD", 2.2990)
    add_rate!(table, "USD", "GBP", 0.7595)
    add_rate!(table, "USD", "JPY", 149.66)
    add_rate!(table, "USD", "NZD", 1.7201)
    add_rate!(table, "USD", "PGK", 4.1227)
    add_rate!(table, "USD", "SBD", 8.3268)
    add_rate!(table, "USD", "TOP", 2.3730)
    add_rate!(table, "USD", "VUV", 119.17)
    add_rate!(table, "USD", "WST", 2.7915)
    add_rate!(table, "USD", "XPF", 105.42)
    add_rate!(table, "USD", "CNY", 7.2450)
    add_rate!(table, "USD", "INR", 84.50)
    add_rate!(table, "USD", "KRW", 1380.0)
    return table
end
