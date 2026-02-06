"""
General helper functions for SDMXer.jl
"""


"""
    is_url(input::String) -> Bool

Detects if a string represents a URL using robust pattern matching.

This function uses multiple patterns to identify various URL formats commonly
encountered when working with SDMX APIs. It distinguishes URLs from XML content
to enable automatic handling of different input types.

# Arguments
- `input::String`: String to test for URL format

# Returns
- `Bool`: true if input appears to be a URL, false otherwise

# Examples
```julia
# Various URL formats recognized
is_url("https://example.com")  # true
is_url("http://stats.pacificdata.org/rest/")  # true
is_url("www.example.com")  # true
is_url("example.com/data")  # true
is_url("ftp://files.example.com")  # true

# Non-URL content rejected
is_url("<xml>content</xml>")  # false
is_url("plain text")  # false
is_url("")  # AssertionError
```

# Throws
- `AssertionError`: If input string is empty

# See also
[`normalize_sdmx_url`](@ref), [`fetch_sdmx_xml`](@ref)
"""
function is_url(input::String)
    @assert !isempty(input) "Input string cannot be empty"
    
    # Convert to lowercase for pattern matching
    lower_input = lowercase(strip(input))
    
    # Pattern 1: Explicit protocol
    if occursin(r"^(https?|ftp)://", lower_input)
        return true
    end
    
    # Pattern 2: Starts with www.
    if occursin(r"^www\.", lower_input)
        return true
    end
    
    # Pattern 3: Domain-like pattern (has dot and looks like domain)
    # Must have at least one dot, and not look like XML content
    if occursin(r"^[a-zA-Z0-9][a-zA-Z0-9\-]*\.[a-zA-Z0-9\-\.]+(/.*)?$", lower_input) && 
       !occursin("<", input)  # Exclude XML content
        return true
    end
    
    return false
end

"""
    normalize_sdmx_url(url::String) -> String

Normalizes URLs for SDMX API compatibility with required parameters.

This function ensures URLs have proper protocols and includes the SDMX
`references=all` parameter needed for complete structure retrieval. It handles
various URL formats and existing query parameters correctly.

# Arguments
- `url::String`: URL to normalize (must be a valid URL)

# Returns
- `String`: Normalized URL with protocol and SDMX parameters

# Examples
```julia
# Add missing protocol
normalize_sdmx_url("stats.pacificdata.org/rest/datastructure")
# Returns: "https://stats.pacificdata.org/rest/datastructure?references=all"

# Add SDMX parameter to existing URL
normalize_sdmx_url("https://api.example.com/structure")
# Returns: "https://api.example.com/structure?references=all"

# Handle existing query parameters
normalize_sdmx_url("https://api.example.com/structure?format=xml")
# Returns: "https://api.example.com/structure?format=xml&references=all"

# Already normalized URLs unchanged
normalize_sdmx_url("https://api.example.com/structure?references=all")
# Returns: "https://api.example.com/structure?references=all"
```

# Throws
- `AssertionError`: If URL is empty or not a valid URL format

# See also
[`is_url`](@ref), [`fetch_sdmx_xml`](@ref)
"""
function normalize_sdmx_url(url::String)
    @assert !isempty(url) "URL cannot be empty"
    @assert is_url(url) "Input must be a valid URL"
    
    normalized_url = strip(url)
    
    # Add protocol if missing
    if !occursin(r"^(https?|ftp)://", lowercase(normalized_url))
        if startswith(lowercase(normalized_url), "ftp")
            normalized_url = "ftp://" * normalized_url
        else
            normalized_url = "https://" * normalized_url
        end
    end
    
    # Check if references=all parameter is already present
    if occursin(r"[?&]references=all", lowercase(normalized_url))
        return normalized_url  # Already has the parameter
    end
    
    # Add references=all parameter
    if occursin("?", normalized_url)
        # URL already has query parameters
        normalized_url *= "&references=all"
    else
        # Add query parameters
        normalized_url *= "?references=all"
    end
    
    return normalized_url
end

"""
    fetch_sdmx_xml(input::String) -> String

Fetches SDMX XML content from URLs or validates XML strings.

This function provides unified handling for SDMX content retrieval, automatically
detecting whether the input is a URL (which it fetches) or XML content (which it
validates and returns). URLs are automatically normalized for SDMX compatibility.

# Arguments
- `input::String`: Either a URL to fetch from or XML content string

# Returns
- `String`: XML content from URL or validated input XML string

# Examples
```julia
# Fetch from SDMX REST API
xml_content = fetch_sdmx_xml("https://stats-sdmx-disseminate.pacificdata.org/rest/datastructure/SPC/DF_BP50")

# Handle various URL formats
xml_content = fetch_sdmx_xml("stats.pacificdata.org/rest/datastructure/SPC/DF_BP50")

# Pass through XML content
xml_string = "<?xml version=\\"1.0\\"?>...>"
xml_content = fetch_sdmx_xml(xml_string)  # Returns xml_string unchanged

# Error handling
try
    xml_content = fetch_sdmx_xml(invalid_url)
catch e
    println("Failed to fetch XML: ", e)
end
```

# Throws
- `AssertionError`: For empty input, HTTP failures, empty responses, or invalid XML

# See also
[`is_url`](@ref), [`normalize_sdmx_url`](@ref)
"""
function fetch_sdmx_xml(input::String)
    @assert !isempty(input) "Input cannot be empty"
    
    if is_url(input)
        # It's a URL - normalize and fetch
        normalized_url = normalize_sdmx_url(input)
        
        response = HTTP.get(normalized_url; require_ssl_verification=false)
        @assert response.status == 200 string("HTTP request failed with status: ", response.status, " for URL: ", normalized_url)
        
        xml_string = String(response.body)
        @assert !isempty(xml_string) "HTTP response body cannot be empty"
        
        return xml_string
    elseif isfile(input)
        # It's a file path - read the file
        return read(input, String)
    elseif occursin("<", input)
        # It's XML content - validate and return
        return input
    else
        error("Input doesn't appear to be valid XML, URL, or file path")
    end
end 