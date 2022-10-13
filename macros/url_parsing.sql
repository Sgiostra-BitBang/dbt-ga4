{% macro extract_hostname_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '(?:http[s]?://)?(?:www\\.)?(.*?)(?:(?:/|:)(?:.)*|$)')
{% endmacro %}

{% macro extract_query_string_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '\\?(.+)')
{% endmacro %}

{% macro remove_query_parameters(url, parameters)%}
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE({{url}}, '(\\?|&)({{ parameters|join("|") }})=[^&]*', '\\1'), '\\?&+', '?'), '&+', '&'), '\\?$|&$', '')
{% endmacro %}


{% macro trim_trailing_slash(url, delimiter) %}
    {% if delimiter is none %}
        {{ return url|trim('/') }}
    {% else %}
        {% set pl_list = url|split(delimiter,1) %}
        {% set pl_list[0] = pl_list[0]|trim('/') %}
        {{ return pl_list|join() }}
    {% endif %}
{% endmacro %}