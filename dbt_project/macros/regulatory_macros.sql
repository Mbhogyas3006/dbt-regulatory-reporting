-- macros/generate_schema_name.sql
-- Custom schema naming: dev uses dev_<schema>, prod uses <schema>
-- This ensures dev runs don't overwrite prod regulatory tables

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}


-- macros/regulatory_macros.sql

-- Macro: Expected Loss calculation (reusable across models)
{% macro calculate_expected_loss(pd_col, lgd_col, ead_col) %}
    round({{ pd_col }} * {{ lgd_col }} * {{ ead_col }}, 2)
{% endmacro %}

-- Macro: Capital requirement at given RWA density
{% macro capital_requirement(rwa_col, min_ratio=0.08) %}
    round({{ rwa_col }} * {{ min_ratio }}, 2)
{% endmacro %}

-- Macro: Rating bucket assignment (standardized across models)
{% macro rating_bucket(rating_col) %}
    case
        when {{ rating_col }} in ('AAA','AA+','AA','AA-') then 'Investment Grade — High'
        when {{ rating_col }} in ('A+','A','A-')          then 'Investment Grade — Upper Mid'
        when {{ rating_col }} in ('BBB+','BBB','BBB-')    then 'Investment Grade — Lower Mid'
        when {{ rating_col }} in ('BB+','BB','BB-')       then 'Sub-Investment Grade — High'
        when {{ rating_col }} in ('B','CCC')              then 'Sub-Investment Grade — Low'
        else 'Unrated'
    end
{% endmacro %}

-- Macro: Basel III capital adequacy status
{% macro capital_adequacy_status(cet1_ratio_col) %}
    case
        when {{ cet1_ratio_col }} < {{ var('cet1_minimum_ratio') }}
            then 'Below Minimum — Regulatory Action Required'
        when {{ cet1_ratio_col }} < {{ var('cet1_minimum_ratio') }} + {{ var('capital_conservation_buffer') }}
            then 'Within Buffer — Restricted Distributions'
        when {{ cet1_ratio_col }} < {{ var('cet1_minimum_ratio') }} + {{ var('capital_conservation_buffer') }} + 2
            then 'Adequate — Monitor Closely'
        else 'Well Capitalised'
    end
{% endmacro %}
