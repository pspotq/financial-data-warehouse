{#- Generic test: no two rows share the same values across `columns` (a composite key).
    Usage in YAML:  data_tests: [{unique_combination: {columns: [symbol, date]}}]  -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
