{# Fails for rows where lower_column > upper_column (both non-null). #}
{% test valid_range(model, lower_column, upper_column) %}
select *
from {{ model }}
where {{ lower_column }} is not null
  and {{ upper_column }} is not null
  and {{ lower_column }} > {{ upper_column }}
{% endtest %}
