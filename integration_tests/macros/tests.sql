{% test assert_equal(model, actual, expected) %}
    select * from {{ model }} where {{ actual }} != {{ expected }}
{% endtest %}

{% test not_empty_string(model, column_name) %}
    select * from {{ model }} where {{ column_name }} = ''
{% endtest %}

{% test assert_close(model, actual, expected, decimal_place=2) %}
    select * from {{ model }} where round({{ actual }}::numeric, 2) != round({{ expected }}::numeric, 2)
{% endtest %}

{% test assert_not_null(model, column) %}
    select * from {{ model }} where column is null
{% endtest %}
