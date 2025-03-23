{% test assert_equal(model, actual, expected) %}
    select *
    from {{ model }}
    where
        abs({{ actual }} - {{ expected }}) > 0.0001
{% endtest %}

{% test assert_equal_string(model, actual, expected) %}
    select * from {{ model }} where {{ actual }} != {{ expected }}
{% endtest %}

{% test not_empty_string(model, column_name) %}
    select * from {{ model }} where {{ column_name }} = ''
{% endtest %}

{% test assert_close(model, actual, expected, decimal_place=2) %}
    select * from {{ model }} where round({{ actual }}::numeric, {{ decimal_place}}) != round({{ expected }}::numeric, {{ decimal_place }})
{% endtest %}

{% test assert_not_null(model, column) %}
    select * from {{ model }} where {{ column }} is null
{% endtest %}
