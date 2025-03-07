{#
    this macro returns desc. of payment type
#}
{% macro get_payment_type_description(payment_type) -%}
    case CAST({{ dbt.safe_cast("payment_type", api.Column.translate_type("float64")) }} AS integer)
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'EMPTY'
    end

{%- endmacro %}