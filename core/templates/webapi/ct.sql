{% load sql_tag %}
CREATE EXTERNAL TABLE {{ schema }} (
    {{ table_fields | field_join  }}
)
ROW FORMAT SERDE '{{ serde }}'
WITH SERDEPROPERTIES (
{% if serde_properties %}{% autoescape off %}
    {{ serde_properties|parameter_join }}
{% endautoescape %}{% else %}
  -- default
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
{% endif %}
)
STORED AS {{ stored_as }}
LOCATION '{{ location }}'
TBLPROPERTIES (
{%if table_properties %}{% autoescape off %}
    {{ table_properties|parameter_join }}
{% endautoescape %}{% else %}
  -- default
  'skip.header.line.count'='1',
  'has_encrypted_data'='false'
{% endif %}
);