{% load sql_tag %}
CREATE EXTERNAL TABLE {{ schema }} (
    {{ table_fields | join:",&#10;    " }}
)
ROW FORMAT SERDE '{{ serde }}'
WITH SERDEPROPERTIES (
{% if serde_properties %}
    {{ serde_properties|parameter_join }}
{% else %}
  -- default
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
{% endif %}
)
STORED AS {{ stored_as }}
LOCATION '{{ location }}'
TBLPROPERTIES (
{% spaceless %}
{%if table_properties %}{{ table_properties|parameter_join }}{% else %}
  -- default
  'skip.header.line.count'='1',
  'has_encrypted_data'='false'
{% endif %}
{% endspaceless %}
);