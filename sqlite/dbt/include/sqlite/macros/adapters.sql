{% macro sqlite__create_table_as(temporary, relation, sql) -%}
  create {% if temporary -%}
    temporary
  {%- endif %} table {{ relation }}
  as
  {{ sql }}
{%- endmacro %}

{% macro sqlite__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    attach '{{ relation }}.db' as {{ relation }}
  {%- endcall -%}
{% endmacro %}

{% macro sqlite__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    detach {{ relation }}
  {%- endcall -%}
{% endmacro %}

{% macro sqlite__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          cid,
          name,
          type,
          notnull,
          dflt_value,
          pk
      from pragma_table_info('{{ relation }}')
      order by cid
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro sqlite__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
      select
          cid,
          name,
          type,
          notnull,
          dflt_value,
          pk
      from pragma_table_info('{{ relation }}')
      order by cid
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro sqlite__information_schema_name(database) -%}
  {% if database_name -%}
    {{ adapter.verify_database(database_name) }}
  {%- endif -%}
  sqlite_master
{%- endmacro %}

{% macro sqlite__list_schemas(database) %}
  {% if database -%}
    {{ adapter.verify_database(database) }}
  {%- endif -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct name from pragma_database_list
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro sqlite__check_schema_exists(information_schema, schema) -%}
  {% if information_schema.database -%}
    {{ adapter.verify_database(information_schema.database) }}
  {%- endif -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    select count(*) from pragma_database_list where name = '{{ schema }}'
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro sqlite__current_timestamp() -%}
  datetime()
{%- endmacro %}

{% macro sqlite__snapshot_string_as_time(timestamp) -%}
    {%- set result = "cast('" ~ timestamp ~ "' as time)" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro sqlite__snapshot_get_time() -%}
  time()
{%- endmacro %}

{% macro sqlite__alter_column_type(relation, column_dict) %}
  {% for column_name in column_dict %}
    {% set comment = column_dict[column_name]['description'] %}
    {% set escaped_comment = sqlite_escape_comment(comment) %}
    comment on column {{ relation }}.{{ column_name }} is {{ escaped_comment }};
  {% endfor %}
{% endmacro %}

{% macro sqlite__rename_relation(relation, to_name) %}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    alter table {{ relation }} rename to {{ to_name }}
  {% endcall %}
{% endmacro %}

{% macro sqlite__drop_relation(relation, to_name) %}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    drop table {{ relation }}
  {% endcall %}
{% endmacro %}

{% macro sqlite__truncate_relation(relation) %}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    delete from {{ relation }}
  {% endcall %}
{% endmacro %}