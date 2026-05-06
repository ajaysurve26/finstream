{% macro set_azure_config() %}
    {% set storage_key = env_var('AZURE_STORAGE_KEY') %}
    {% do run_query("SET fs.azure.account.key.finstreamdatalake.dfs.core.windows.net=" ~ storage_key) %}
{% endmacro %}