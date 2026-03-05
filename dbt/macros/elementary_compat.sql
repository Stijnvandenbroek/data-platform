-- Compatibility shim: dbt 1.11 changed macro resolution so unnamespaced intra-package
-- calls from elementary no longer resolve. Forwarding macro makes it findable globally.
{% macro get_upload_artifact_method(table_relation, metadata_hashes) %}
    {% do return(elementary.get_upload_artifact_method(table_relation, metadata_hashes)) %}
{% endmacro %}
