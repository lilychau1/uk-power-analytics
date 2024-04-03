from common.file_config import *

create_partitioned_generation_table_query = f"""
CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{bmrs_generation_table_name_partitioned}
PARTITION BY DATE(settlement_date) 
CLUSTER BY bm_unit AS
SELECT * FROM {BIGQUERY_DATASET}.{bmrs_generation_table_name} 
"""

create_aggregated_table_query = f"""
CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.generation_capacity_360_view
AS
SELECT      g.*
,           c.psr_type as bmrs_capacity_psr_type
,           bpi.fuel_type as bmrs_unit_fuel_type
,           case
                when c.psr_type not in ('Generation', 'Other') then coalesce(c.psr_type, bpi.fuel_type)
                else bpi.fuel_type
            end as inferred_fuel_type
,           case
                when c.psr_type not in ('Generation', 'Other') then pfc.new_psr_type
                else pfbpi.new_psr_type
            end as new_psr_type
,           case
                when c.psr_type not in ('Generation', 'Other') then pfc.fuel_category
                else pfbpi.fuel_category
            end as fuel_category
,           case
                when c.psr_type not in ('Generation', 'Other') then pfc.source_category
                else pfbpi.source_category
            end as source_category
,           c.installed_capacity
,           pid.name
,           ploc.longitude
,           ploc.latitude
,           ploc.city
,           ploc.county
,           ploc.gsp_name
FROM        {BIGQUERY_DATASET}.{bmrs_generation_table_name} g
LEFT JOIN   {BIGQUERY_DATASET}.{bmrs_capacity_table_name} c
ON          c.bm_unit = g.bm_unit

LEFT JOIN   {BIGQUERY_DATASET}.{power_plant_id_table_name} pid
on          pid.bm_unit = g.bm_unit

LEFT JOIN   {BIGQUERY_DATASET}.{power_plant_location_table_name} ploc
on          ploc.dictionary_id = pid.dictionary_id

LEFT JOIN   {BIGQUERY_DATASET}.{bmrs_power_plant_info_table_name} bpi
on          bpi.bm_unit = g.bm_unit

LEFT JOIN   {BIGQUERY_DATASET}.{psr_fuel_type_mapping_table_name} pfbpi
on          pfbpi.bm_fuel_type = bpi.fuel_type

LEFT JOIN   {BIGQUERY_DATASET}.{psr_fuel_type_mapping_table_name} pfc
on          pfc.psr_type = c.psr_type
"""