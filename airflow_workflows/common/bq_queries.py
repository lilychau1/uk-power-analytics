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
,           coalesce(case
                when g.bm_unit like 'I_%' then 'Interconnector'
                when c.psr_type not in ('Generation', 'Other') then coalesce(c.psr_type, bpi.fuel_type)
                else bpi.fuel_type
            end, bo.psr_type) as inferred_fuel_type
,           coalesce(case
                when g.bm_unit like 'I_%' then 'Interconnector'
                when c.psr_type not in ('Generation', 'Other') then pfc.new_psr_type
                else pfbpi.new_psr_type
            end, pfbo.new_psr_type) as new_psr_type
,           coalesce(case
                when g.bm_unit like 'I_%' then 'Interconnector'
                when c.psr_type not in ('Generation', 'Other') then pfc.fuel_category
                else pfbpi.fuel_category
            end, pfbo.fuel_category) as fuel_category
,           coalesce(case
                when g.bm_unit like 'I_%' then 'Interconnector'
                when c.psr_type not in ('Generation', 'Other') then pfc.source_category
                else pfbpi.source_category
            end, pfbo.source_category) as source_category
,           c.installed_capacity
,           pid.name
,           coalesce(ploc.longitude, bo.longitude) as longitude
,           coalesce(ploc.latitude, bo.latitude) as latitude
,           ploc.city
,           ploc.county
,           ploc.gsp_name
FROM        {BIGQUERY_DATASET}.{bmrs_generation_table_name_partitioned} g
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

LEFT JOIN   {BIGQUERY_DATASET}.{bm_unit_info_overlay_table_name} bo
on          bo.bm_unit = g.bm_unit

LEFT JOIN   {BIGQUERY_DATASET}.{psr_fuel_type_mapping_table_name} pfbo
on          pfbo.psr_type = bo.psr_type

"""

create_generation_by_unit_by_settlement_date_table_query = f"""
CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{generation_by_unit_by_settlement_date_table_name}
PARTITION BY DATE(settlement_date) 
CLUSTER BY bm_unit AS

SELECT      bm_unit
,           settlement_date
,           sum(quantity) as daily_generation_mwh
FROM        {BIGQUERY_DATASET}.{bmrs_generation_table_name_partitioned}
WHERE       quantity > 0
group by    bm_unit, settlement_date
"""

create_fuel_wth_max_daily_gen_by_county_table_query = f"""
CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{fuel_with_max_daily_gen_by_county_table_name}
AS
WITH generation AS
(
    SELECT      g.bm_unit
    ,           g.settlement_date
    ,           g.daily_generation_mwh
    ,           coalesce(case
                    when g.bm_unit like 'I_%' then 'Interconnector'
                    when c.psr_type not in ('Generation', 'Other') then pfc.source_category
                    else pfbpi.source_category
                end, pfbo.source_category) as source_category
    ,           ploc.county
    FROM        {BIGQUERY_DATASET}.{generation_by_unit_by_settlement_date_table_name} g
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

    LEFT JOIN   {BIGQUERY_DATASET}.{bm_unit_info_overlay_table_name} bo
    on          bo.bm_unit = g.bm_unit

    LEFT JOIN   {BIGQUERY_DATASET}.{psr_fuel_type_mapping_table_name} pfbo
    on          pfbo.psr_type = bo.psr_type
),

generation_by_source_category AS
(
    SELECT      county
    ,           source_category
    ,           sum(daily_generation_mwh) as total_daily_generation_mwh
    FROM        generation
    WHERE       source_category <> 'Interconnector'
    GROUP BY    1, 2
),

generation_by_source_category_ranked_by_quantity AS
(
    SELECT      county
    ,           source_category
    ,           ROW_NUMBER() OVER(PARTITION BY county, source_category ORDER BY total_daily_generation_mwh DESC) as rn
    FROM        generation_by_source_category
)

SELECT  county
,       source_category
FROM    generation_by_source_category_ranked_by_quantity
WHERE   rn = 1

"""