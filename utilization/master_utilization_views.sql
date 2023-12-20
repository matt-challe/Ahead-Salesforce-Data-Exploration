USE WAREHOUSE COMPUTE_WH;
USE ROLE SYSADMIN;
USE DATABASE internal_salesforce_data_analytics; 
USE SCHEMA utilization_analytics;

---MASTER 2023 UTILIZATION CREATION
CREATE OR REPLACE VIEW master_utilization_2023 AS 
WITH prep_util_calc AS (

    SELECT 
        id AS util_calc_id,
        pse_time_period_c AS time_period_id,
        name AS acs_date_param, --Hist or Sched and Month or Week and dates
        pse_scheduled_utilization_end_date_c,
        pse_historical_utilization_end_date_c
        

    FROM salesforce_database.salesforce.pse_utilization_calculation_c
    WHERE is_deleted ILIKE 'False' --remove deteled calcs
    AND time_period_id IS NOT NULL --remove NULLS
    AND pse_scheduled_utilization_end_date_c IS NOT NULL
    AND pse_historical_utilization_end_date_c IS NOT NULL
    AND pse_time_period_types_c ILIKE 'Month'
    
        
),

--prepare pse_utilization_detail_c
prep_util_detail AS(

    SELECT
        pse_utilization_calculation_c AS util_calc_id,
        pse_resource_c AS resource_id,
        resource_role_c AS resource_role,
        utilization_target_c AS util_target,
        adjusted_utilization_target_c AS adjusted_util_target,
        ahead_practice_text_c AS practice,
        resource_blp_c AS resource_price,
        pse_historical_billable_hours_c,
        pse_historical_calendar_hours_c
        
    FROM salesforce_database.salesforce.pse_utilization_detail_c
    WHERE is_deleted ILIKE 'False' --remove deteled 
    AND DATE_PART(YEAR, TO_DATE(last_modified_date)) = 2023
    AND pse_time_period_type_c ILIKE 'Month'
),

--prepare resources
prep_resource_actuals AS (
    SELECT 
        pse_resource_c AS resource_id,
        SPLIT_PART(name, ' - ', 1) AS consultant_name
    FROM salesforce_database.salesforce.pse_resource_actuals_c
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY resource_id) = 1
),
---join calculation id with utilization table
staging_A AS (
    SELECT 
        *
    FROM prep_util_detail
    LEFT OUTER JOIN prep_util_calc ON prep_util_detail.util_calc_id = prep_util_calc.util_calc_id
),
--- join staging A with resources
staging_B AS (
    SELECT 
        *
    FROM prep_resource_actuals
    RIGHT OUTER JOIN staging_A ON staging_A.resource_id = prep_resource_actuals.resource_id
),
--- remove resources with no utilization target
filter AS (
    SELECT * 
    FROM staging_B
    WHERE util_target >0
    AND adjusted_util_target>0
),
-- aggregate ahead practices
agg_ahead_practice AS (
    SELECT 
        *,
        CASE 
            WHEN practice ILIKE '%advisory%' THEN 'Advisory' --done
            WHEN practice ILIKE '%appdev%' OR practice ILIKE '%application%' THEN 'AppDev' --done
            WHEN practice ILIKE '%cloud%' THEN 'Cloud' -- done
            WHEN practice ILIKE '%next gen data%' THEN 'Next Gen Data Center PMs' --done
            WHEN practice ILIKE '%Digital Solutions%' THEN 'Digital Solutions PMs'
            WHEN practice ILIKE '%data%' THEN 'Data'-- done
            WHEN practice ILIKE '%ema%' THEN 'EMA'
            WHEN practice ILIKE '%esm%' THEN 'ESM' --done
            WHEN practice ILIKE '%euc%' THEN 'EUC' --done
            WHEN practice ILIKE '%ms%' OR practice ILIKE '%managed%' THEN 'Managed Services'
            WHEN practice ILIKE '%mod%' THEN 'Modern Infrastructure'--done
            WHEN practice ILIKE '%network%' THEN 'Network'--done
            WHEN practice ILIKE '%security%' AND practice NOT ILIKE '%ms%' THEN 'Security'
            WHEN practice ILIKE '%DevOps%' THEN 'DevOps'
            WHEN practice ILIKE '%ahead%' THEN 'AHEAD Services'
            WHEN practice ILIKE '%TSA%' THEN 'TSA'
            WHEN practice ILIKE 'Products' THEN 'Products'
            
            ELSE practice
        END AS ahead_practice
    FROM filter
),
util_calc AS(
    SELECT  
        consultant_name,
        resource_role,
        resource_price,
        ahead_practice,
        SPLIT_PART(acs_date_param, '_', 5) AS month,
        util_target,
        adjusted_util_target,
        ROUND((pse_historical_billable_hours_c/NULLIF(pse_historical_calendar_hours_c,0))*100, 1) AS effective_utilization
    FROM agg_ahead_practice
    WHERE acs_date_param ILIKE 'ACS_Historical_Monthly%'
)
SELECT * FROM util_calc
ORDER BY ahead_practice,consultant_name,month;
------------------------------------------------------------------------------------------------------------------------------------------

---MASTER 2022 UTILIZATION CREATION
CREATE OR REPLACE VIEW master_utilization_2022 AS 
WITH prep_util_calc AS (

    SELECT 
        id AS util_calc_id,
        pse_time_period_c AS time_period_id,
        name AS acs_date_param, --Hist or Sched and Month or Week and dates
        pse_scheduled_utilization_end_date_c,
        pse_historical_utilization_end_date_c
        

    FROM salesforce_database.salesforce.pse_utilization_calculation_c
    WHERE is_deleted ILIKE 'False' --remove deteled calcs
    AND time_period_id IS NOT NULL --remove NULLS
    AND pse_scheduled_utilization_end_date_c IS NOT NULL
    AND pse_historical_utilization_end_date_c IS NOT NULL
    AND pse_time_period_types_c ILIKE 'Month'
    
        
),

--prepare pse_utilization_detail_c
prep_util_detail AS(

    SELECT
        pse_utilization_calculation_c AS util_calc_id,
        pse_resource_c AS resource_id,
        resource_role_c AS resource_role,
        utilization_target_c AS util_target,
        adjusted_utilization_target_c AS adjusted_util_target,
        ahead_practice_text_c AS practice,
        resource_blp_c AS resource_price,
        pse_historical_billable_hours_c,
        pse_historical_calendar_hours_c
        
    FROM salesforce_database.salesforce.pse_utilization_detail_c
    WHERE is_deleted ILIKE 'False' --remove deteled 
    AND DATE_PART(YEAR, TO_DATE(last_modified_date)) = 2022
    AND pse_time_period_type_c ILIKE 'Month'
),

--prepare resources
prep_resource_actuals AS (
    SELECT 
        pse_resource_c AS resource_id,
        SPLIT_PART(name, ' - ', 1) AS consultant_name
    FROM salesforce_database.salesforce.pse_resource_actuals_c
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY resource_id) = 1
),
---join calculation id with utilization table
staging_A AS (
    SELECT 
        *
    FROM prep_util_detail
    LEFT OUTER JOIN prep_util_calc ON prep_util_detail.util_calc_id = prep_util_calc.util_calc_id
),
--- join staging A with resources
staging_B AS (
    SELECT 
        *
    FROM prep_resource_actuals
    RIGHT OUTER JOIN staging_A ON staging_A.resource_id = prep_resource_actuals.resource_id
),
--- remove resources with no utilization target
filter AS (
    SELECT * 
    FROM staging_B
    WHERE util_target >0
    AND adjusted_util_target>0
),
-- aggregate ahead practices
agg_ahead_practice AS (
    SELECT 
        *,
        CASE 
            WHEN practice ILIKE '%advisory%' THEN 'Advisory' --done
            WHEN practice ILIKE '%appdev%' OR practice ILIKE '%application%' THEN 'AppDev' --done
            WHEN practice ILIKE '%cloud%' THEN 'Cloud' -- done
            WHEN practice ILIKE '%next gen data%' THEN 'Next Gen Data Center PMs' --done
            WHEN practice ILIKE '%Digital Solutions%' THEN 'Digital Solutions PMs'
            WHEN practice ILIKE '%data%' THEN 'Data'-- done
            WHEN practice ILIKE '%ema%' THEN 'EMA'
            WHEN practice ILIKE '%esm%' THEN 'ESM' --done
            WHEN practice ILIKE '%euc%' THEN 'EUC' --done
            WHEN practice ILIKE '%ms%' OR practice ILIKE '%managed%' THEN 'Managed Services'
            WHEN practice ILIKE '%mod%' THEN 'Modern Infrastructure'--done
            WHEN practice ILIKE '%network%' THEN 'Network'--done
            WHEN practice ILIKE '%security%' AND practice NOT ILIKE '%ms%' THEN 'Security'
            WHEN practice ILIKE '%DevOps%' THEN 'DevOps'
            WHEN practice ILIKE '%ahead%' THEN 'AHEAD Services'
            WHEN practice ILIKE '%TSA%' THEN 'TSA'
            WHEN practice ILIKE 'Products' THEN 'Products'
            
            ELSE practice
        END AS ahead_practice
    FROM filter
),
util_calc AS(
    SELECT  
        consultant_name,
        resource_role,
        resource_price,
        ahead_practice,
        SPLIT_PART(acs_date_param, '_', 5) AS month,
        util_target,
        adjusted_util_target,
        ROUND((pse_historical_billable_hours_c/NULLIF(pse_historical_calendar_hours_c,0))*100, 1) AS effective_utilization
    FROM agg_ahead_practice
    WHERE acs_date_param ILIKE 'ACS_Historical_Monthly%'
)
SELECT * FROM util_calc
ORDER BY ahead_practice,consultant_name,month;
------------------------------------------------------------------------------------------------------------------------------------------

---MASTER 2021 UTILIZATION CREATION
CREATE OR REPLACE VIEW master_utilization_2021 AS 
WITH prep_util_calc AS (

    SELECT 
        id AS util_calc_id,
        pse_time_period_c AS time_period_id,
        name AS acs_date_param, --Hist or Sched and Month or Week and dates
        pse_scheduled_utilization_end_date_c,
        pse_historical_utilization_end_date_c
        

    FROM salesforce_database.salesforce.pse_utilization_calculation_c
    WHERE is_deleted ILIKE 'False' --remove deteled calcs
    AND time_period_id IS NOT NULL --remove NULLS
    AND pse_scheduled_utilization_end_date_c IS NOT NULL
    AND pse_historical_utilization_end_date_c IS NOT NULL
    AND pse_time_period_types_c ILIKE 'Month'
    
        
),

--prepare pse_utilization_detail_c
prep_util_detail AS(

    SELECT
        pse_utilization_calculation_c AS util_calc_id,
        pse_resource_c AS resource_id,
        resource_role_c AS resource_role,
        utilization_target_c AS util_target,
        adjusted_utilization_target_c AS adjusted_util_target,
        ahead_practice_text_c AS practice,
        resource_blp_c AS resource_price,
        pse_historical_billable_hours_c,
        pse_historical_calendar_hours_c
        
    FROM salesforce_database.salesforce.pse_utilization_detail_c
    WHERE is_deleted ILIKE 'False' --remove deteled 
    AND DATE_PART(YEAR, TO_DATE(last_modified_date)) = 2021
    AND pse_time_period_type_c ILIKE 'Month'
),

--prepare resources
prep_resource_actuals AS (
    SELECT 
        pse_resource_c AS resource_id,
        SPLIT_PART(name, ' - ', 1) AS consultant_name
    FROM salesforce_database.salesforce.pse_resource_actuals_c
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY resource_id) = 1
),
---join calculation id with utilization table
staging_A AS (
    SELECT 
        *
    FROM prep_util_detail
    LEFT OUTER JOIN prep_util_calc ON prep_util_detail.util_calc_id = prep_util_calc.util_calc_id
),
--- join staging A with resources
staging_B AS (
    SELECT 
        *
    FROM prep_resource_actuals
    RIGHT OUTER JOIN staging_A ON staging_A.resource_id = prep_resource_actuals.resource_id
),
--- remove resources with no utilization target
filter AS (
    SELECT * 
    FROM staging_B
    WHERE util_target >0
    AND adjusted_util_target>0
),
-- aggregate ahead practices
agg_ahead_practice AS (
    SELECT 
        *,
        CASE 
            WHEN practice ILIKE '%advisory%' THEN 'Advisory' --done
            WHEN practice ILIKE '%appdev%' OR practice ILIKE '%application%' THEN 'AppDev' --done
            WHEN practice ILIKE '%cloud%' THEN 'Cloud' -- done
            WHEN practice ILIKE '%next gen data%' THEN 'Next Gen Data Center PMs' --done
            WHEN practice ILIKE '%Digital Solutions%' THEN 'Digital Solutions PMs'
            WHEN practice ILIKE '%data%' THEN 'Data'-- done
            WHEN practice ILIKE '%ema%' THEN 'EMA'
            WHEN practice ILIKE '%esm%' THEN 'ESM' --done
            WHEN practice ILIKE '%euc%' THEN 'EUC' --done
            WHEN practice ILIKE '%ms%' OR practice ILIKE '%managed%' THEN 'Managed Services'
            WHEN practice ILIKE '%mod%' THEN 'Modern Infrastructure'--done
            WHEN practice ILIKE '%network%' THEN 'Network'--done
            WHEN practice ILIKE '%security%' AND practice NOT ILIKE '%ms%' THEN 'Security'
            WHEN practice ILIKE '%DevOps%' THEN 'DevOps'
            WHEN practice ILIKE '%ahead%' THEN 'AHEAD Services'
            WHEN practice ILIKE '%TSA%' THEN 'TSA'
            WHEN practice ILIKE 'Products' THEN 'Products'
            
            ELSE practice
        END AS ahead_practice
    FROM filter
),
util_calc AS(
    SELECT  
        consultant_name,
        resource_role,
        resource_price,
        ahead_practice,
        SPLIT_PART(acs_date_param, '_', 5) AS month,
        util_target,
        adjusted_util_target,
        ROUND((pse_historical_billable_hours_c/NULLIF(pse_historical_calendar_hours_c,0))*100, 1) AS effective_utilization
    FROM agg_ahead_practice
    WHERE acs_date_param ILIKE 'ACS_Historical_Monthly%'
)
SELECT * FROM util_calc
ORDER BY ahead_practice,consultant_name,month;
