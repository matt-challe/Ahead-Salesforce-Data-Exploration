-- TABLE
--- WBS_Actuals_C 

-- ADDITIONAL DOCUMENTATION REFERENCE 
--- *add 

-- TABLE OF CONTENTS 
--- QUERY 1a: How many hours are allocated to the average project, that has a WBS, for each practice?
--- QUERY 1b: How many hours are allocated to the average Data project that has a WBS?
--- QUERY 2: How are hours split amongst different consultant types within Data?

-------------------------------------------------------------------------------------------------
-- AUTHORS
--- Jillian Wedin, Habeeba Mansour, Matt Challe  
-------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------
-- PRE RUN SELECTIONS 
USE WAREHOUSE COMPUTE_WH;
USE ROLE READ_ONLY;
USE SALESFORCE_DATABASE;
USE SCHEMA SALESFORCE;
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------
-- QUERY 1a
--- How many hours are allocated to the average project, that has a WBS, for each practice?

-- join opportunity and wbs_actuals_c selecting only won opps and where wbs hours are more than 0 
WITH staging AS (
    SELECT 
        ahead_practice_c AS ahead_practice,
        TO_TIMESTAMP(wbs_actuals_c.created_date) AS wbs_created_date,
        opportunity_c AS opportunity_id,
        DATE_PART(YEAR, TO_DATE(opportunity.created_date)) AS year_opp_created,
        role_c AS consultant_type, 
        wbs_hours_c AS hours
    FROM opportunity
    LEFT JOIN wbs_actuals_c ON wbs_actuals_c.opportunity_c = opportunity.id
    WHERE DATE_PART(YEAR, TO_DATE(opportunity.created_date)) >= 2021
        AND hours != 0 AND is_won = TRUE AND is_closed = TRUE
    ORDER BY opportunity_id 
),
-- select the newest wbs date for each opportunity - assumes that the primary wbs is the most recently created
staging_wbs_date_filter AS (
    SELECT 
        opportunity_id AS temp_opp_id,
        MAX(wbs_created_date) AS newest_wbs_date
    FROM staging
    GROUP BY opportunity_id
),
-- filter data by joining the latest wbs opp date and opp id with the original staging data
wbs_date_filter AS (
    SELECT
        * 
    FROM staging_wbs_date_filter
    INNER JOIN staging ON staging_wbs_date_filter.temp_opp_id = staging.opportunity_id AND staging_wbs_date_filter.newest_wbs_date = staging.wbs_created_date
),
-- organize data by year and practice, average hours per opp, and number of opps in that year
final AS (
    SELECT 
        year_opp_created,
        ahead_practice,
        ROUND(AVG(hours),1) AS avg_hours_per_opp,
        COUNT(DISTINCT(opportunity_id)) AS num_opps
    FROM wbs_date_filter
    GROUP BY year_opp_created, ahead_practice
    ORDER BY year_opp_created, ahead_practice
)
SELECT * FROM final;


-------------------------------------------------------------------------------------------------
-- QUERY 1b
--- How many hours are allocated to the average Data project that has a WBS?

-- filter data for data practice only 
WITH data_prac_filter AS (
    SELECT 
        * 
    FROM opportunity 
    WHERE LOWER(ahead_practice_c) = 'data' OR LOWER(ahead_practice_c) = 'data (l2)'
),
-- join opportunity and wbs_actuals_c selecting only won opps and where wbs hours are more than 0 
staging AS (
    SELECT 
        TO_TIMESTAMP(wbs_actuals_c.created_date) AS wbs_created_date,
        opportunity_c AS opportunity_id,
        DATE_PART(YEAR, TO_DATE(data_prac_filter.created_date)) AS year_opp_created,
        role_c AS consultant_type, 
        wbs_hours_c AS hours
    FROM data_prac_filter
    LEFT JOIN wbs_actuals_c ON wbs_actuals_c.opportunity_c = data_prac_filter.id
    WHERE DATE_PART(YEAR, TO_DATE(data_prac_filter.created_date)) >= 2021
        AND hours != 0 AND is_won = TRUE AND is_closed = TRUE
    ORDER BY opportunity_id 
),
-- select the newest wbs date for each opportunity - assumes that the primary wbs is the most recently created
staging_wbs_date_filter AS (
    SELECT 
        opportunity_id AS temp_opp_id,
        MAX(wbs_created_date) AS newest_wbs_date
    FROM staging
    GROUP BY opportunity_id
),
-- filter data by joining the latest wbs opp date and opp id with the original staging data
wbs_date_filter AS (
    SELECT
        * 
    FROM staging_wbs_date_filter
    INNER JOIN staging ON staging_wbs_date_filter.temp_opp_id = staging.opportunity_id AND staging_wbs_date_filter.newest_wbs_date = staging.wbs_created_date
),
-- further filter to eliminate rows that have ahead practice mislabeled 
consultant_filter AS (
    SELECT 
        * 
    FROM wbs_date_filter
    WHERE LOWER(consultant_type) NOT LIKE '%contractor%'
        AND LOWER (consultant_type) NOT LIKE '%protection%'
        AND LOWER(consultant_type) LIKE '%data%'
),
-- organize data by year, average hours per opp, and number of opps in that year
final AS (
    SELECT 
        year_opp_created,
        ROUND(AVG(hours),1) AS avg_hours_per_opp,
        COUNT(DISTINCT(opportunity_id)) AS num_opps
    FROM consultant_filter
    GROUP BY year_opp_created
    ORDER BY year_opp_created
)
SELECT * FROM final;

-------------------------------------------------------------------------------------------------
-- QUERY 2
--- How are hours split amongst different consultant types within Data?

-- filter data for data practice only 
WITH data_prac_filter AS (
    SELECT * 
    FROM opportunity 
    WHERE LOWER(ahead_practice_c) = 'data' OR LOWER(ahead_practice_c) = 'data (l2)'
),
-- join opportunity and wbs_actuals_c selecting only won opps and where wbs hours are more than 0 
staging AS (
    SELECT 
        TO_TIMESTAMP(wbs_actuals_c.created_date) AS wbs_created_date,
        opportunity_c AS opportunity_id,
        DATE_PART(YEAR, TO_DATE(data_prac_filter.created_date)) AS year_opp_created,
        role_c AS consultant_type, 
        wbs_hours_c AS hours
    FROM data_prac_filter
    LEFT JOIN wbs_actuals_c ON wbs_actuals_c.opportunity_c = data_prac_filter.id
    WHERE DATE_PART(YEAR, TO_DATE(data_prac_filter.created_date)) >= 2021
        AND hours != 0 AND is_won = TRUE AND is_closed = TRUE
    ORDER BY opportunity_id 
),
-- select the newest wbs date for each opportunity - assumes that the primary wbs is the most recently created
staging_wbs_date_filter AS (
    SELECT 
        opportunity_id AS temp_opp_id,
        MAX(wbs_created_date) AS newest_wbs_date
    FROM staging
    GROUP BY opportunity_id
),
-- filter data by joining the latest wbs opp date and opp id with the original staging data
wbs_date_filter AS (
    SELECT
        * 
    FROM staging_wbs_date_filter
    INNER JOIN staging ON staging_wbs_date_filter.temp_opp_id = staging.opportunity_id AND staging_wbs_date_filter.newest_wbs_date = staging.wbs_created_date
),
-- further filter to eliminate rows that have ahead practice mislabeled 
consultant_filter AS (
    SELECT 
        * 
    FROM wbs_date_filter
    WHERE LOWER(consultant_type) NOT LIKE '%contractor%'
        AND LOWER (consultant_type) NOT LIKE '%protection%'
        AND LOWER(consultant_type) LIKE '%data%'
),
-- organize data by year, average hours per opp, and consultant type
final AS (
    SELECT 
        year_opp_created,
        consultant_type,
        ROUND(AVG(hours),1) AS avg_hours_per_opp
    FROM consultant_filter
    GROUP BY year_opp_created, consultant_type
    ORDER BY year_opp_created, avg_hours_per_opp DESC
)
SELECT * FROM final;