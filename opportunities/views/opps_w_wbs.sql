create or replace view INTERNAL_SALESFORCE_DATA_ANALYTICS.OPPORTUNITY_ANALYTICS.OPPS_W_WBS(
	YEAR_OPP_CLOSED,
	QUARTER_OPP_CLOSED,
	AHEAD_PRACTICE,
	AVG_HOURS_PER_OPP,
	NUM_OPPS_W_WBS
) as 

-- join opportunity and wbs_actuals_c selecting only won opps and where wbs hours are more than 0 
WITH staging AS (
    SELECT 
        ahead_practice_c AS ahead_practice_temp,
        TO_TIMESTAMP(wbs.created_date) AS wbs_created_date,
        opportunity_c AS opportunity_id,
        TO_DATE(opp.close_date) AS opp_close_date,
        DATE_PART(YEAR, opp_close_date) AS year_opp_closed,
        DATE_PART(QUARTER, opp_close_date) AS quarter_opp_closed,
        role_c AS consultant_type, 
        wbs_hours_c AS hours
    FROM salesforce_database.salesforce.opportunity AS opp
    LEFT JOIN salesforce_database.salesforce.wbs_actuals_c AS wbs ON wbs.opportunity_c = opp.id
    WHERE DATEDIFF(YEAR, opp_close_date, CURRENT_DATE()) <= 3 AND DATE_PART(YEAR, opp_close_date) >= 2021 AND opp_close_date <= CURRENT_DATE()
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
-- aggregate ahead practices
agg_ahead_practice AS (
    SELECT 
        *,
        CASE 
            WHEN ahead_practice_temp ILIKE '%advisory%' THEN 'Advisory'
            WHEN ahead_practice_temp ILIKE '%appdev%' OR ahead_practice_temp ILIKE '%application%' THEN 'AppDev'
            WHEN ahead_practice_temp ILIKE '%cloud%' THEN 'Cloud'
            WHEN ahead_practice_temp ILIKE '%data protection%' THEN 'Data Protection'
            WHEN ahead_practice_temp ILIKE '%data%' THEN 'Data'
            WHEN ahead_practice_temp ILIKE '%ema%' THEN 'EMA'
            WHEN ahead_practice_temp ILIKE '%esm%' THEN 'ESM'
            WHEN ahead_practice_temp ILIKE '%euc%' THEN 'EUC'
            WHEN ahead_practice_temp ILIKE '%ms%' OR ahead_practice_temp ILIKE '%managed%' THEN 'Managed Services'
            WHEN ahead_practice_temp ILIKE '%modern%' THEN 'Modern Infrastructure'
            WHEN ahead_practice_temp ILIKE '%network%' THEN 'Network'
            WHEN ahead_practice_temp ILIKE '%security%' AND ahead_practice_temp NOT ILIKE '%ms%' THEN 'Security'
            ELSE 'Null'
        END AS ahead_practice
    FROM wbs_date_filter
),
-- organize data by year and practice, average hours per opp, and number of opps in that year
final AS (
    SELECT 
        year_opp_closed,
        quarter_opp_closed,
        ahead_practice,
        ROUND(AVG(hours),1) AS avg_hours_per_opp,
        COUNT(DISTINCT(opportunity_id)) AS num_opps_w_wbs
    FROM agg_ahead_practice
    -- filter out managed services
    GROUP BY ahead_practice, year_opp_closed, quarter_opp_closed
    HAVING ahead_practice NOT ILIKE 'managed services'
    ORDER BY ahead_practice, year_opp_closed, quarter_opp_closed
)
SELECT * FROM final;