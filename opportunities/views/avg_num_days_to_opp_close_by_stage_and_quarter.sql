create or replace view INTERNAL_SALESFORCE_DATA_ANALYTICS.OPPORTUNITY_ANALYTICS.AVG_NUM_DAYS_TO_OPP_CLOSE_BY_STAGE_AND_QUARTER(
	AHEAD_PRACTICE,
	YEAR_OPP_CLOSED,
	QUARTER_OPP_CLOSED,
	NUM_OPPS,
	PROSPECTING_LEAD_AVG_DAYS_TILL_CLOSE,
	PIPELINE_AVG_DAYS_TILL_CLOSE,
	UPSIDE_AVG_DAYS_TILL_CLOSE,
	STRONG_UPSIDE_AVG_DAYS_TILL_CLOSE,
	COMMITTED_AVG_DAYS_TILL_CLOSE
) as 
-- join opportunity and opportunity field history to calculate the days to close of won opps based on their current stage 
WITH staging AS (
    SELECT 
        new_value,
        ahead_practice_c AS ahead_practice_temp,
        TO_DATE(opp.created_date) AS opp_created_date,
        TO_DATE(opp.close_date) AS opp_close_date,
        DATE_PART(YEAR, opp_close_date) AS year_opp_closed,
        DATE_PART(QUARTER, opp_close_date) AS quarter_opp_closed,
        opp_history.created_date AS change_created_date_and_time,
        TO_DATE(change_created_date_and_time) AS change_created_date,
        opp_close_date - opp_created_date AS total_days_to_close,
        opp.id AS opp_id
    FROM salesforce_database.salesforce.opportunity_field_history AS opp_history
    INNER JOIN salesforce_database.salesforce.opportunity AS opp ON opp_history.opportunity_id = opp.id
    WHERE is_closed = TRUE AND is_won = TRUE
        AND total_days_to_close > 1 AND LOWER(field) = 'stagename'
        AND DATEDIFF(QUARTER, opp_close_date, CURRENT_DATE()) <= 3 AND opp_close_date <= CURRENT_DATE()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id, TO_DATE(change_created_date_and_time) ORDER BY    change_created_date_and_time DESC) = 1
),
-- calculate standard deviation of days to close
int_statistics AS (
    SELECT 
        STDDEV(total_days_to_close) AS stdev, 
        AVG(total_days_to_close) AS mean 
    FROM staging
),
-- label opps whose number of days to close is an outlier 
outlier_filter AS (
    SELECT 
        *,
        total_days_to_close,
    CASE
        WHEN total_days_to_close BETWEEN mean - 3*stdev AND mean + 3*stdev THEN 'not outlier'
        ELSE 'outlier'
    END AS outlier_label
    FROM staging
    CROSS JOIN int_statistics
),
-- remove opps with outlier 
remove_outliers AS (
    SELECT 
        *
    FROM outlier_filter   
    WHERE outlier_label = 'not outlier'
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
    FROM remove_outliers
),
-- aggregate days to close together for presentation 
final AS (
    SELECT  
        ahead_practice,
        year_opp_closed,
        quarter_opp_closed,
        COUNT(DISTINCT(opp_id)) AS num_opps,
        -- prospecting stage name not included because all values were nulls
        ROUND(AVG(CASE 
            WHEN LOWER(new_value) LIKE '%prospecting%lead%' THEN opp_close_date - change_created_date
            ELSE NULL
        END),1) AS prospecting_lead_avg_days_till_close,            
        ROUND(AVG(CASE 
            WHEN LOWER(new_value) = 'pipeline' THEN opp_close_date - change_created_date
            ELSE NULL
        END),1) AS pipeline_avg_days_till_close,
        ROUND(AVG(CASE 
            WHEN LOWER(new_value) = 'upside' THEN opp_close_date - change_created_date
            ELSE NULL
        END),1) AS upside_avg_days_till_close,
        ROUND(AVG(CASE 
            WHEN LOWER(new_value) = 'strong upside' THEN opp_close_date - change_created_date
            ELSE NULL
        END),1) AS strong_upside_avg_days_till_close,
        ROUND(AVG(CASE 
            WHEN LOWER(new_value) LIKE '%commit%' THEN opp_close_date - change_created_date
            ELSE NULL
        END),1) AS committed_avg_days_till_close
         
    FROM agg_ahead_practice
    GROUP BY ahead_practice, year_opp_closed, quarter_opp_closed
    ORDER BY ahead_practice, year_opp_closed, quarter_opp_closed
)
SELECT * FROM final;