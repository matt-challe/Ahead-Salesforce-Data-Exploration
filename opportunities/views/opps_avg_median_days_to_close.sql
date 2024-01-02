create or replace view INTERNAL_SALESFORCE_DATA_ANALYTICS.OPPORTUNITY_ANALYTICS.OPPS_AVG_MEDIAN_DAYS_TO_CLOSE(
	AHEAD_PRACTICE,
	YEAR_OPP_CLOSED,
	NUM_OPPS_WON,
	MEDIAN_DAYS_TO_CLOSE,
	AVG_DAYS_TO_CLOSE
) as

-- calculate the number of days to close for each opportunity, omit opps that close in a day or less 
WITH num_days_to_close AS (
    SELECT 
        id AS opp_id,
        ahead_practice_c AS ahead_practice_temp,
        TO_DATE(close_date) AS opp_close_date,
        DATE_PART(YEAR, opp_close_date) AS year_opp_closed,
        --DATE_PART(QUARTER, opp_close_date) AS quarter_opp_closed,
        opp_close_date-TO_DATE(created_date) AS days_to_close
    FROM salesforce_database.salesforce.opportunity
    WHERE is_closed = TRUE AND is_won = TRUE
        AND days_to_close > 1
        AND DATEDIFF(YEAR, opp_close_date, CURRENT_DATE()) <= 2 AND opp_close_date <= CURRENT_DATE()
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
    FROM num_days_to_close
),
-- calculate standard deviation of days to close
int_statistics AS (
    SELECT 
        STDDEV(days_to_close) AS stdev, 
        AVG(days_to_close) AS mean 
    FROM agg_ahead_practice
),
-- label opps whose number of days to close is an outlier 
outlier_filter AS(
    SELECT 
        *,
        CASE
            WHEN days_to_close BETWEEN mean - 3*stdev AND mean + 3*stdev THEN 'not outlier'
            ELSE 'outlier'
        END AS outlier_label
    FROM agg_ahead_practice
    CROSS JOIN int_statistics
),
-- calculate median and average days to close for presentation, remove outliers
final AS (
    SELECT 
        ahead_practice,
        year_opp_closed,
        --quarter_opp_closed,
        COUNT(*) AS num_opps_won,
        ROUND(MEDIAN(days_to_close),1) AS median_days_to_close, 
        ROUND(AVG(days_to_close),1) AS avg_days_to_close
    FROM outlier_filter
    WHERE outlier_label = 'not outlier'
    GROUP BY ahead_practice, year_opp_closed--, quarter_opp_closed
    ORDER BY ahead_practice, year_opp_closed--, quarter_opp_closed
)
SELECT * FROM final;