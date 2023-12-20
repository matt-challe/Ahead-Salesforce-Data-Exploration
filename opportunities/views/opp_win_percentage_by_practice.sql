create or replace view INTERNAL_SALESFORCE_DATA_ANALYTICS.OPPORTUNITY_ANALYTICS.OPP_WIN_PERCENTAGE_BY_PRACTICE(
	AHEAD_PRACTICE,
	YEAR_OPP_CLOSED,
	QUARTER_OPP_CLOSED,
	WON_OPPS,
	ALL_CLOSED_OPPS,
	LOST_OPPS,
	OPP_WIN_PERCENT
) as 

-- aggregate ahead practices
WITH staging AS ( 
    SELECT 
        is_closed,
        is_won,
        stage_name,
        TO_DATE(close_date) AS opp_close_date,
        DATE_PART(YEAR, opp_close_date) AS year_opp_closed,
        DATE_PART(QUARTER, opp_close_date) AS quarter_opp_closed,
        ahead_practice_c AS ahead_practice_temp,
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
    FROM salesforce_database.salesforce.opportunity AS opp
    WHERE DATEDIFF(YEAR, opp_close_date, CURRENT_DATE()) <= 3 AND DATE_PART(YEAR, opp_close_date) >= 2021 AND opp_close_date <= CURRENT_DATE()
),
-- count number of closed opps, grouped by year and quarter
denominator AS (
    SELECT 
        ahead_practice,
        year_opp_closed,
        quarter_opp_closed,
        COUNT(*) AS all_closed_opps
    FROM staging 
    WHERE is_closed = TRUE AND LOWER(stage_name) NOT LIKE '%duplicate%'
    GROUP BY ahead_practice, year_opp_closed, quarter_opp_closed
    ORDER BY ahead_practice, year_opp_closed, quarter_opp_closed
),
-- count number of won opps, grouped by year
numerator AS (
    SELECT 
        ahead_practice,
        year_opp_closed,
        quarter_opp_closed,
        COUNT(*) AS won_opps
    FROM staging
    WHERE is_closed = TRUE AND is_won = TRUE
    GROUP BY ahead_practice, year_opp_closed, quarter_opp_closed
    ORDER BY ahead_practice, year_opp_closed, quarter_opp_closed
),
-- divide above ctes for opp to win percent, grouped by year 
final AS (
    SELECT
        ahead_practice,
        numerator.year_opp_closed,
        numerator.quarter_opp_closed,
        won_opps,
        all_closed_opps,
        all_closed_opps - won_opps AS lost_opps,
        100*(ROUND(
        numerator.won_opps
        /
        denominator.all_closed_opps
        ,3)) AS opp_win_percent
    FROM denominator 
    LEFT JOIN numerator USING (ahead_practice, year_opp_closed, quarter_opp_closed)
    ORDER BY numerator.ahead_practice, numerator.year_opp_closed, numerator.quarter_opp_closed
)
SELECT * FROM final;