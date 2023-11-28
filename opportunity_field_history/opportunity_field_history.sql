-- TABLE
--- OPPORTUNITY_FIELD_HISTORY

-- ADDITIONAL DOCUMENTATION REFERENCE 
--- *add 

-- TABLE OF CONTENTS 
--- QUERY 1a: How many times is the close date updated on average per opportunity?
--- QUERY 1b: How many times is the close date updated on average per Data opportunity?
--- QUERY 2a: How many times is the stage name (ie: upside, committed, etc.) updated on average per opportunity?
--- QUERY 2b: How many times is the stage name (ie: upside, committed, etc.) updated on average per Data opportunity?  
--- QUERY 3a: How are opp close dates changing? 
--- QUERY 3b: How are Data opp close dates changing? 

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
--- How many times is the close date updated on average per opportunity?

-- join opportunity_field_history and opportunity tables, selecting only won opps that have had their close date changed, and where the opp was created on or after Jan 1 2021
WITH numerator_staging AS (
    SELECT 
        opportunity_id,
        TO_DATE(opportunity_field_history.created_date) AS change_created_date,
        TO_DATE(opportunity.created_date) AS opp_created_date
    FROM opportunity_field_history
    INNER JOIN opportunity ON opportunity_field_history.opportunity_id = opportunity.id
    WHERE LOWER(field) = 'closedate' AND DATE_PART(YEAR, opp_created_date) >= 2021
        AND is_won = TRUE AND is_closed = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id, change_created_date ORDER BY change_created_date) = 1
    -- ensures that opps whose closedate is changed multiple times in one day is only counted once
),
-- count the number of opp closedate changes, grouped by year
numerator AS (
    SELECT 
        DATE_PART(YEAR, opp_created_date) AS year_opp_created,
        COUNT(*) AS closedate_change_count
    FROM numerator_staging
    GROUP BY year_opp_created
),
-- count total number of opps since Jan 1 2021, grouped by year
denominator AS (
    SELECT 
        DATE_PART(YEAR, TO_DATE(opportunity.created_date)) AS year_opp_created_temp,
        COUNT(id) AS num_opps
    FROM opportunity 
    WHERE DATE_PART(YEAR, TO_DATE(opportunity.created_date)) >= 2021
        AND is_won = TRUE AND is_closed = TRUE
    GROUP BY year_opp_created_temp
),
-- divide above ctes for average number of closedate changes per opp, grouped by year
final AS (
    SELECT 
        year_opp_created,
        ROUND(
        numerator.closedate_change_count 
        /
        denominator.num_opps
        , 1) AS avg_closedate_changes_per_opp
    FROM numerator
    INNER JOIN denominator ON numerator.year_opp_created = denominator.year_opp_created_temp
    ORDER BY year_opp_created
)
SELECT * FROM final;

-------------------------------------------------------------------------------------------------
-- QUERY 1b
--- How many times is the close date updated on average per Data opportunity?

-- filter data for data practice only 
WITH data_prac_filter AS (
    SELECT 
        * 
    FROM opportunity 
    WHERE LOWER(ahead_practice_c) = 'data' OR LOWER(ahead_practice_c) = 'data (l2)'
),
-- join opportunity_field_history and opportunity tables, selecting only won opps that have had their close date changed, and where the opp was created on or after Jan 1 2021
numerator_staging AS (
    SELECT 
        opportunity_id,
        TO_DATE(opportunity_field_history.created_date) AS change_created_date,
        TO_DATE(data_prac_filter.created_date) AS opp_created_date
    FROM opportunity_field_history
    INNER JOIN data_prac_filter ON opportunity_field_history.opportunity_id = data_prac_filter.id
    WHERE LOWER(field) = 'closedate' AND DATE_PART(YEAR, opp_created_date) >= 2021
        AND is_won = TRUE AND is_closed = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id, change_created_date ORDER BY change_created_date) = 1
),
-- count the number of opp closedate changes, grouped by year
numerator AS (
    SELECT 
        DATE_PART(YEAR, opp_created_date) AS year_opp_created,
        COUNT(*) AS closedate_change_count
    FROM numerator_staging
    GROUP BY year_opp_created
),
-- count total number of opps since Jan 1 2021, grouped by year
denominator AS (
    SELECT 
        DATE_PART(YEAR, TO_DATE(data_prac_filter.created_date)) AS year_opp_created_temp,
        COUNT(id) AS num_opps
    FROM data_prac_filter 
    WHERE DATE_PART(YEAR, TO_DATE(data_prac_filter.created_date)) >= 2021
        AND is_won = TRUE AND is_closed = TRUE
    GROUP BY year_opp_created_temp
),
-- divide above ctes for average number of closedate changes per opp, grouped by year
final AS (
    SELECT 
        year_opp_created,
        ROUND(
        numerator.closedate_change_count 
        /
        denominator.num_opps
        , 1) AS avg_closedate_changes_per_opp
    FROM numerator
    INNER JOIN denominator ON numerator.year_opp_created = denominator.year_opp_created_temp
    ORDER BY year_opp_created
)
SELECT * FROM final;

-------------------------------------------------------------------------------------------------
-- QUERY 2a
--- How many times is the stage name (ie: upside, committed, etc.) updated on average per opportunity?

-- join opportunity_field_history and opportunity tables, selecting only won opps that have had their stage changed, and where the opp was created on or after Jan 1 2021
WITH numerator_staging AS (
    SELECT 
        opportunity_id,
        TO_DATE(opportunity_field_history.created_date) AS change_created_date,
        TO_DATE(opportunity.created_date) AS opp_created_date
    FROM opportunity_field_history
    INNER JOIN opportunity ON opportunity_field_history.opportunity_id = opportunity.id
    WHERE LOWER(field) = 'stagename' AND DATE_PART(YEAR, opp_created_date) >= 2021
        AND is_won = TRUE AND is_closed = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id, change_created_date ORDER BY change_created_date) = 1
    -- ensures that opps whose stagename is changed multiple times in one day is only counted once
    ORDER BY change_created_date, opportunity_id
),
-- count the number of opp stagename changes, grouped by year
numerator AS (
    SELECT 
        DATE_PART(YEAR, opp_created_date) AS year_opp_created,
        COUNT(*) AS stagename_change_count
    FROM numerator_staging
    GROUP BY year_opp_created
),
-- count total number of opps since Jan 1 2021, grouped by year
denominator AS (
    SELECT 
        DATE_PART(YEAR, TO_DATE(opportunity.created_date)) AS year_opp_created__temp,
        COUNT(id) AS num_opps
    FROM opportunity 
    WHERE DATE_PART(YEAR, TO_DATE(opportunity.created_date)) >= 2021
        AND is_won = TRUE AND is_closed = TRUE
    GROUP BY year_opp_created__temp
),
-- divide above ctes for average number of stagename changes per opp, grouped by year
final AS (
    SELECT 
        year_opp_created,
        ROUND(
        numerator.stagename_change_count 
        /
        denominator.num_opps
        , 1) AS avg_stagename_changes_per_opp
    FROM numerator
    INNER JOIN denominator ON numerator.year_opp_created = denominator.year_opp_created__temp
    ORDER BY year_opp_created
)
SELECT * FROM final;

-------------------------------------------------------------------------------------------------
-- QUERY 2b
--- How many times is the stage name (ie: upside, committed, etc.) updated on average per Data opportunity? 

-- filter data for data practice only 
WITH data_prac_filter AS (
    SELECT 
        * 
    FROM opportunity 
    WHERE LOWER(ahead_practice_c) = 'data' OR LOWER(ahead_practice_c) = 'data (l2)'
),
-- join opportunity_field_history and opportunity tables, selecting only won opps that have had their stage changed, and where the opp was created on or after Jan 1 2021
numerator_staging AS (
    SELECT 
        opportunity_id,
        TO_DATE(opportunity_field_history.created_date) AS change_created_date,
        TO_DATE(data_prac_filter.created_date) AS opp_created_date
    FROM opportunity_field_history
    INNER JOIN data_prac_filter ON opportunity_field_history.opportunity_id = data_prac_filter.id
    WHERE LOWER(field) = 'stagename' AND DATE_PART(YEAR, opp_created_date) >= 2021
        AND is_won = TRUE AND is_closed = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id, change_created_date ORDER BY change_created_date) = 1
),
-- count the number of opp stagename changes, grouped by year
numerator AS (
    SELECT 
        DATE_PART(YEAR, opp_created_date) AS year_opp_created,
        COUNT(*) AS stagename_change_count
    FROM numerator_staging
    GROUP BY year_opp_created
),
-- count total number of opps since Jan 1 2021, grouped by year
denominator AS ( 
    SELECT 
        DATE_PART(YEAR, TO_DATE(data_prac_filter.created_date)) AS year_opp_created__temp,
        COUNT(id) AS num_opps
    FROM data_prac_filter 
    WHERE DATE_PART(YEAR, TO_DATE(data_prac_filter.created_date)) >= 2021
        AND is_won = TRUE AND is_closed = TRUE
    GROUP BY year_opp_created__temp
),
-- divide above ctes for average number of stagename changes per opp, grouped by year
final AS (
    SELECT 
        year_opp_created,
        ROUND(
        numerator.stagename_change_count 
        /
        denominator.num_opps
        , 1) AS avg_stagename_changes_per_data_opp
    FROM numerator
    INNER JOIN denominator ON numerator.year_opp_created = denominator.year_opp_created__temp
    ORDER BY year_opp_created
)
SELECT * FROM final;

-------------------------------------------------------------------------------------------------
-- QUERY 3a
--- How are opp close dates changing? 

-- join opportunity_field_history and opportunity tables, selecting only opps that were created on or after Jan 1 2021 and have been won and closed 
WITH opp_staging AS (
    SELECT
        TO_DATE(opportunity.created_date) AS opp_created_date,
        TO_DATE(opportunity_field_history.created_date) AS change_created_date,
        *
    FROM opportunity_field_history
    INNER JOIN opportunity ON opportunity_field_history.opportunity_id = opportunity.id
    WHERE is_won = TRUE AND is_closed = TRUE 
        AND DATE_PART(YEAR, TO_DATE(opp_created_date)) >= 2021
),
-- select unique opp ids
all_opps AS (
    SELECT 
        DISTINCT(opportunity_id) AS all_opps,
        DATE_PART(YEAR, opp_created_date) AS year_opp_created_all
               
    FROM opp_staging
),
-- select unique opp ids that have had their close date changed
closedate_changed_opps AS (
    SELECT 
        DISTINCT(opportunity_id) AS cd_opps,
        DATE_PART(YEAR, opp_created_date) AS year_opp_created_cd
    FROM opp_staging
    WHERE LOWER(field) = 'closedate'
),
-- using above two ctes, use the nulls from the join to determine which opps have not had their close date changed
opps_w_unchanged_closedate AS ( 
    SELECT 
        year_opp_created_all, 
        COUNT(*) AS num_opps_w_unchanged_closedate
    FROM all_opps
    LEFT OUTER JOIN closedate_changed_opps ON all_opps = cd_opps 
    WHERE cd_opps IS NULL
    GROUP BY year_opp_created_all
    ORDER BY year_opp_created_all
),
-- calculate number of days until close when opp close date is changed and calculate number of days the close date is moved by
opp_dim AS (
    SELECT 
        opportunity_id,
        DATE_PART(YEAR, opp_created_date) AS year_opp_created,
        old_value AS old_closedate, 
        new_value AS new_closedate,
        DATEDIFF(DAY, change_created_date, old_closedate) AS num_days_to_original_closedate_when_closedate_changed, 
        DATEDIFF(DAY, old_closedate, new_closedate) AS num_days_closedate_changed,
        ROW_NUMBER() OVER (PARTITION BY opportunity_id, change_created_date ORDER BY change_created_date) AS change_rank
        -- ensures that opps whose closedate is changed multiple times in one day is only counted once
    FROM opp_staging
    WHERE LOWER(field) = 'closedate'
    QUALIFY change_rank = 1
),
-- label and count records based on opp_dim calculations 
opp_rollup AS (
    SELECT 
        year_opp_created,
        COUNT(DISTINCT(opportunity_id)) AS num_unique_opps_closedate_changed,
        SUM(CASE
            WHEN num_days_to_original_closedate_when_closedate_changed <= 0 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changed_on_or_after_close,
        SUM(CASE
            WHEN num_days_to_original_closedate_when_closedate_changed BETWEEN 1 AND 7 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changed_within_oneweek_from_close,
        SUM(CASE
            WHEN num_days_to_original_closedate_when_closedate_changed BETWEEN 8 AND 30 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changed_within_onemonth_from_close,
        SUM(CASE
            WHEN num_days_to_original_closedate_when_closedate_changed > 30 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changed_more_than_onemonth_from_close,
        SUM(CASE
            WHEN num_days_closedate_changed <= 0 THEN 1
            ELSE 0
        END) AS num_opps_closedate_pulled_ahead,
        SUM(CASE
            WHEN num_days_closedate_changed BETWEEN 1 AND 7 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changedby_oneweek_orless,
        SUM(CASE 
            WHEN num_days_closedate_changed BETWEEN 8 AND 30 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changedby_onemonth_orless,
        SUM(CASE 
            WHEN num_days_closedate_changed > 30 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changedby_morethan_onemonth
        
    FROM opp_dim
    GROUP BY year_opp_created
    ORDER BY year_opp_created
),
-- pull data together for presentation 
final AS (
    SELECT 
        year_opp_created,
        (num_unique_opps_closedate_changed + num_opps_w_unchanged_closedate) AS num_unique_opps,
        num_opps_w_unchanged_closedate,
        num_opps_closedate_changed_on_or_after_close,
        num_opps_closedate_changed_within_oneweek_from_close,
        num_opps_closedate_changed_within_onemonth_from_close,
        num_opps_closedate_changed_more_than_onemonth_from_close,
        num_opps_closedate_pulled_ahead,
        num_opps_closedate_changedby_oneweek_orless,
        num_opps_closedate_changedby_onemonth_orless,
        num_opps_closedate_changedby_morethan_onemonth
    FROM opp_rollup
    INNER JOIN opps_w_unchanged_closedate ON year_opp_created = year_opp_created_all
)
SELECT * FROM final;

-------------------------------------------------------------------------------------------------
-- QUERY 3b
--- How are Data opp close dates changing? 

-- filter data for data practice only 
WITH data_prac_filter AS (
    SELECT * 
    FROM opportunity 
    WHERE LOWER(ahead_practice_c) = 'data' OR LOWER(ahead_practice_c) = 'data (l2)'
),
-- join opportunity_field_history and opportunity tables, selecting only opps that were created on or after Jan 1 2021 and have been won and closed 
opp_staging AS (
    SELECT
        TO_DATE(data_prac_filter.created_date) AS opp_created_date,
        TO_DATE(opportunity_field_history.created_date) AS change_created_date,
        *
    FROM opportunity_field_history
    INNER JOIN data_prac_filter ON opportunity_field_history.opportunity_id = data_prac_filter.id
    WHERE is_won = TRUE AND is_closed = TRUE 
        AND DATE_PART(YEAR, TO_DATE(opp_created_date)) >= 2021
),
-- select unique opp ids
all_opps AS (
    SELECT 
        DISTINCT(opportunity_id) AS all_opps,
        DATE_PART(YEAR, opp_created_date) AS year_opp_created_all
    FROM opp_staging
),
-- select unique opp ids that have had their close date changed
closedate_changed_opps AS (
    SELECT 
        DISTINCT(opportunity_id) AS cd_opps,
        DATE_PART(YEAR, opp_created_date) AS year_opp_created_cd
    FROM opp_staging
    WHERE LOWER(field) = 'closedate'
),
-- using above two ctes, use the nulls from the join to determine which opps have not had their close date changed
opps_w_unchanged_closedate AS ( 
    SELECT 
        year_opp_created_all, 
        COUNT(*) AS num_opps_w_unchanged_closedate
    FROM all_opps
    LEFT OUTER JOIN closedate_changed_opps ON all_opps = cd_opps
    WHERE cd_opps IS NULL
    GROUP BY year_opp_created_all
    ORDER BY year_opp_created_all
),
-- calculate number of days until close when opp close date is changed and calculate number of days the close date is moved by
opp_dim AS (
    SELECT 
        opportunity_id,
        DATE_PART(YEAR, opp_created_date) AS year_opp_created,
        old_value AS old_closedate, 
        new_value AS new_closedate,
        DATEDIFF(DAY, change_created_date, old_closedate) AS num_days_to_original_closedate_when_closedate_changed, 
        DATEDIFF(DAY, old_closedate, new_closedate) AS num_days_closedate_changed,
        ROW_NUMBER() OVER (PARTITION BY opportunity_id, change_created_date ORDER BY change_created_date) AS change_rank
        -- ensures that opps whose closedate is changed multiple times in one day is only counted once
    FROM opp_staging
    WHERE LOWER(field) = 'closedate'
    QUALIFY change_rank = 1
),
-- label and count records based on opp_dim calculations 
opp_rollup AS (
    SELECT 
        year_opp_created,
        COUNT(DISTINCT(opportunity_id)) AS num_unique_opps_closedate_changed,
        SUM(CASE
            WHEN num_days_to_original_closedate_when_closedate_changed <= 0 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changed_on_or_after_close,
        SUM(CASE
            WHEN num_days_to_original_closedate_when_closedate_changed BETWEEN 1 AND 7 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changed_within_oneweek_from_close,
        SUM(CASE
            WHEN num_days_to_original_closedate_when_closedate_changed BETWEEN 8 AND 30 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changed_within_onemonth_from_close,
        SUM(CASE
            WHEN num_days_to_original_closedate_when_closedate_changed > 30 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changed_more_than_onemonth_from_close,
        SUM(CASE
            WHEN num_days_closedate_changed <= 0 THEN 1
            ELSE 0
        END) AS num_opps_closedate_pulled_ahead,
        SUM(CASE
            WHEN num_days_closedate_changed BETWEEN 1 AND 7 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changedby_oneweek_orless,
        SUM(CASE 
            WHEN num_days_closedate_changed BETWEEN 8 AND 30 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changedby_onemonth_orless,
        SUM(CASE 
            WHEN num_days_closedate_changed > 30 THEN 1
            ELSE 0
        END) AS num_opps_closedate_changedby_morethan_onemonth
        
    FROM opp_dim
    GROUP BY year_opp_created
    ORDER BY year_opp_created
),
-- pull data together for presentation 
final AS (
    SELECT 
        year_opp_created,
        (num_unique_opps_closedate_changed + num_opps_w_unchanged_closedate) AS num_unique_opps,
        num_opps_w_unchanged_closedate,
        num_opps_closedate_changed_on_or_after_close,
        num_opps_closedate_changed_within_oneweek_from_close,
        num_opps_closedate_changed_within_onemonth_from_close,
        num_opps_closedate_changed_more_than_onemonth_from_close,
        num_opps_closedate_pulled_ahead,
        num_opps_closedate_changedby_oneweek_orless,
        num_opps_closedate_changedby_onemonth_orless,
        num_opps_closedate_changedby_morethan_onemonth
    FROM opp_rollup
    INNER JOIN opps_w_unchanged_closedate ON year_opp_created = year_opp_created_all
)
SELECT * FROM final;
