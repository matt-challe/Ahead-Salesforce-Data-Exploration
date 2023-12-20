-- TABLE
--- PSE_Resource_Request_C

-- DOCUMENTATION REFERENCE 
--- *add 

-- TABLE OF CONTENTS 
--- QUERY 1a: By year and quarter, how often do we hold resources for opps that we do not win?
--- QUERY 1b: By year, quarter and practice, how often do we hold resources for opps that we do not win?
--- QUERY 1c: By role, how often do we hold Data resources for opps that we do not win? 

-------------------------------------------------------------------------------------------------
-- AUTHORS
--- Jillian Wedin, Habeeba Mansour, Matt Challe  

-------------------------------------------------------------------------------------------------
-- PRE RUN SELECTIONS 
USE WAREHOUSE COMPUTE_WH;
USE ROLE READ_ONLY;
USE SALESFORCE_DATABASE;
USE SCHEMA SALESFORCE;
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------
-- QUERY 1a
--- By year and quarter, how often do we hold resources for opps that we do not win?

-- join resource request, opportunity, and resource request history, filter for data on or after 2021, closed-won opps, and resource requests that in their last state had a person assigned (not a placeholder)
WITH staging AS ( 
    SELECT 
        pse_req.resource_practice_c AS ahead_practice,
        pse_req.pse_resource_c AS resource_id,
        pse_req.id AS resource_req_id,
        pse_req.created_date AS resource_req_date,
        pse_history.created_date AS resource_req_change_date,
        pse_history.field,
        pse_history.old_value,
        pse_history.new_value,
        opp.id AS opp_id,
        TO_DATE(opp.close_date) AS opp_closed_date,
        CASE 
            WHEN DATE_PART(QUARTER, opp_closed_date) = 1 THEN 'Q1'
            WHEN DATE_PART(QUARTER, opp_closed_date) = 2 THEN 'Q2'
            WHEN DATE_PART(QUARTER, opp_closed_date) = 3 THEN 'Q3'
            WHEN DATE_PART(QUARTER, opp_closed_date) = 4 THEN 'Q4'
        END AS qtr_opp_closed,
        DATE_PART(YEAR, opp_closed_date) AS year_opp_closed
    FROM pse_resource_request_c AS pse_req
    LEFT OUTER JOIN opportunity AS opp ON pse_req.pse_opportunity_c = opp.id 
    LEFT OUTER JOIN pse_resource_request_history AS pse_history ON pse_history.parent_id = pse_req.id
    WHERE is_won = FALSE AND is_closed = TRUE 
        AND year_opp_closed >= 2021 AND opp_closed_date <= CURRENT_DATE()
        AND resource_id IN (SELECT DISTINCT(pse_resource_c) FROM pse_resource_actuals_c)
        -- filters for resource requests that in their final state had a person assigned (not a placeholder)
),
-- captures last record's date where the resource was changed on a resource request, note not all resource requests have had their resource changed
last_resource_change_date AS (
    SELECT 
        *,
        resource_req_change_date AS last_resource_change_date 
    FROM staging
    WHERE LOWER(field) = 'pse__staffer_resource__c' 
        AND new_value NOT LIKE '%0%'
    -- filter out resource id numbers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
    -- if the resource is changed (field: pse__staffer_resource__c, is updated) more than once on the same resource_req_id, only select the last change
),
-- captures the last record's date where the resource hold was removed on a resource request
last_resource_hold_status_change_date AS (
    SELECT 
        *, 
        resource_req_change_date AS last_resource_hold_status_change_date
    FROM staging
    WHERE LOWER(field) = 'pse__resource_held__c' 
        AND old_value = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
    -- if the resource request's hold status (field: pse__resource_held__c) is updated more than once, only select the last status change of TRUE to FALSE
),
-- captures the first record's date where the resource hold was removed (changed from TRUE to FALSE) on a resource request
first_resource_hold_status_change_date AS (
    SELECT 
        *, 
        resource_req_change_date AS first_resource_hold_status_change_date
    FROM staging
    WHERE LOWER(field) = 'pse__resource_held__c' 
    AND old_value = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date) = 1
    -- if the resource request's hold status (field: pse__resource_held__c) is updated more than once, only select the first status change of TRUE to FALSE
),
-- selects the date of the last change to a resource request. change type not specified. used to capture cancelled or deleted resource requests
final_change_to_resource_request_date AS (
    SELECT 
        *,
        resource_req_change_date AS final_change_to_resource_request_date
    FROM staging
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
),
-- join above cte's together to get all key dates into main table
join_all_ctes AS (
    SELECT 
        staging.*,
        last_resource_change_date,
        first_resource_hold_status_change_date,
        last_resource_hold_status_change_date,
        final_change_to_resource_request_date
    FROM staging
    LEFT OUTER JOIN last_resource_change_date USING (resource_req_id, resource_req_change_date)
    LEFT OUTER JOIN last_resource_hold_status_change_date USING (resource_req_id, resource_req_change_date)
    LEFT OUTER JOIN first_resource_hold_status_change_date USING (resource_req_id, resource_req_change_date)
    LEFT OUTER JOIN final_change_to_resource_request_date USING (resource_req_id, resource_req_change_date)
),
-- push key dates (last_resource_change_date, first/last_resource_hold_status_change_date) through null records to allow for calculating date deltas 
-- window functions used to isolate by resource_request_id
modify_date_columns AS(
    SELECT 
        *,
        MAX(last_resource_change_date) OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_resource_change_date_ext,
        MAX(first_resource_hold_status_change_date) OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_resource_hold_status_change_date_ext,
        MAX(last_resource_hold_status_change_date) OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_resource_hold_status_change_date_ext
    FROM join_all_ctes  
),
-- calculates number of day delta to determine if a resource was held 10 days or more for an opp that was lost, this calc only for resource requests where the resource was changed
calc_if_resource_held__req_where_resource_changed AS (
    SELECT 
        *,
        CASE 
            -- if the resource has had its hold status changed (TRUE to FALSE) once or multiple times; subtract the last resource change date from the last hold status change date
            WHEN DATEDIFF(DAY, last_resource_change_date_ext, last_resource_hold_status_change_date_ext) >= 10 THEN TRUE
            -- if the above evaluates to null (null implies the hold status never changed); subtract the last resource change date from the resource request's final change date 
            WHEN DATEDIFF(DAY, last_resource_change_date_ext, last_resource_hold_status_change_date_ext) IS NULL AND DATEDIFF(DAY, last_resource_change_date_ext, final_change_to_resource_request_date) >= 10 THEN TRUE
            ELSE FALSE
        END AS resource_held_and_opp_lost
    FROM modify_date_columns
    WHERE resource_req_id IN (SELECT DISTINCT(resource_req_id) FROM last_resource_change_date)
    -- calculate only for resource_req_ids that have had their resource changed
),
-- calculates number of day delta to determine if a resource was held 10 days or more for an opp that was lost, this calc only for resource requests where the resource was NOT changed
calc_if_resource_held__req_where_resource_not_changed AS (
    SELECT 
        *,
        CASE 
            -- if the resource had its hold status changed (TRUE to FALSE) once or multiple times; subtract the date the resource request was created from the first hold status change date 
            WHEN DATEDIFF(DAY, resource_req_date, first_resource_hold_status_change_date_ext) >= 10 THEN TRUE 
            -- if the above evaluates to null (null implies the hold status never changed); subtract the date the resource request was created from the resource request's final change date 
            WHEN DATEDIFF(DAY, resource_req_date, first_resource_hold_status_change_date_ext) IS NULL AND DATEDIFF(DAY, resource_req_date, final_change_to_resource_request_date) >= 10 THEN TRUE
            ELSE FALSE
        END AS resource_held_and_opp_lost
    FROM modify_date_columns
    WHERE resource_req_id NOT IN (SELECT DISTINCT(resource_req_id) FROM last_resource_change_date)
    -- calculate only for resource_req_ids that have NOT had their resource changed 
),
-- union both calc_if_resource_held ctes to combine the results
union_resource_held_ctes AS (
    SELECT * FROM calc_if_resource_held__req_where_resource_changed
    UNION 
    SELECT * FROM calc_if_resource_held__req_where_resource_not_changed
),
-- count the number of resources held and opps lost
final AS (
    SELECT 
        year_opp_closed,
        qtr_opp_closed,
        COUNT(DISTINCT(resource_req_id)) AS num_resources_held_and_opp_lost,
        -- count resource_req_id because resource_id may be found multiple times amongst different resource requests
        COUNT(DISTINCT(opp_id)) AS num_opps_lost_w_resources_held
    FROM union_resource_held_ctes
    WHERE resource_held_and_opp_lost = TRUE 
    GROUP BY year_opp_closed, qtr_opp_closed
    ORDER BY year_opp_closed, qtr_opp_closed
)
SELECT * FROM final; 


-------------------------------------------------------------------------------------------------
-- QUERY 1b
--- By year, quarter and practice, how often do we hold resources for opps that we do not win?

-- join resource request, opportunity, and resource request history, filter for data on or after 2021, closed-won opps, and resource requests that in their last state had a person assigned (not a placeholder)
WITH staging AS ( 
    SELECT 
        pse_req.resource_practice_c AS ahead_practice,
        pse_req.pse_resource_c AS resource_id,
        pse_req.id AS resource_req_id,
        pse_req.created_date AS resource_req_date,
        pse_history.created_date AS resource_req_change_date,
        pse_history.field,
        pse_history.old_value,
        pse_history.new_value,
        opp.id AS opp_id,
        TO_DATE(opp.close_date) AS opp_closed_date,
        CASE 
            WHEN DATE_PART(QUARTER, opp_closed_date) = 1 THEN 'Q1'
            WHEN DATE_PART(QUARTER, opp_closed_date) = 2 THEN 'Q2'
            WHEN DATE_PART(QUARTER, opp_closed_date) = 3 THEN 'Q3'
            WHEN DATE_PART(QUARTER, opp_closed_date) = 4 THEN 'Q4'
        END AS qtr_opp_closed,
        DATE_PART(YEAR, opp_closed_date) AS year_opp_closed
    FROM pse_resource_request_c AS pse_req
    LEFT OUTER JOIN opportunity AS opp ON pse_req.pse_opportunity_c = opp.id 
    LEFT OUTER JOIN pse_resource_request_history AS pse_history ON pse_history.parent_id = pse_req.id
    WHERE is_won = FALSE AND is_closed = TRUE 
        AND year_opp_closed >= 2021 AND opp_closed_date <= CURRENT_DATE()
        AND resource_id IN (SELECT DISTINCT(pse_resource_c) FROM pse_resource_actuals_c)
        -- filters for resource requests that in their final state had a person assigned (not a placeholder)
),
-- captures last record's date where the resource was changed on a resource request, note not all resource requests have had their resource changed
last_resource_change_date AS (
    SELECT 
        *,
        resource_req_change_date AS last_resource_change_date 
    FROM staging
    WHERE LOWER(field) = 'pse__staffer_resource__c' 
        AND new_value NOT LIKE '%0%'
    -- filter out resource id numbers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
    -- if the resource is changed (field: pse__staffer_resource__c, is updated) more than once on the same resource_req_id, only select the last change
),
-- captures the last record's date where the resource hold was removed on a resource request
last_resource_hold_status_change_date AS (
    SELECT 
        *, 
        resource_req_change_date AS last_resource_hold_status_change_date
    FROM staging
    WHERE LOWER(field) = 'pse__resource_held__c' 
        AND old_value = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
    -- if the resource request's hold status (field: pse__resource_held__c) is updated more than once, only select the last status change of TRUE to FALSE
),
-- captures the first record's date where the resource hold was removed (changed from TRUE to FALSE) on a resource request
first_resource_hold_status_change_date AS (
    SELECT 
        *, 
        resource_req_change_date AS first_resource_hold_status_change_date
    FROM staging
    WHERE LOWER(field) = 'pse__resource_held__c' 
    AND old_value = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date) = 1
    -- if the resource request's hold status (field: pse__resource_held__c) is updated more than once, only select the first status change of TRUE to FALSE
),
-- selects the date of the last change to a resource request. change type not specified. used to capture cancelled or deleted resource requests
final_change_to_resource_request_date AS (
    SELECT 
        *,
        resource_req_change_date AS final_change_to_resource_request_date
    FROM staging
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
),
-- join above cte's together to get all key dates into main table
join_all_ctes AS (
    SELECT 
        staging.*,
        last_resource_change_date,
        first_resource_hold_status_change_date,
        last_resource_hold_status_change_date,
        final_change_to_resource_request_date
    FROM staging
    LEFT OUTER JOIN last_resource_change_date USING (resource_req_id, resource_req_change_date)
    LEFT OUTER JOIN last_resource_hold_status_change_date USING (resource_req_id, resource_req_change_date)
    LEFT OUTER JOIN first_resource_hold_status_change_date USING (resource_req_id, resource_req_change_date)
    LEFT OUTER JOIN final_change_to_resource_request_date USING (resource_req_id, resource_req_change_date)
),
-- push key dates (last_resource_change_date, first/last_resource_hold_status_change_date) through null records to allow for calculating date deltas 
-- window functions used to isolate by resource_request_id
modify_date_columns AS(
    SELECT 
        *,
        MAX(last_resource_change_date) OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_resource_change_date_ext,
        MAX(first_resource_hold_status_change_date) OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_resource_hold_status_change_date_ext,
        MAX(last_resource_hold_status_change_date) OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_resource_hold_status_change_date_ext
    FROM join_all_ctes  
),
-- calculates number of day delta to determine if a resource was held 10 days or more for an opp that was lost, this calc only for resource requests where the resource was changed
calc_if_resource_held__req_where_resource_changed AS (
    SELECT 
        *,
        CASE 
            -- if the resource has had its hold status changed (TRUE to FALSE) once or multiple times; subtract the last resource change date from the last hold status change date
            WHEN DATEDIFF(DAY, last_resource_change_date_ext, last_resource_hold_status_change_date_ext) >= 10 THEN TRUE
            -- if the above evaluates to null (null implies the hold status never changed); subtract the last resource change date from the resource request's final change date 
            WHEN DATEDIFF(DAY, last_resource_change_date_ext, last_resource_hold_status_change_date_ext) IS NULL AND DATEDIFF(DAY, last_resource_change_date_ext, final_change_to_resource_request_date) >= 10 THEN TRUE
            ELSE FALSE
        END AS resource_held_and_opp_lost
    FROM modify_date_columns
    WHERE resource_req_id IN (SELECT DISTINCT(resource_req_id) FROM last_resource_change_date)
    -- calculate only for resource_req_ids that have had their resource changed
),
-- calculates number of day delta to determine if a resource was held 10 days or more for an opp that was lost, this calc only for resource requests where the resource was NOT changed
calc_if_resource_held__req_where_resource_not_changed AS (
    SELECT 
        *,
        CASE 
            -- if the resource had its hold status changed (TRUE to FALSE) once or multiple times; subtract the date the resource request was created from the first hold status change date 
            WHEN DATEDIFF(DAY, resource_req_date, first_resource_hold_status_change_date_ext) >= 10 THEN TRUE 
            -- if the above evaluates to null (null implies the hold status never changed); subtract the date the resource request was created from the resource request's final change date 
            WHEN DATEDIFF(DAY, resource_req_date, first_resource_hold_status_change_date_ext) IS NULL AND DATEDIFF(DAY, resource_req_date, final_change_to_resource_request_date) >= 10 THEN TRUE
            ELSE FALSE
        END AS resource_held_and_opp_lost
    FROM modify_date_columns
    WHERE resource_req_id NOT IN (SELECT DISTINCT(resource_req_id) FROM last_resource_change_date)
    -- calculate only for resource_req_ids that have NOT had their resource changed 
),
-- union both calc_if_resource_held ctes to combine the results
union_resource_held_ctes AS (
    SELECT * FROM calc_if_resource_held__req_where_resource_changed
    UNION 
    SELECT * FROM calc_if_resource_held__req_where_resource_not_changed
),
-- count the number of resources held and opps lost
final AS (
    SELECT 
        ahead_practice,
        year_opp_closed,
        qtr_opp_closed,
        COUNT(DISTINCT(resource_req_id)) AS num_resources_held_and_opp_lost,
        -- count resource_req_id because resource_id may be found multiple times amongst different resource requests
        COUNT(DISTINCT(opp_id)) AS num_opps_lost_w_resources_held
    FROM union_resource_held_ctes
    WHERE resource_held_and_opp_lost = TRUE 
    GROUP BY ahead_practice, year_opp_closed, qtr_opp_closed
    ORDER BY ahead_practice, year_opp_closed, qtr_opp_closed
)
SELECT * FROM final; 


-------------------------------------------------------------------------------------------------
-- QUERY 1c
--- By role, how often do we hold Data resources for opps that we do not win? 

-- filter data for data practice only 
WITH data_prac_filter AS (
    SELECT 
        *,
        resource_practice_c AS ahead_practice
    FROM pse_resource_request_c
    WHERE 
        LOWER(ahead_practice) = 'data'
),
-- join resource request, opportunity, and resource request history, filter for data on or after 2021, closed-won opps, and resource requests that in their last state had a person assigned (not a placeholder)
staging AS ( 
    SELECT 
        pse_req.resource_practice_c AS ahead_practice,
        pse_resource_role_c AS resource_role,
        pse_req.pse_resource_c AS resource_id,
        pse_req.id AS resource_req_id,
        pse_req.created_date AS resource_req_date,
        pse_history.created_date AS resource_req_change_date,
        pse_history.field,
        pse_history.old_value,
        pse_history.new_value,
        opp.id AS opp_id,
        TO_DATE(opp.close_date) AS opp_closed_date,
        CASE 
            WHEN DATE_PART(QUARTER, opp_closed_date) = 1 THEN 'Q1'
            WHEN DATE_PART(QUARTER, opp_closed_date) = 2 THEN 'Q2'
            WHEN DATE_PART(QUARTER, opp_closed_date) = 3 THEN 'Q3'
            WHEN DATE_PART(QUARTER, opp_closed_date) = 4 THEN 'Q4'
        END AS qtr_opp_closed,
        DATE_PART(YEAR, opp_closed_date) AS year_opp_closed
    FROM data_prac_filter AS pse_req
    LEFT OUTER JOIN opportunity AS opp ON pse_req.pse_opportunity_c = opp.id 
    LEFT OUTER JOIN pse_resource_request_history AS pse_history ON pse_history.parent_id = pse_req.id
    WHERE is_won = FALSE AND is_closed = TRUE 
        AND year_opp_closed >= 2021 AND opp_closed_date <= CURRENT_DATE()
        AND resource_id IN (SELECT DISTINCT(pse_resource_c) FROM pse_resource_actuals_c)
        -- filters for resource requests that in their final state had a person assigned (not a placeholder)
),
-- captures last record's date where the resource was changed on a resource request, note not all resource requests have had their resource changed
last_resource_change_date AS (
    SELECT 
        *,
        resource_req_change_date AS last_resource_change_date 
    FROM staging
    WHERE LOWER(field) = 'pse__staffer_resource__c' 
        AND new_value NOT LIKE '%0%'
    -- filter out resource id numbers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
    -- if the resource is changed (field: pse__staffer_resource__c, is updated) more than once on the same resource_req_id, only select the last change
),
-- captures the last record's date where the resource hold was removed on a resource request
last_resource_hold_status_change_date AS (
    SELECT 
        *, 
        resource_req_change_date AS last_resource_hold_status_change_date
    FROM staging
    WHERE LOWER(field) = 'pse__resource_held__c' 
        AND old_value = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
    -- if the resource request's hold status (field: pse__resource_held__c) is updated more than once, only select the last status change of TRUE to FALSE
),
-- captures the first record's date where the resource hold was removed (changed from TRUE to FALSE) on a resource request
first_resource_hold_status_change_date AS (
    SELECT 
        *, 
        resource_req_change_date AS first_resource_hold_status_change_date
    FROM staging
    WHERE LOWER(field) = 'pse__resource_held__c' 
    AND old_value = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date) = 1
    -- if the resource request's hold status (field: pse__resource_held__c) is updated more than once, only select the first status change of TRUE to FALSE
),
-- selects the date of the last change to a resource request. change type not specified. used to capture cancelled or deleted resource requests
final_change_to_resource_request_date AS (
    SELECT 
        *,
        resource_req_change_date AS final_change_to_resource_request_date
    FROM staging
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
),
-- join above cte's together to get all key dates into main table
join_all_ctes AS (
    SELECT 
        staging.*,
        last_resource_change_date,
        first_resource_hold_status_change_date,
        last_resource_hold_status_change_date,
        final_change_to_resource_request_date
    FROM staging
    LEFT OUTER JOIN last_resource_change_date USING (resource_req_id, resource_req_change_date)
    LEFT OUTER JOIN last_resource_hold_status_change_date USING (resource_req_id, resource_req_change_date)
    LEFT OUTER JOIN first_resource_hold_status_change_date USING (resource_req_id, resource_req_change_date)
    LEFT OUTER JOIN final_change_to_resource_request_date USING (resource_req_id, resource_req_change_date)
),
-- push key dates (last_resource_change_date, first/last_resource_hold_status_change_date) through null records to allow for calculating date deltas 
-- window functions used to isolate by resource_request_id
modify_date_columns AS(
    SELECT 
        *,
        MAX(last_resource_change_date) OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_resource_change_date_ext,
        MAX(first_resource_hold_status_change_date) OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_resource_hold_status_change_date_ext,
        MAX(last_resource_hold_status_change_date) OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_resource_hold_status_change_date_ext
    FROM join_all_ctes  
),
-- calculates number of day delta to determine if a resource was held 10 days or more for an opp that was lost, this calc only for resource requests where the resource was changed
calc_if_resource_held__req_where_resource_changed AS (
    SELECT 
        *,
        CASE 
            -- if the resource has had its hold status changed (TRUE to FALSE) once or multiple times; subtract the last resource change date from the last hold status change date
            WHEN DATEDIFF(DAY, last_resource_change_date_ext, last_resource_hold_status_change_date_ext) >= 10 THEN TRUE
            -- if the above evaluates to null (null implies the hold status never changed); subtract the last resource change date from the resource request's final change date 
            WHEN DATEDIFF(DAY, last_resource_change_date_ext, last_resource_hold_status_change_date_ext) IS NULL AND DATEDIFF(DAY, last_resource_change_date_ext, final_change_to_resource_request_date) >= 10 THEN TRUE
            ELSE FALSE
        END AS resource_held_and_opp_lost
    FROM modify_date_columns
    WHERE resource_req_id IN (SELECT DISTINCT(resource_req_id) FROM last_resource_change_date)
    -- calculate only for resource_req_ids that have had their resource changed
),
-- calculates number of day delta to determine if a resource was held 10 days or more for an opp that was lost, this calc only for resource requests where the resource was NOT changed
calc_if_resource_held__req_where_resource_not_changed AS (
    SELECT 
        *,
        CASE 
            -- if the resource had its hold status changed (TRUE to FALSE) once or multiple times; subtract the date the resource request was created from the first hold status change date 
            WHEN DATEDIFF(DAY, resource_req_date, first_resource_hold_status_change_date_ext) >= 10 THEN TRUE 
            -- if the above evaluates to null (null implies the hold status never changed); subtract the date the resource request was created from the resource request's final change date 
            WHEN DATEDIFF(DAY, resource_req_date, first_resource_hold_status_change_date_ext) IS NULL AND DATEDIFF(DAY, resource_req_date, final_change_to_resource_request_date) >= 10 THEN TRUE
            ELSE FALSE
        END AS resource_held_and_opp_lost
    FROM modify_date_columns
    WHERE resource_req_id NOT IN (SELECT DISTINCT(resource_req_id) FROM last_resource_change_date)
    -- calculate only for resource_req_ids that have NOT had their resource changed 
),
-- union both calc_if_resource_held ctes to combine the results
union_resource_held_ctes AS (
    SELECT * FROM calc_if_resource_held__req_where_resource_changed
    UNION 
    SELECT * FROM calc_if_resource_held__req_where_resource_not_changed
),
-- count the number of resources held and opps lost
final AS (
    SELECT 
        resource_role,
        COUNT(DISTINCT(resource_req_id)) AS num_resources_held_and_opp_lost,
        -- count resource_req_id because resource_id may be found multiple times amongst different resource requests
        COUNT(DISTINCT(opp_id)) AS num_opps_lost_w_resources_held
    FROM union_resource_held_ctes
    WHERE resource_held_and_opp_lost = TRUE 
    GROUP BY resource_role
    ORDER BY resource_role
)
SELECT * FROM final; 
