 ---

 #### General Nice to Knows
1. A resource hold is when a resource's (ie: consultant, project manager) calendar is blocked off to support an upcoming project 
2. Opportunities are only allowed resource requests if in stage 'strong upside' or higher
	- If opps fall below 'strong upside', the resource requests are auto-canceled and resources released
	- Auto-canceled resource requests are not marked in the resource request history (manual cancellations are)
3. Each resource for a project gets a unique resource request 
4. By default, when resource requests are added, the resource is held
	- In many cases, a generic resource is added (ie: associate technical consultant Data)
	- In other cases, a specific person is added, depending on the direction from the delivery manager 
5. Resources are not officially on a project until they are 'assigned'
	- 10 days before the project start date on the resource request, the opp team receives an email to validate if the resource on the request should be moved to 'assigned'
	- 2 days before the project start date, if an existing resource request is not marked as 'assigned' (corresponds to field: pse__Status_c) then the resource request is auto-canceled 
6. Resource requests are often manually deleted (not the same as canceled) if the underlying opp is lost, reduced in stage below strong upside, a duplicate, or to clean up a project (even if the request is already canceled)
	- When a resource request is deleted, it is reflected in the *pse_resource_request_history* table where the field: *pse__Preferred_Schedule_c* changes to 'null', there is no 'deleted' verbiage for tracking 
1. Nulls in the ahead practice field *usually* indicate the resource request is for a project manager since they are cross-practice
2. Defined terms 
	- *pse_staffer_c* - assigned resource 
	- *pse_resource_c* - suggested resource
3. Links to helpful information 
	- [Placeholders for resource requests (including IDs)](https://thinkahead.lightning.force.com/lightning/r/Report/00O4u00000631cTEAQ/view)
	- [List of real resources, past and present (including IDs)](https://thinkahead.lightning.force.com/lightning/r/Report/00O4u0000062LHEEA2/view?queryScope=userFolders) 
		- You can access this list using the below logic in your query's ***where*** clause 
```sql
WHERE resource_id IN (SELECT DISTINCT(pse_resource_c) FROM pse_resource_actuals_c) 
```

--- 
#### Query Specific Nice to Knows


[Query Logic Flow (LucidChart)](https://lucid.app/lucidchart/a727e719-70d3-4815-975d-f09325e99b3c/edit?viewport_loc=-1060%2C-145%2C3914%2C2008%2C0_0&invitationId=inv_e66daabf-7632-4e1c-9484-88e55a46928a)
![[resources_held_query_logic.png]]
##### Logic Gaps (future improvements)
1. If the resource is changed multiple times on a resource request, only the final resource and their time held is considered  
2. If a resource is held and released multiple times on the same resource request, only the first OR last hold is considered (first - if the resource has never been changed; last - if the resource has been changed)  
3. Resource requests are not marked in the resource request history if they are auto-cancelled by Salesforce, current logic uses the final change date available in the history  
4. When subtracting the last resource change date from the last hold status change date, we cannot be certain that the final hold status change happened chronologically next - it's possible it was changed multiple times  

***Implication***: The count of resource ids and associated opportunities is a conservative number and the actual count is likely higher

---

Data explored using query below 
```sql
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
        job_id_c AS AHD,
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
        AND year_opp_closed >= 2021
        AND resource_id IN (SELECT DISTINCT(pse_resource_c) FROM pse_resource_actuals_c)
        -- filters for resource requests that in their last state had a person assigned (not a placeholder)
),
-- prep consultant names to join to main table
prep_consultant_name AS (
    SELECT
        pse_resource_c AS resource_id,
        SPLIT_PART(name, ' - ', 1) AS consultant_name
    FROM pse_resource_actuals_c
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY resource_id) = 1
),
-- filters to only resource requests where an actual person was eventually requested (not a placeholder), adds consultant names
add_consultant_name AS (
    SELECT
        *
    FROM staging
    LEFT OUTER JOIN prep_consultant_name USING (resource_id)
    WHERE resource_id IN (SELECT DISTINCT(pse_resource_c) FROM pse_resource_actuals_c)
),
-- serializes each resource request so they are in order of their history 
serialize_resource_request_history AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date) AS order_of_events
    FROM add_consultant_name
),
-- captures last record's date where the resource was changed on a resource request, note not all resource requests have had their resource changed
last_resource_change_date AS (
    SELECT
        *,
        resource_req_change_date AS last_resource_change_date
    FROM serialize_resource_request_history
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
    FROM serialize_resource_request_history
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
    FROM serialize_resource_request_history
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
    FROM serialize_resource_request_history
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_req_id ORDER BY resource_req_change_date DESC) = 1
),
-- join above cte's together to get all key dates into main table
join_all_ctes AS (
    SELECT
        serialize_resource_request_history.*,
        last_resource_change_date,
        first_resource_hold_status_change_date,
        last_resource_hold_status_change_date,
        final_change_to_resource_request_date
    FROM serialize_resource_request_history
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
            WHEN DATEDIFF(DAY, last_resource_change_date_ext, last_resource_hold_status_change_date_ext) IS NOT NULL THEN DATEDIFF(DAY, last_resource_change_date_ext, last_resource_hold_status_change_date_ext)
            -- if the above evaluates to null (null implies the hold status never changed); subtract the last resource change date from the resource request's final change date
            WHEN DATEDIFF(DAY, last_resource_change_date_ext, last_resource_hold_status_change_date_ext) IS NULL AND DATEDIFF(DAY, last_resource_change_date_ext, final_change_to_resource_request_date) IS NOT NULL THEN DATEDIFF(DAY, last_resource_change_date_ext, final_change_to_resource_request_date)
            ELSE NULL
        END AS num_days_held__resource_changed,
        NULL AS num_days_held__resource_never_changed,
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
        NULL AS num_days_held__resource_changed,
        CASE
            -- if the resource had its hold status changed (TRUE to FALSE) once or multiple times; subtract the date the resource request was created from the first hold status change date
            WHEN DATEDIFF(DAY, resource_req_date, first_resource_hold_status_change_date_ext) IS NOT NULL THEN DATEDIFF(DAY, resource_req_date, first_resource_hold_status_change_date_ext)
            -- if the above evaluates to null (null implies the hold status never changed); subtract the date the resource request was created from the resource request's final change date
            WHEN DATEDIFF(DAY, resource_req_date, first_resource_hold_status_change_date_ext) IS NULL AND DATEDIFF(DAY, resource_req_date, final_change_to_resource_request_date) IS NOT NULL THEN DATEDIFF(DAY, resource_req_date, final_change_to_resource_request_date)
            ELSE NULL
        END AS num_days_held__resource_never_changed,
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
    UNION ALL
    SELECT * FROM calc_if_resource_held__req_where_resource_not_changed
)
SELECT
    ahead_practice,
    AHD,
    resource_req_id,
    resource_req_date,
    consultant_name,
    order_of_events,
    resource_req_change_date,
    field,
    old_value,
    new_value,
    num_days_held__resource_never_changed,
    num_days_held__resource_changed,
    final_change_to_resource_request_date,
    resource_held_and_opp_lost,
    year_opp_closed,
    qtr_opp_closed
FROM union_resource_held_ctes
-- comment out WHERE clause if you would like to see the full resource request history
WHERE 
((LOWER(field) = 'pse__staffer_resource__c' AND new_value NOT LIKE '%0%') OR LOWER(field) = 'pse__resource_held__c') OR final_change_to_resource_request_date IS NOT NULL
-- example resource requests that use the different sorting logic in the query
-- AND 
--resource_req_id = 'a2o4u0000021f7wAAA' -- resource changed from placeholder to person AND hold status never changed
--resource_req_id = 'a2o4u000002a4s4AAA' -- resource changed from placeholder to person AND hold status changed 
--resource_req_id = 'a2o4u0000021f7zAAA' -- resource never changed and hold status never changed
--resource_req_id = 'a2o4u0000021gIxAAI' -- resource never changed and hold status changed
ORDER BY opp_closed_date, resource_req_id, order_of_events;
```