 ---
 #### Nice to Knows
1. Opportunities are only allowed resource requests if in stage 'strong upside' or higher
	- If opps fall below 'strong upside', the resource requests are auto-canceled and resources released
2. Each resource for a project gets a unique resource request 
3. By default, when resource requests are added, the resource is held
	- In many cases, a generic resource is added (ie: associate technical consultant Data)
	- In other cases, a specific person is added, depending on the direction from the delivery manager 
4. Resources are not officially on a project until they are 'assigned'
	- 10 days before the project start date on the resource request, the opp team receives an email to validate if the resource on the request should be moved to 'assigned'
	- 2 days before the project start date, if an existing resource request is not marked as 'assigned' (corresponds to field: pse__Status__c) then the resource request (RR) is canceled 
5. Resource requests are often deleted (not the same as canceled) if the underlying opp is lost, reduced in stage below strong upside, a duplicate, or to clean up a project (even if the RR is already canceled)
6. When a resource request is deleted, it is reflected in the *pse_resource_request_history* table where the field: *pse__Preferred_Schedule_c* changes to 'null', there is no 'deleted' verbiage for tracking 
7. Nulls in the ahead practice field usually indicate the resource request is for a project manager since they are cross-practice
8. Defined terms 
	- *pse_staffer_c* - assigned resource 
	- *pse_resource_c* - suggested resource
9. Links to helpful information 
	- [Placeholders for resource requests (including IDs)](https://thinkahead.lightning.force.com/lightning/r/Report/00O4u00000631cTEAQ/view)
	- [List of real resources, past and present (including IDs)](https://thinkahead.lightning.force.com/lightning/r/Report/00O4u0000062LHEEA2/view?queryScope=userFolders) 
		- You can access this list using the below logic in your query's ***where*** clause 
```sql
WHERE resource_id IN (SELECT DISTINCT(pse_resource_c) FROM pse_resource_actuals_c) 
```

---
Data explored using query below 
```sql
-- join resource request, opportunity, and resource request history, filter for data after 2021, closed-won opps, and resources that have been held and released
WITH staging AS ( 
    SELECT 
        pse_req.resource_practice_c AS ahead_practice,
        pse_req.pse_resource_c AS resource_id,
        pse_req.id AS req_id,
        pse_req.created_date AS resource_req_date,
        pse_history.created_date AS req_change_history_date,
        opp.close_date AS opp_close_date,
        pse_resource_request_name_c AS resource_name,
        pse_resource_held_c AS resource_held,
        pse_history.field,
        pse_history.old_value,
        pse_history.new_value,
        pse_start_date_c AS project_start_date, 
        opp.id AS opp_id,
        job_id_c AS AHD,
        is_closed,
        is_won,
        DATE_PART(YEAR, TO_DATE(opp.created_date)) AS opp_created_year
    FROM pse_resource_request_c AS pse_req
    LEFT OUTER JOIN opportunity AS opp ON pse_req.pse_opportunity_c = opp.id 
    LEFT OUTER JOIN pse_resource_request_history AS pse_history ON pse_history.parent_id = pse_req.id
    WHERE  
        is_won = FALSE AND is_closed = TRUE 
        AND opp_created_year = 2023
),
consultant_name AS (
    SELECT 
        pse_resource_c,
        SPLIT_PART(name, ' - ', 1) AS consultant_name
    FROM pse_resource_actuals_c
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pse_resource_c ORDER BY pse_resource_c) = 1
)
-- pulls opps that were opened in 2023 and lost/closed. Filters by pulling only resource requests where an actual person was eventually requested (not a placeholder)
SELECT 
    *
FROM staging
LEFT OUTER JOIN consultant_name ON staging.resource_id = consultant_name.pse_resource_c
WHERE resource_id IN (SELECT DISTINCT(pse_resource_c) FROM pse_resource_actuals_c) 
ORDER BY AHD, resource_id, resource_req_date, req_change_history_date
LIMIT 500;
```