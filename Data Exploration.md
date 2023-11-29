## Tables to include from top half of list:
``` sql
OPPORTUNITY --- information on all opportunities

--Columns of interest:
- ID 
- AHEAD_PRACTICE_C 
- AHEAD_PRACTICE_1_C  
- CREATED_DATE 
- FISCAL_QUARTER 
- FISCAL_YEAR 
- STAGE_NAME 
- IS_WON 
- CLOSE_DATE 
- NAME 
- PARENT_TERRITORY_C 

OPPORTUNITY_FIELD_HISTORY --- history of changes 

--Columns of interest:
- OPPORTUNITY_ID 
- CREATED_DATE 
- FIELD 
- OLD_VALUE 
- NEW_VALUE 
```

## Jillian exploratory code:
``` sql
SELECT * 

FROM WBS_ACTUALS_C 

LIMIT 5; 

-- ID, NAME, RATE_CARD_C, ROLE_C, WBS_HOURS_C 

-- the WBS_HOURS per role could be helpful 

-- yes 

-- need to understand how OPPORTUNITY_C relates to other table. Entity Relationship Diagram? 

-- what time period were these projects (and WBS hours) assigned for? 

SELECT * 

FROM SERVICES_REQUEST_C 

LIMIT 5; 

-- services request info -- 

------ maybe, isn't this going to be duplicative of what's in the opportunity table? 

SELECT * 

FROM PSE_UTILIZATION_SUMMARY_C 

LIMIT 5; 

-- ID, hours, non-billable hours 

-- all the PSE data could be useful -- 

---------- yes 

SELECT * 

FROM PSE_UTILIZATION_CALCULATION_C 

LIMIT 5; 

-- ID 

------- look at this one together, am I missing something?  Utlization_detail_c may be helpful? 

SELECT * 

FROM PSE_TIMECARD_C 

LIMIT 5; 

-- hours, revenue contribution 

------- include status, include project or resource to get practice data 

SELECT * 

FROM PSE_RESOURCE_ACTUALS_C 

LIMIT 5; 

-- billable hours per project -- 

----------- Doesn't seem accurate 

SELECT * 

FROM PSE_PROJ_C 

LIMIT 5; 

-- alot of metrics -- 

--------- pse_project_stage_c, opportunity_stage_c, pse_planned_hours_c, pse_project_status_c, pse_practice_c 

May be duplicative of opportunities table 

SELECT * 

FROM PSE_PRACTICE_C 

LIMIT 5; 

-- utilization by practice -- 

----------- name may be helpful for PSE_PROJ_C but don't see much else 

SELECT * 

FROM UTILIZATION_SNAPSHOT_C 

LIMIT 5; 

-- every employee's name and their historical utilization and target -- 

 -- alot of user information, ticketing information, survey data 

------ data not updated since 2018 

SELECT * 

FROM PSE_WORK_CALENDAR_C 

LIMIT 5; 

-- is this time cards? -- 

------- only 2 rows, just shows standard hours per day 

SELECT *  

FROM WBS_TEMPLATE_C 

LIMIT 5; 

-- ID, Name, HOURS_C, PRODUCT_C, BILLING_TYPE_C 

------- no, only a template 

SELECT * 

FROM WBS_ROLE_SKILL_C 

LIMIT 5; 

-- ID, ROLE_C 

-------- no,  data is given in WBS_ACTUAL_C 

SELECT * 

FROM PSE_TRANSACTION_DELTA_C 

LIMIT 5; 

-- empty -- 

------- Empty 

SELECT * 

FROM PSE_TRANSACTION_C 

LIMIT 5; 

-- Project, resource, timecard, is billed, etc. 

------- skip, utilization data is clear enough 

SELECT * 

FROM PSE_TEAM_C; 

-- PSE description, project, team owner 

-------- table is empty 

SELECT * 

FROM PSE_SKILL_CERTIFICATION_ZONE_C; 

-- group, practice, region, certification 

------- table is empty 

SELECT * 

FROM PSE_SCHEDULE_C 

LIMIT 5; 

-- scheduled hours for the PSE -- 

--------- table is empty 

SELECT * 

FROM PSE_PRACTICE_ACTUALS_C 

LIMIT 5; 

-- utilization, bookings, billings, billable hours, etc. by practice 

------ duplicative, poor data quality 

SELECT * 

FROM PSE_GROUP_ACTUALS_C 

LIMIT 5; 

-- billable hours by group -- 

------ poor data quality 

SELECT * 

FROM PSE_FORECAST_WORKSHEET_C 

LIMIT 5; 

-- actuals and forecasts -- 

------- shows promise but data is incomplete 

SELECT * 

FROM PSE_FORECAST_C 

LIMIT 5; 

-- forecasting upside and downside of opportunities 

--------- no data 

SELECT * 

FROM PSE_BACKLOG_DETAIL_C 

LIMIT 5; 

-- backlog data -- 

----- poor data quality 

SELECT * 

FROM PROJECT_TRACKER_C 

LIMIT 5; 

-- simpler data with opp stage, planned hours, billable hours, project type, etc. -- 

------- most recent data is ~1400 days old, no easy way to see which practice is involved 

```

## Held resource data exploration 

```sql
PSE_FORECAST_DETAIL_C (0 rows in table) 

- pse_scheduled_held_resource_requests_c  
- pse_scheduled_unheld_resource_requests_c  

PSE_PRACTICE_C (0s for every row in columns) 

- pse_hist_sch_utilization_held_hours_c 
- pse_scheduled_utilization_held_hours_c  

PSE_RESOURCE_REQUEST_C (88.8k rows) 

- pse_opportunity_c 
- pse_planned_bill_rate_c  
- pse_percent_allocated_c 
- pse_resource_role_c 
- pse_resource_held_c  
- pse_sow_hours 
- pse_start_date 
- pse_end_date_c  
- pse_status_c  
- rm_notes_c  
- resource_practice_c 
- resource_sub_practice_c 
- notes_for_resource_c  
- project_lost_or_closed_c 
- notes_for_resource_c 
- resource_role_2_c 
- pse_assignment_c 
- pse_exclude_from_planners_c 

PSE_RESOURCE_REQUEST_HISTORY 

- field  
- old_value 
- new_value
```
