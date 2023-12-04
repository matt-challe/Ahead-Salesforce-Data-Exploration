
## Important Tables:
``` sql
- pse_utilization_calculation_c
- pse_utilization_detail_c
- pse_utilization_summary_
- pse_resource_actuals_c -- just for the consultant names
```

## Query joining all relevant tables:

*Note: Even with filter when the query is not limited there's 111 million rows formed in 11 mins*

``` sql
WITH filter AS( --Only checking 2023 to minimize compute resources
    SELECT DATE_PART(YEAR, TO_DATE(created_date)),
    * 

    FROM pse_utilization_summary_c
    WHERE DATE_PART(YEAR, TO_DATE(created_date)) > 2023
    
),
staging AS (  

    SELECT  

        pse_utilization_calculation_c.id AS util_calc_id,
        
        pse_utilization_calculation_c.pse_calculate_historical_utilization_c AS calc_hist_util,

        pse_utilization_calculation_c.pse_calculate_scheduled_utilization_c AS calc_sched_util,

        util_detail.pse_resource_c AS resource_id,

        pse_utilization_calculation_c.name AS name,

        *
 
    FROM pse_utilization_calculation_c 

    INNER JOIN pse_utilization_detail_c AS util_detail ON pse_utilization_calculation_c.id = util_detail.pse_utilization_calculation_c

    INNER JOIN pse_utilization_summary_c AS util_summary ON pse_utilization_calculation_c.id = util_summary.pse_utilization_calculation_c 
 
),
names_and_roles AS(

    SELECT 
    
    resource_id,

    util_calc_id,

    *

    FROM staging 

    INNER JOIN pse_resource_actuals_c ON resource_id = pse_resource_actuals_c.pse_resource_c
    

)
SELECT *  FROM names_and_roles

LIMIT 500000; --to be able to see stats
```

### Important Columns from Previous Query

```sql
- pse_pass_through_billings -- floats
- pse_non_billable_internal_hours_c -- ints
- pse_non_billable_external_hours_c -- ints
- pse_invoiced_c -- floats
- pse_internal_costs_c -- floats
- pse_external_costs_c -- floats
- pse_expense_costs_c -- floats
- pse_excluded_hours_c -- ints
- pse_credited_non_billable_internal_hours_c --ints
- pse_billings_c -- floats
- pse_billed_c -- floats
- pse_billable_internal_hours_c --floats
- pse_billable_external_hours_c --floats
- pse_time_period_c --string
- pse_resource_c --string
- name_5 --string
- pse_time_period_c -- string
- pse_template_key_c -- int
- pse_target_hours_c --float
- pse_resource_role_c --string
- pse_held_hours_c --float
- pse_held_hours_oppz_pct_weighted_c --float
- pse_calender_hours_c --int
- pse_assigned_non_billable_hours_c --int
- pse_assigned_excluded_hours_c --int
- pse_assigned_credited_hours_c --int
- pse_assigned_billable_hours_c --int
- pse_actual_non_billable_hours_c --int
- pse_actual_exluded_hours_c --int
- pse_actual_credited_hours_c --int
- pse_actual_billable_hours_c --int
- name_4 --acs_scheduled_weekly or acs_historical_monthly_2023_06
- resource_role_c --string
- utilization_target_c --int
- resource_blp_c --float
- adjusted_utilization_target_c --int
- pse_utilization_target_hours --float per week most is 30.4 76%(40)
- pse_scheduled_utilization_target_hours --same same
- pse_time_period_c_2 --string
- pse_time_period --string Week or Month
- pse_template_key_c --int
- pse_scheduled_start_date_c --string
- pse_scheduled_end_date_c --string
- pse_scheduled_calender_hours -- int 40 most of the time
- pse_scheduled_billable_hours_c --float
- pse_historical_calender_hours_c --int sometimes 0 sometimes 16
- pse_historical_billable_hours_c --int sometimes 0 sometimes 8
- pse_time_period_c --string
- pse_time_period_types_c --string Week or Month
- pse_scheduled_utilization_start_date_c --string
- pse_scheduled_utilization_end_date_c --string
```