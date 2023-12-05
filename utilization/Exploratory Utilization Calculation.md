
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
- pse_pass_through_billings -- sometimes 0 sometimes 614.52
```