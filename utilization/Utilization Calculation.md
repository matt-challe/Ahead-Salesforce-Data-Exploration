
## Three main tables

``` sql
- pse_utilization_calculation_c
- pse_utilization_detail_c
- pse_resource_actuals_c -- just for the consultant names
```

## Master Utilization Views

Three views corresponding to 2021, 2022, and 2023.
``` SQL
master_utilization_2023
master_utilization_2022
master_utilization_2021
```

These views contain the consultant names, roles, practice, utilization target, effective utilization. Each consultant has their utilization per month.

## Aggregated Utilization Views

Three main style of views:
* average utilization per practice in _year_
* average utilization per practice per quarter in _year_
* average utilization per resource role in _year_ 