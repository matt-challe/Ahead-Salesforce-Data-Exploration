USE WAREHOUSE COMPUTE_WH;
USE ROLE SYSADMIN;
USE DATABASE internal_salesforce_data_analytics; 
USE SCHEMA utilization_analytics;

--create view with the average utilization per practice in 2023
create or replace view AVERAGE_UTILIZATION_PER_PRACTICE_2023 AS
WITH staging AS(
    SELECT 
        ahead_practice,
        ROUND((AVG(effective_utilization)/AVG(adjusted_util_target))*100,1) AS percent_of_util_achieved,
        ROUND(AVG(adjusted_util_target),1) AS average_utilization_target,
        ROUND(AVG(effective_utilization),1) AS average_utilization
    FROM master_utilization_2023
    WHERE ahead_practice IS NOT NULL
    GROUP BY ahead_practice
    HAVING AVG(effective_utilization) IS NOT NULL
    ORDER BY percent_of_util_achieved DESC
)
SELECT * FROM staging;

--create view with the average utilization per practice in 2022
create or replace view AVERAGE_UTILIZATION_PER_PRACTICE_2022 as 
WITH staging AS(
    SELECT 
        ahead_practice,
        ROUND((AVG(effective_utilization)/AVG(adjusted_util_target))*100,1) AS percent_of_util_achieved,
        ROUND(AVG(adjusted_util_target),1) AS average_utilization_target,
        ROUND(AVG(effective_utilization),1) AS average_utilization
    FROM master_utilization_2022
    WHERE ahead_practice IS NOT NULL
    GROUP BY ahead_practice
    HAVING AVG(effective_utilization) IS NOT NULL
    ORDER BY percent_of_util_achieved DESC
)
SELECT * FROM staging;

--create view with the average utilization per practice in 2021
create or replace view AVERAGE_UTILIZATION_PER_PRACTICE_2021 as 
WITH staging AS(
    SELECT 
        ahead_practice,
        ROUND((AVG(effective_utilization)/AVG(adjusted_util_target))*100,1) AS percent_of_util_achieved,
        ROUND(AVG(adjusted_util_target),1) AS average_utilization_target,
        ROUND(AVG(effective_utilization),1) AS average_utilization
    FROM master_utilization_2021
    WHERE ahead_practice IS NOT NULL
    GROUP BY ahead_practice
    HAVING AVG(effective_utilization) IS NOT NULL
    ORDER BY percent_of_util_achieved DESC
)
SELECT * FROM staging;

---create view with the average utilization per practice per quarter in 2023
create or replace view AVERAGE_UTILIZATION_PER_PRACTICE_PER_QUARTER_2023 as
WITH get_quarter AS (
    SELECT 
        *,
        CASE 
            WHEN month BETWEEN 1 AND 3 THEN 'Q1'
            WHEN month BETWEEN 4 AND 6 THEN 'Q2'
            WHEN month BETWEEN 7 AND 9 THEN 'Q3'
            WHEN month BETWEEN 10 AND 12 THEN 'Q4'
        END AS quarter
    FROM master_utilization_2023 
),
util_quarter AS (
    SELECT 
        ahead_practice,
        quarter,
        ROUND((AVG(effective_utilization)/AVG(adjusted_util_target))*100,1) AS percent_of_util_achieved,
        ROUND(AVG(adjusted_util_target),1) AS average_utilization_target,
        ROUND(AVG(effective_utilization),1) AS average_utilization
    FROM get_quarter
    WHERE ahead_practice IS NOT NULL
    GROUP BY ahead_practice, quarter
    ORDER BY ahead_practice, quarter
)
SELECT * FROM util_quarter;

---create view with the average utilization per practice per quarter in 2022
create or replace view AVERAGE_UTILIZATION_PER_PRACTICE_PER_QUARTER_2022 as
WITH get_quarter AS (
    SELECT 
        *,
        CASE 
            WHEN month BETWEEN 1 AND 3 THEN 'Q1'
            WHEN month BETWEEN 4 AND 6 THEN 'Q2'
            WHEN month BETWEEN 7 AND 9 THEN 'Q3'
            WHEN month BETWEEN 10 AND 12 THEN 'Q4'
        END AS quarter
    FROM master_utilization_2022 
),
util_quarter AS (
    SELECT 
        ahead_practice,
        quarter,
        ROUND((AVG(effective_utilization)/AVG(adjusted_util_target))*100,1) AS percent_of_util_achieved,
        ROUND(AVG(adjusted_util_target),1) AS average_utilization_target,
        ROUND(AVG(effective_utilization),1) AS average_utilization
    FROM get_quarter
    WHERE ahead_practice IS NOT NULL
    GROUP BY ahead_practice, quarter
    ORDER BY ahead_practice, quarter
)
SELECT * FROM util_quarter;

---create view with the average utilization per practice per quarter in 2021
create or replace view AVERAGE_UTILIZATION_PER_PRACTICE_PER_QUARTER_2021 as
WITH get_quarter AS (
    SELECT 
        *,
        CASE 
            WHEN month BETWEEN 1 AND 3 THEN 'Q1'
            WHEN month BETWEEN 4 AND 6 THEN 'Q2'
            WHEN month BETWEEN 7 AND 9 THEN 'Q3'
            WHEN month BETWEEN 10 AND 12 THEN 'Q4'
        END AS quarter
    FROM master_utilization_2021 
),
util_quarter AS (
    SELECT 
        ahead_practice,
        quarter,
        ROUND((AVG(effective_utilization)/AVG(adjusted_util_target))*100,1) AS percent_of_util_achieved,
        ROUND(AVG(adjusted_util_target),1) AS average_utilization_target,
        ROUND(AVG(effective_utilization),1) AS average_utilization
    FROM get_quarter
    WHERE ahead_practice IS NOT NULL
    GROUP BY ahead_practice, quarter
    ORDER BY ahead_practice, quarter
)
SELECT * FROM util_quarter;

---average utilization per resource role in 2023
create or replace view average_utilization_per_resource_2023 AS
WITH agg_resource_roles AS(
    SELECT 
    *,
    CASE 
        WHEN resource_role ILIKE 'associate technical%' THEN 'Associate Technical Consultant'
        WHEN resource_role ILIKE 'consultant:%' THEN 'Consultant'
        WHEN resource_role ILIKE 'delivery manager%' THEN 'Delivery Manager'
        WHEN resource_role ILIKE 'engineer%' THEN 'Engineer'
        WHEN resource_role ILIKE 'managing principle%' THEN 'Managing Principal'
        WHEN resource_role ILIKE 'principal consultant%' THEN 'Principal Consultant'
        WHEN resource_role ILIKE 'principal technical consultant%' THEN 'Principal Technical Consultant'
        WHEN resource_role ILIKE 'program manager%' THEN 'Program Manager'
        WHEN resource_role ILIKE 'project manager%' THEN 'Project Manager'
        WHEN resource_role ILIKE 'senior associate technical%' THEN 'Senior Associate Technical Consultant'
        WHEN resource_role ILIKE 'senior consultant%' THEN 'Senior Consultant'
        WHEN resource_role ILIKE 'senior technical consultant%' THEN 'Senior Technical Consultant'
        WHEN resource_role ILIKE 'team lead%' THEN 'Team Lead'
        WHEN resource_role ILIKE 'technical consultant%' THEN 'Technical Consultant'
    END AS resource_roles

    FROM master_utilization_2023
),
staging AS(
    SELECT
        resource_roles,
        ROUND(AVG(resource_price),0) AS average_resource_price_USD,
        ROUND((AVG(effective_utilization)/AVG(adjusted_util_target))*100,1) AS percent_of_util_achieved,
        ROUND(AVG(adjusted_util_target),1) AS average_utilization_target,
        ROUND(AVG(effective_utilization),1) AS average_utilization
    FROM agg_resource_roles
    WHERE resource_roles IS NOT NULL
    GROUP BY resource_roles
    ORDER BY percent_of_util_achieved DESC
)
SELECT * FROM staging;

---average utilization per resource role in 2022
create or replace view average_utilization_per_resource_2022 AS
WITH agg_resource_roles AS(
    SELECT 
    *,
    CASE 
        WHEN resource_role ILIKE 'associate technical%' THEN 'Associate Technical Consultant'
        WHEN resource_role ILIKE 'consultant:%' THEN 'Consultant'
        WHEN resource_role ILIKE 'delivery manager%' THEN 'Delivery Manager'
        WHEN resource_role ILIKE 'engineer%' THEN 'Engineer'
        WHEN resource_role ILIKE 'managing principle%' THEN 'Managing Principal'
        WHEN resource_role ILIKE 'principal consultant%' THEN 'Principal Consultant'
        WHEN resource_role ILIKE 'principal technical consultant%' THEN 'Principal Technical Consultant'
        WHEN resource_role ILIKE 'program manager%' THEN 'Program Manager'
        WHEN resource_role ILIKE 'project manager%' THEN 'Project Manager'
        WHEN resource_role ILIKE 'senior associate technical%' THEN 'Senior Associate Technical Consultant'
        WHEN resource_role ILIKE 'senior consultant%' THEN 'Senior Consultant'
        WHEN resource_role ILIKE 'senior technical consultant%' THEN 'Senior Technical Consultant'
        WHEN resource_role ILIKE 'team lead%' THEN 'Team Lead'
        WHEN resource_role ILIKE 'technical consultant%' THEN 'Technical Consultant'
    END AS resource_roles

    FROM master_utilization_2022
),
staging AS(
    SELECT
        resource_roles,
        ROUND(AVG(resource_price),0) AS average_resource_price_USD,
        ROUND((AVG(effective_utilization)/AVG(adjusted_util_target))*100,1) AS percent_of_util_achieved,
        ROUND(AVG(adjusted_util_target),1) AS average_utilization_target,
        ROUND(AVG(effective_utilization),1) AS average_utilization
    FROM agg_resource_roles
    WHERE resource_roles IS NOT NULL
    GROUP BY resource_roles
    ORDER BY percent_of_util_achieved DESC
)
SELECT * FROM staging;

---average utilization per resource role in 2021
create or replace view average_utilization_per_resource_2021 AS
WITH agg_resource_roles AS(
    SELECT 
    *,
    CASE 
        WHEN resource_role ILIKE 'associate technical%' THEN 'Associate Technical Consultant'
        WHEN resource_role ILIKE 'consultant:%' THEN 'Consultant'
        WHEN resource_role ILIKE 'delivery manager%' THEN 'Delivery Manager'
        WHEN resource_role ILIKE 'engineer%' THEN 'Engineer'
        WHEN resource_role ILIKE 'managing principle%' THEN 'Managing Principal'
        WHEN resource_role ILIKE 'principal consultant%' THEN 'Principal Consultant'
        WHEN resource_role ILIKE 'principal technical consultant%' THEN 'Principal Technical Consultant'
        WHEN resource_role ILIKE 'program manager%' THEN 'Program Manager'
        WHEN resource_role ILIKE 'project manager%' THEN 'Project Manager'
        WHEN resource_role ILIKE 'senior associate technical%' THEN 'Senior Associate Technical Consultant'
        WHEN resource_role ILIKE 'senior consultant%' THEN 'Senior Consultant'
        WHEN resource_role ILIKE 'senior technical consultant%' THEN 'Senior Technical Consultant'
        WHEN resource_role ILIKE 'team lead%' THEN 'Team Lead'
        WHEN resource_role ILIKE 'technical consultant%' THEN 'Technical Consultant'
    END AS resource_roles

    FROM master_utilization_2021
),
staging AS(
    SELECT
        resource_roles,
        ROUND(AVG(resource_price),0) AS average_resource_price_USD,
        ROUND((AVG(effective_utilization)/AVG(adjusted_util_target))*100,1) AS percent_of_util_achieved,
        ROUND(AVG(adjusted_util_target),1) AS average_utilization_target,
        ROUND(AVG(effective_utilization),1) AS average_utilization
    FROM agg_resource_roles
    WHERE resource_roles IS NOT NULL
    GROUP BY resource_roles
    ORDER BY percent_of_util_achieved DESC
)
SELECT * FROM staging;
