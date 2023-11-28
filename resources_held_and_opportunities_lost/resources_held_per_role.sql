--Which roles are being held more

--Joining the resource requests, resource request history, and opportunities tables
--Only joining when a resource request is existed and then was cancelled (old_value=True)
--Opportunity must not have been won and must be closed
WITH staging AS (  

    SELECT  

        pse_req.resource_practice_c AS ahead_practice, --practice

        pse_req.pse_resource_c AS resource_id, --ID used to join

        pse_req.pse_resource_role_c AS resource_role, --role

        pse_history.field, --Indicates if it is a resource held

        pse_history.old_value, --BOOLEAN if True

        pse_history.new_value, --BOOLEAN if False

        opp.id AS opp_id, --opp ID

        DATE_PART(YEAR, TO_DATE(opp.created_date)) AS opp_created_year 

    FROM pse_resource_request_c AS pse_req 

    LEFT OUTER JOIN opportunity AS opp ON pse_req.pse_opportunity_c = opp.id  

    LEFT OUTER JOIN pse_resource_request_history AS pse_history ON pse_history.parent_id = pse_req.id 

    WHERE  

        LOWER(field) = 'pse__resource_held__c' 

        AND old_value = TRUE --resources were held

        AND is_won = FALSE AND is_closed = TRUE --opportunity is closed and has not been won

        AND DATE_PART(YEAR, TO_DATE(opp.created_date)) >= 2021 

), --count by role and order
dim_resources AS (  

    SELECT  resource_role, 

    COUNT(*) AS num_resources__resource_held_and_opp_lost 

    FROM staging 

    GROUP BY resource_role 

    ORDER BY num_resources__resource_held_and_opp_lost DESC

)
SELECT * FROM dim_resources;

