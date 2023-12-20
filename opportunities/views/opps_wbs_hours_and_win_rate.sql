create or replace view INTERNAL_SALESFORCE_DATA_ANALYTICS.OPPORTUNITY_ANALYTICS.OPPS_WBS_HOURS_AND_WIN_RATE(
	AHEAD_PRACTICE,
	YEAR_OPP_CLOSED,
	QUARTER_OPP_CLOSED,
	AVG_HOURS_PER_OPP,
	NUM_OPPS_W_WBS,
	WON_OPPS,
	ALL_CLOSED_OPPS,
	LOST_OPPS,
	OPP_WIN_PERCENT
) as

-- join views opps_w_wbs and opp_win_percentage_by_practice to enable greater slicer flexibility in PowerBI
SELECT 
    * 
FROM opps_w_wbs
FULL OUTER JOIN opp_win_percentage_by_practice USING (ahead_practice, year_opp_closed, quarter_opp_closed)
ORDER BY ahead_practice, year_opp_closed, quarter_opp_closed;