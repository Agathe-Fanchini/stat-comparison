QUERY_1 = """WITH {page_condition} AS page
SELECT session_id,
       session_number_of_views,
       dom_interactive_after_msec,
       session_number_of_transactions,
       is_last,
       scroll_rate,
       session_duration_msec,
       sumIf(sign *
             if(view_duration_msec < page_interaction_time_msec, view_duration_msec, page_interaction_time_msec) /
             view_duration_msec,
             (((view_duration_msec <= 1800000) AND (view_duration_msec > 0)) AND (has_all_interaction_events))) /
       sumIf(sign, ((has_all_interaction_events AND
                     (view_duration_msec <= 1800000 AND view_duration_msec >= 0)))) AS activityRate

FROM views
WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})
AND page
AND is_excludable = 0
GROUP BY session_id, session_number_of_views, dom_interactive_after_msec, session_number_of_transactions, is_last, scroll_rate, session_duration_msec, is_first

SETTINGS distributed_group_by_no_merge = 0"""

QUERY_bouncers = """WITH
(({page_condition})) as Bounce_Page

SELECT

{input_select}

uniqExact(session_id) AS Sessions,

uniqExactIf(session_id, Bounce_Page AND session_number_of_views=1) AS Bounce_Sessions


FROM views

WHERE   project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
        AND is_excludable = 0

AND session_number_of_views=1
AND Bounce_Page
GROUP BY {input_select_name}
ORDER BY {input_select_name}

SETTINGS distributed_group_by_no_merge=0"""


QUERY_exiters = """
WITH
(({page_condition})) as Exit_Page

SELECT

{input_select}

uniqExact(session_id) AS Sessions,

uniqExactIf(session_id, Exit_Page AND is_last) AS Exit_Sessions

FROM views

WHERE   project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
        AND is_excludable = 0


AND Exit_Page

GROUP BY {input_select_name}
ORDER BY {input_select_name}

SETTINGS distributed_group_by_no_merge=0;"""


QUERY_transaction = """WITH
(({page_condition})) as Page

SELECT

{input_select}

uniqExact(session_id) AS Sessions,

uniqExactIf(session_id, Page AND session_number_of_transactions=1) AS Transaction_Sessions


FROM views

WHERE   project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
        AND is_excludable = 0

AND Page
GROUP BY {input_select_name}
ORDER BY {input_select_name}

SETTINGS distributed_group_by_no_merge=0;"""


QUERY_activity_bouncers = """WITH
(({page_condition})) as Bounce_Page

SELECT
       multiIf(
    (activityRate >0 AND activityRate <0.1), '1. 10%',
    (activityRate >0.1 AND activityRate <0.2), '2. 20%',
    (activityRate >0.2 AND activityRate <0.3), '3. 30%',
    (activityRate >0.3 AND activityRate <0.4), '4. 40%',
    (activityRate >0.5 AND activityRate <0.6), '5. 50%',
    (activityRate >0.6 AND activityRate <0.7), '6. 60%',
    (activityRate >0.7 AND activityRate <0.8), '7. 70%',
    (activityRate >0.8 AND activityRate <0.9), '8. 80%',
    (activityRate >0.9 AND activityRate <1), '9. 90%',
    (activityRate = 1), 'z10. 100%'
, 'zznull') as Activity_Rate,
       uniqExact(session_id) as All_Sessions,
       uniqExactIf(session_id, Bounce_Page AND session_number_of_views=1) AS Bounce_Sessions

FROM
(
WITH
(({page_condition})) as Page
SELECT sumIf(sign *
             if(view_duration_msec < page_interaction_time_msec, view_duration_msec, page_interaction_time_msec) /
             view_duration_msec,
             (((view_duration_msec <= 1800000) AND (view_duration_msec > 0)) AND (has_all_interaction_events))) /
       sumIf(sign, ((has_all_interaction_events AND
                     (view_duration_msec <= 1800000 AND view_duration_msec >= 0)))) AS activityRate, 
                     custom_vars_view.position, custom_vars_view.value, query, path, prefix,
       (session_id),
       (session_number_of_views)
 FROM views
WHERE project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
        AND is_excludable = 0

AND Page
GROUP BY session_id, session_number_of_views, custom_vars_view.position, custom_vars_view.value, query, path, prefix
 )
GROUP By Activity_Rate
ORDER BY Activity_Rate
SETTINGS distributed_group_by_no_merge = 0;


"""

QUERY_activity_exiters = """ WITH
(({page_condition})) as Exit_Page

SELECT
       multiIf(
    (activityRate >0 AND activityRate <0.1), '1. 10%',
    (activityRate >0.1 AND activityRate <0.2), '2. 20%',
    (activityRate >0.2 AND activityRate <0.3), '3. 30%',
    (activityRate >0.3 AND activityRate <0.4), '4. 40%',
    (activityRate >0.5 AND activityRate <0.6), '5. 50%',
    (activityRate >0.6 AND activityRate <0.7), '6. 60%',
    (activityRate >0.7 AND activityRate <0.8), '7. 70%',
    (activityRate >0.8 AND activityRate <0.9), '8. 80%',
    (activityRate >0.9 AND activityRate <1), '9. 90%',
    (activityRate = 1), 'z10. 100%'
, 'zznull') as Activity_Rate,
       uniqExact(session_id) as All_Sessions,
       uniqExactIf(session_id, Exit_Page AND session_number_of_views=1) AS Exit_Sessions

FROM
(
WITH
(({page_condition})) as Page
SELECT sumIf(sign *
             if(view_duration_msec < page_interaction_time_msec, view_duration_msec, page_interaction_time_msec) /
             view_duration_msec,
             (((view_duration_msec <= 1800000) AND (view_duration_msec > 0)) AND (has_all_interaction_events))) /
       sumIf(sign, ((has_all_interaction_events AND
                     (view_duration_msec <= 1800000 AND view_duration_msec >= 0)))) AS activityRate, 
                     custom_vars_view.position, custom_vars_view.value, query, path, prefix,
       (session_id),
       (session_number_of_views)
 FROM views
WHERE project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
        AND is_excludable = 0

AND Page
GROUP BY session_id, session_number_of_views, custom_vars_view.position, custom_vars_view.value, query, path, prefix
 )
GROUP By Activity_Rate
ORDER BY Activity_Rate
SETTINGS distributed_group_by_no_merge = 0;"""

QUERY_activity_transaction = """ WITH
(({page_condition})) as the_Page

SELECT
       multiIf(
    (activityRate >0 AND activityRate <0.1), '1. 10%',
    (activityRate >0.1 AND activityRate <0.2), '2. 20%',
    (activityRate >0.2 AND activityRate <0.3), '3. 30%',
    (activityRate >0.3 AND activityRate <0.4), '4. 40%',
    (activityRate >0.5 AND activityRate <0.6), '5. 50%',
    (activityRate >0.6 AND activityRate <0.7), '6. 60%',
    (activityRate >0.7 AND activityRate <0.8), '7. 70%',
    (activityRate >0.8 AND activityRate <0.9), '8. 80%',
    (activityRate >0.9 AND activityRate <1), '9. 90%',
    (activityRate = 1), 'z10. 100%'
, 'zznull') as Activity_Rate,
       uniqExact(session_id) as All_Sessions,
       uniqExactIf(session_id, the_Page AND session_number_of_transactions=1) AS Transaction_Sessions

FROM
(
WITH
(({page_condition})) as Page
SELECT sumIf(sign *
             if(view_duration_msec < page_interaction_time_msec, view_duration_msec, page_interaction_time_msec) /
             view_duration_msec,
             (((view_duration_msec <= 1800000) AND (view_duration_msec > 0)) AND (has_all_interaction_events))) /
       sumIf(sign, ((has_all_interaction_events AND
                     (view_duration_msec <= 1800000 AND view_duration_msec >= 0)))) AS activityRate, 
                     custom_vars_view.position, custom_vars_view.value, query, path, prefix,
       (session_id),
       (session_number_of_views),
       (session_number_of_transactions)            
 FROM views
WHERE project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
        AND is_excludable = 0

AND Page
GROUP BY session_id, session_number_of_views, custom_vars_view.position, custom_vars_view.value, query, session_number_of_transactions, path, prefix
 )
GROUP By Activity_Rate
ORDER BY Activity_Rate
SETTINGS distributed_group_by_no_merge = 0;"""