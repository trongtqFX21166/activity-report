USE activity_dev;

CREATE TABLE daily_rule_flow_metrics (
    campaignid text,
    campaignname text,
    ruleid text,
    rulename text,
    date date,
    month int,
    year int,
    timestamp bigint,
    rule_sequence int,
    total_profiles_evaluated int,
    profiles_evaluated int,
    total_actions int,
    addpoint_actions int,
    points_awarded int,
    total_successful_actions int,
    total_points_awarded int,
    profiles_not_matched int,
    PRIMARY KEY ((campaignid, date), rule_sequence, ruleid)
) WITH CLUSTERING ORDER BY (rule_sequence ASC, ruleid ASC);