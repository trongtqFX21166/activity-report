USE activity_dev;

CREATE TABLE activity_dev.activitytransaction (
    id text,
	phone text,
	date long,
	month int,
	year int,
	type varchar,
	event varchar,
	reportcode varchar, 
	campaignId varchar,
	ruleId varchar,
	campaignname text,
	rulename text,
	eventname text,
	reportname text,
	name text,
	value int,
	timestamp long
    PRIMARY KEY (id, phone, date, month, year, type, event, reportcode, campaignid, ruleid)
);
