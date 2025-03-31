-- Drop existing table
DROP TABLE IF EXISTS activity_dev.membershiptransactionmonthly;

-- Recreate with explicit clustering order
CREATE TABLE activity_dev.membershiptransactionmonthly (
    phone text,
    membershipcode text,
    month int,
    year int,
    rank int,
    membershipname text,
    totalpoints int,
    timestamp bigint,
    PRIMARY KEY ((phone), month, year)
) WITH CLUSTERING ORDER BY (month ASC, year ASC);