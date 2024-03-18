# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

Note that we consider the do not consider `a->b` and `b->a` as the same trip pair.
So as an example, you would consider the following trip pairs as different pairs:
```plaintext
Yorkville East -> Steinway
Steinway -> Yorkville East
```

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

Options:
1. Yorkville East, Steinway
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

p.s. The trip time between taxi zones does not take symmetricity into account, i.e. `A -> B` and `B -> A` are considered different trips. This applies to subsequent questions as well.

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

Options:
1. 5
2. 3
3. 10
4. 1

SQL

CREATE MATERIALIZED VIEW taxi_zone_trip_durations AS
SELECT
    tp.pulocationid AS pickup_zone_id,
    tzp.Zone AS pickup_zone_name,
    tp.dolocationid AS dropoff_zone_id,
    tzd.Zone AS dropoff_zone_name,
    AVG(EXTRACT(EPOCH FROM (tp.tpep_dropoff_datetime - tp.tpep_pickup_datetime)) / 60) AS avg_trip_time_minutes,
    MIN(EXTRACT(EPOCH FROM (tp.tpep_dropoff_datetime - tp.tpep_pickup_datetime)) / 60) AS min_trip_time_minutes,
    MAX(EXTRACT(EPOCH FROM (tp.tpep_dropoff_datetime - tp.tpep_pickup_datetime)) / 60) AS max_trip_time_minutes,
    COUNT(*) AS trip_count
FROM
    trip_data tp
JOIN
    taxi_zone tzp ON tp.pulocationid = tzp.location_id
JOIN
    taxi_zone tzd ON tp.dolocationid = tzd.location_id
GROUP BY
    tp.pulocationid, tzp.Zone, tp.dolocationid, tzd.Zone;
---

SELECT
    pickup_zone_name,
    dropoff_zone_name,
    avg_trip_time_minutes,
    min_trip_time_minutes,
    max_trip_time_minutes,
    trip_count
FROM
    taxi_zone_trip_durations
ORDER BY
    avg_trip_time_minutes DESC
LIMIT 1;



## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.




Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

SQL

WITH LatestPickupTime AS (
    SELECT MAX(tpep_pickup_datetime) AS max_pickup_time
    FROM trip_data
),
Timeframe AS (
    SELECT
        max_pickup_time - INTERVAL '17 hours' AS start_time,
        max_pickup_time AS end_time
    FROM LatestPickupTime
),
BusiestZones AS (
    SELECT
        tp.pulocationid,
        tz.Zone,
        COUNT(*) AS num_pickups
    FROM
        trip_data tp
    JOIN Timeframe tf ON tp.tpep_pickup_datetime BETWEEN tf.start_time AND tf.end_time
    JOIN taxi_zone tz ON tp.pulocationid = tz.location_id
    GROUP BY tp.pulocationid, tz.Zone
    ORDER BY num_pickups DESC
    LIMIT 3
)
SELECT * FROM BusiestZones;