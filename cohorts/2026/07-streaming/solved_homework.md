# Homework

In this homework, we'll practice streaming with Kafka (Redpanda) and PyFlink.

We use Redpanda, a drop-in replacement for Kafka. It implements the same
protocol, so any Kafka client library works with it unchanged.

For this homework we will be using Green Taxi Trip data from October 2025:

- [green_tripdata_2025-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet)

## Setup

We'll use the same infrastructure from the [workshop](../../../07-streaming/workshop/).

Follow the setup instructions: build the Docker image, start the services:

```bash
cd 07-streaming/workshop/
docker compose build
docker compose up -d
```

```plain
CONTAINER ID   IMAGE                           COMMAND                  CREATED          STATUS          PORTS                                                                                                                                                                                                            NAMES
903efa2d44a1   pyflink-workshop                "/docker-entrypoint.…"   38 seconds ago   Up 36 seconds   6121-6123/tcp, 8081/tcp                                                                                                                                                                                          workshop-taskmanager-1
8a3247075e0f   pyflink-workshop                "/docker-entrypoint.…"   39 seconds ago   Up 37 seconds   6123/tcp, 0.0.0.0:8081->8081/tcp, [::]:8081->8081/tcp                                                                                                                                                            workshop-jobmanager-1
f58e4ca5f90a   postgres:18                     "docker-entrypoint.s…"   39 seconds ago   Up 37 seconds   0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp                                                                                                                                                                      workshop-postgres-1
b23daa017654   redpandadata/redpanda:v25.3.9   "/entrypoint.sh redp…"   39 seconds ago   Up 37 seconds   0.0.0.0:8082->8082/tcp, [::]:8082->8082/tcp, 8081/tcp, 0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp, 0.0.0.0:28082->28082/tcp, [::]:28082->28082/tcp, 9644/tcp, 0.0.0.0:29092->29092/tcp, [::]:29092->29092/tcp   workshop-redpanda-1
```

This gives us:

- Redpanda (Kafka-compatible broker) on `localhost:9092`
- Flink Job Manager at [http://localhost:8081](http://localhost:8081)

![alt text](media/image-01-flink-job-manager.png)

- Flink Task Manager
- PostgreSQL on `localhost:5432` (user: `postgres`, password: `postgres`)

If you previously ran the workshop and have old containers/volumes,
do a clean start:

```bash
docker compose down -v
docker compose build
docker compose up -d
```

Note: the container names (like `workshop-redpanda-1`) assume the
directory is called `workshop`. If you renamed it, adjust accordingly.

## Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

```plain
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
```

## Question 2. Sending data to Redpanda

Create a topic called `green-trips`:

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

```plain
TOPIC        STATUS
green-trips  OK
```

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

Convert each row to a dictionary and send it to the `green-trips` topic.
You'll need to handle the datetime columns - convert them to strings
before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:

```python
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?

- 10 seconds
- 60 seconds
- 120 seconds
- 300 seconds

```plain
Sent: GreenRide(PULocationID=247, DOLocationID=69, trip_distance=0.7, total_amount=10.0, lpep_pickup_datetime=1759278107000, lpep_dropoff_datetime=1759278277000, passenger_count=1, tip_amount=1.7)
Sent: GreenRide(PULocationID=66, DOLocationID=25, trip_distance=1.61, total_amount=16.68, lpep_pickup_datetime=1759277643000, lpep_dropoff_datetime=1759278254000, passenger_count=1, tip_amount=2.78)
Sent: GreenRide(PULocationID=244, DOLocationID=244, trip_distance=0.0, total_amount=13.2, lpep_pickup_datetime=1759277804000, lpep_dropoff_datetime=1759277807000, passenger_count=1, tip_amount=2.2)
...
Sent: GreenRide(PULocationID=41, DOLocationID=41, trip_distance=0.83, total_amount=10.5, lpep_pickup_datetime=1759341185000, lpep_dropoff_datetime=1759341426000, passenger_count=1, tip_amount=0.0)
Sent: GreenRide(PULocationID=82, DOLocationID=138, trip_distance=3.68, total_amount=28.7, lpep_pickup_datetime=1759338371000, lpep_dropoff_datetime=1759338997000, passenger_count=1, tip_amount=2.0)
Sent: GreenRide(PULocationID=82, DOLocationID=138, trip_distance=3.62, total_amount=32.04, lpep_pickup_datetime=1759340063000, lpep_dropoff_datetime=1759340693000, passenger_count=1, tip_amount=5.34)
took 11.36 seconds
```

## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic
(set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

- **6506** (answer)
- 7506
- 8506
- 9506

```plain
number of rides which trip_distance > 5:  5887
```

## Part 2: PyFlink (Questions 4-6)

For the PyFlink questions, you'll adapt the workshop code to work with
the green taxi data. The key differences from the workshop:

- Topic name: `green-trips` (instead of `rides`)
- Datetime columns use `lpep_` prefix (instead of `tpep_`)
- You'll need to handle timestamps as strings (not epoch milliseconds)

You can convert string timestamps to Flink timestamps in your source DDL:

```sql
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Before running the Flink jobs, create the necessary PostgreSQL tables
for your results.

Important notes for the Flink jobs:

- Place your job files in `workshop/src/job/` - this directory is
  mounted into the Flink containers at `/opt/src/job/`
- Submit jobs with:
  `docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/your_job.py`
- The `green-trips` topic has 1 partition, so set parallelism to 1
  in your Flink jobs (`env.set_parallelism(1)`). With higher parallelism,
  idle consumer subtasks prevent the watermark from advancing.
- Flink streaming jobs run continuously. Let the job run for a minute
  or two until results appear in PostgreSQL, then query the results.
  You can cancel the job from the Flink UI at [http://localhost:8081](http://localhost:8081)
- If you sent data to the topic multiple times, delete and recreate
  the topic to avoid duplicates:
  `docker exec -it workshop-redpanda-1 rpk topic delete green-trips`

## Question 4. Tumbling window - pickup location

Create a Flink job that reads from `green-trips` and uses a 5-minute
tumbling window to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns:
`window_start`, `PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

- 42
- **74** (ANSWER)
- 75
- 166

```sql
SELECT PULocationID, num_trips
FROM processed_events_aggregated_5min
ORDER BY num_trips DESC
LIMIT 3;
```

```plain
| PULocationID | num_trips |
|--------------|-----------|
| 74           | 61        |
| 74           | 60        |
| 74           | 58        |
```

## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap
on `PULocationID`, using `lpep_pickup_datetime` as the event time
with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other.
When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID`
with the longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- 31
- 51
- **81** (Closest to the answer)

```sql
SELECT MAX(num_trips) AS longest_session_trips
FROM aggregation_q5;
```

```plain
| longest_session_trips |
|-----------------------|
| 301                   |
```

## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the
total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

- **2025-10-01 18:00:00** (closest to the answer)
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

```sql
SELECT window_start, tip_revenue
FROM aggregation_q6
ORDER BY tip_revenue DESC
LIMIT 1;
```

```plain
| window_start            | tip_revenue |
|-------------------------|-------------|
| 2025-10-03 22:00:00.000 | 227.5       |
```

## Submitting the solutions

- Form for submitting: [https://courses.datatalks.club/de-zoomcamp-2026/homework/hw7](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw7)

## Learning in public

We encourage everyone to share what they learned.
Read more about the [benefits of learning in public](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

## Example post for LinkedIn

```markdown
Week 7 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 7 - Streaming with PyFlink. Learned how to:

- Set up Redpanda as a Kafka replacement
- Build Kafka producers and consumers in Python
- Create tumbling and session windows in Flink
- Analyze real-time taxi trip data with stream processing

Here's my homework solution: <LINK>

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

## Example post for Twitter/X

```markdown
Module 7 of Data Engineering Zoomcamp done!

- Kafka producers and consumers
- PyFlink tumbling and session windows
- Real-time taxi data analysis
- Redpanda as Kafka replacement

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```
