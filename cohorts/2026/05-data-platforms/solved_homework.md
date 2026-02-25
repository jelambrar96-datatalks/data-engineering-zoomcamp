# Module 5 Homework: Data Platforms with Bruin

In this homework, we'll use Bruin to build a complete data pipeline, from ingestion to reporting.

## Setup

1. Install Bruin CLI: `curl -LsSf https://getbruin.com/install/cli | sh`
2. Initialize the zoomcamp template: `bruin init zoomcamp my-pipeline`
3. Configure your `.bruin.yml` with a DuckDB connection
4. Follow the tutorial in the [main module README](../../../05-data-platforms/)

After completing the setup, you should have a working NYC taxi data pipeline.

---

### Question 1. Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

- `bruin.yml` and `assets/`
- `.bruin.yml` and `pipeline.yml` (assets can be anywhere)
- **`.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`** (ANSWER)
- `pipeline.yml` and `assets/` only

```plain
.
├── bruin-pipeline
│   ├── assets
│   │   ├── my_python_asset.py
│   │   ├── players.asset.yml
│   │   └── player_stats.sql
│   ├── pipeline.yml
│   └── README.md
├── duckdb.db
└── logs
```

---

### Question 2. Materialization Strategies

You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

- **`append` - always add new rows** (ANSWER)
- `replace` - truncate and rebuild entirely
- `time_interval` - incremental based on a time column
- `view` - create a virtual table only

---

### Question 3. Pipeline Variables

You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

- `bruin run --taxi-types yellow`
- `bruin run --var taxi_types=yellow`
- **`bruin run --var 'taxi_types=["yellow"]'`** (ANSWER)
- `bruin run --set taxi_types=["yellow"]`

---

### Question 4. Running with Dependencies

You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

- `bruin run ingestion.trips --all`
- **`bruin run ingestion/trips.py --downstream`** (ANSWER)
- `bruin run pipeline/trips.py --recursive`
- `bruin run --select ingestion.trips+`

---

### Question 5. Quality Checks

You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

- `name: unique`
- **`name: not_null`** (ANSWER)
- `name: positive`
- `name: accepted_values, value: [not_null]`

---

### Question 6. Lineage and Dependencies

After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

- `bruin graph`
- `bruin dependencies`
- **`bruin lineage`** (ANSWER)
- `bruin show`

---

### Question 7. First-Time Run

You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

- `--create`
- `--init`
- **`--full-refresh`** (ANSWER)
- `--truncate`

---

## Submitting the solutions

- Form for submitting: <https://courses.datatalks.club/de-zoomcamp-2026/homework/hw5>

=======

## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example post for [Linkedin](https://www.linkedin.com/posts/jelambrar96_github-datatalksclubdata-engineering-zoomcamp-share-7432467501760020480-qtet?utm_source=social_share_send&utm_medium=member_desktop_web&rcm=ACoAACWmcWwBQc9OQJ6mmIIAo22Xwuwa8p_gRR4).

```
🚀 Week 2 of Data Engineering Zoomcamp by DataTalksClub and Will Russell complete!

Just finished Module 2 - Workflow Orchestration with Kestra. Learned how to:

✅ Orchestrate data pipelines with Kestra flows
✅ Use variables and expressions for dynamic workflows
✅ Implement backfill for historical data
✅ Schedule workflows with timezone support
✅ Process NYC taxi data (Yellow & Green) for 2019-2021

Built ETL pipelines that extract, transform, and load taxi trip data automatically!

Thanks to the Kestra team for the great orchestration tool!

Here's my homework solution: https://lnkd.in/daF_qgCV

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://lnkd.in/eg5mjkmA

#DataEngineer #BigData #LearningInPublic #ContinuousLearning #DataScience #Python #dezoomcamp #kestra
```

### Example post for [Twitter/X](https://x.com/jelambrar/status/2026702634921476254)

```
📊 Module 5 of Data Engineering Zoomcamp done!

- Data Platforms with Bruin
- End-to-end ELT pipelines
- Data quality & lineage
- Deployment to BigQuery

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/

#DataEngineer #BigData #bruin #dezoomcamp
```
