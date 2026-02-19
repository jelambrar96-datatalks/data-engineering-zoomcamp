with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
    -- Filter out records with null dispatching_base_num (data quality requirement)
    where dispatching_base_num is not null
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(Affiliated_base_number as string) as affiliated_base_number,
        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- flags
        cast(SR_Flag as integer) as shared_request_flag

    from source
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
