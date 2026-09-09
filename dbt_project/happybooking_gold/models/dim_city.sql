-- dim_city: one row per unique city, enriched with weather and
-- currency context where available.
--
-- This is a dimension table - it describes the "who/where" context
-- for bookings, rather than the bookings themselves. Keeping it
-- separate from fact_booking follows standard star-schema design.
--
-- We deduplicate on city alone (not city+country), since the same
-- city name should map to exactly one dimension row - the source
-- data had inconsistent country values for a handful of cities,
-- which was causing a join fan-out downstream in fact_booking.

{{ config(materialized='table') }}

with distinct_cities as (

    select
        city,
        min(country) as country
    from {{ source('silver', 'silver_hotel_booking') }}
    where city is not null
    group by city

),

weather as (

    select
        queried_city,
        resolved_city,
        country as weather_country,
        temperature_c,
        humidity_pct,
        weather_code
    from {{ source('bronze', 'bronze_weather_enrichment') }}

)

select
    row_number() over (order by distinct_cities.city) as city_id,
    distinct_cities.city,
    distinct_cities.country,
    weather.temperature_c,
    weather.humidity_pct,
    weather.weather_code
from distinct_cities
left join weather
    on distinct_cities.city = weather.queried_city