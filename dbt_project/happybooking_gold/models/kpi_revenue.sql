-- kpi_revenue: city-level revenue and booking behavior summary.
--
-- Bookings with no resolvable city (city was NULL or unmatched
-- after Silver cleaning) are excluded from this geographic rollup,
-- since they can't be attributed to a city_id - they still exist
-- in fact_booking, just not represented here.

{{ config(materialized='table') }}

select
    fact_booking.city_id,
    dim_city.city,
    dim_city.country,
    count(*) as total_bookings,
    sum(fact_booking.total_price) as total_revenue,
    avg(fact_booking.total_price) as avg_booking_value,
    sum(case when fact_booking.is_cancelled = true then 1 else 0 end) as cancelled_bookings,
    round(
        sum(case when fact_booking.is_cancelled = true then 1 else 0 end)
        / count(*) * 100,
        2
    ) as cancellation_rate_pct,
    sum(case when fact_booking.has_valid_keys = false then 1 else 0 end) as invalid_key_bookings
from {{ ref('fact_booking') }} as fact_booking
left join {{ ref('dim_city') }} as dim_city
    on fact_booking.city_id = dim_city.city_id
where fact_booking.city_id is not null
group by
    fact_booking.city_id,
    dim_city.city,
    dim_city.country