-- fact_booking: one row per booking, the core measurable event
-- in this star schema. References dim_city via city_id.
--
-- Silver's batch and stream sources were deduplicated independently
-- before being unioned, but a handful of test-stream records
-- overlapped with rows already present in the batch split - so we
-- re-deduplicate here, keeping the most recently updated version
-- of each booking_id.

{{ config(materialized='table') }}

with bookings as (

    select
        *,
        row_number() over (
            partition by booking_id
            order by updated_at desc
        ) as row_num
    from {{ source('silver', 'silver_hotel_booking') }}

),

deduped as (

    select
        booking_id,
        hotel_id,
        customer_id,
        city,
        country,
        booking_date,
        checkin_date,
        checkout_date,
        nights,
        adults,
        children,
        infants,
        room_type,
        rooms_booked,
        total_price,
        room_price,
        tax_amount,
        service_fee,
        paid_amount,
        payment_status,
        payment_method,
        booking_status,
        is_cancelled,
        is_date_logic_valid,
        has_valid_keys
    from bookings
    where row_num = 1

)

select
    deduped.*,
    dim_city.city_id
from deduped
left join {{ ref('dim_city') }} as dim_city
    on deduped.city = dim_city.city