select
    order_id,
    order_number,
    product,
    amount,
    current_timestamp as loaded_at
from {{ ref('stg_orders') }}
