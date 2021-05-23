
with bigquery_orders as (

    select

        user_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(id) as number_of_orders

    from {{ var("orders_catalog") }}.{{ var("orders_schema") }}.jaffle_shop_orders

    group by user_id

),


final as (

    select
        jaffle_shop_customers.id as customer_id,
        jaffle_shop_customers.first_name,
        jaffle_shop_customers.last_name,
        bigquery_orders.first_order_date,
        bigquery_orders.most_recent_order_date,
        coalesce(bigquery_orders.number_of_orders, 0) as number_of_orders

    from jaffle_shop_customers

    left join bigquery_orders  ON jaffle_shop_customers.id = bigquery_orders.user_id

)

select * from final
