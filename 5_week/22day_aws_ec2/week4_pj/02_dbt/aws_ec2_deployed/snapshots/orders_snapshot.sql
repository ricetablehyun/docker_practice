-- snapshots/orders_snapshot.sql
{% snapshot orders_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_sk',
        strategy='check',
        check_cols=['status', 'country']
    )
}}

SELECT * FROM {{ ref('stg_dirty') }}

{% endsnapshot %}