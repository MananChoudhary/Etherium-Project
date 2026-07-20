{{ config(
    materialized='incremental',
    unique_key=['date', 'transaction_category'],
    incremental_strategy='merge'
) }}

/*
===============================================================================
Model: daily_transaction_summary
Description: Aggregates enriched transactions to provide a daily summary of 
             transaction counts and total Ether volume per category.
Notes: 
  - Divides raw value by 1e18 to convert from Wei to human-readable Ether (ETH).
Dependencies: 
  - Ref: transactions_enriched
===============================================================================
*/

select
date,
transaction_category,
count(*) as tx_count,
sum(value)/1e18 as sum_eth_value

from {{ ref('transactions_enriched') }}

{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}

group by 
date,
transaction_category