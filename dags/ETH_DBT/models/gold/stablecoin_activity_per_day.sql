{{ config(
    materialized='incremental',
    unique_key=['date', 'token_address'],
    incremental_strategy='merge'
) }}

/*
===============================================================================
Model: daily_stablecoin_volume
Description: Tracks the daily transaction volume for the top two US Dollar 
             stablecoins on Ethereum: USDT and USDC.
Notes: 
  - Filters strictly for USDT (0xdac17f...) and USDC (0xa0b869...).
  - Divides raw value by 1e6 to adjust for the 6-decimal precision of stablecoins.
Dependencies: 
  - Source: ETH.TOKEN_TRANSFERS
===============================================================================
*/

select
date,
token_address,
sum(value/1e6) as total_usd_value

from {{ source('ETH', 'TOKEN_TRANSFERS')}}

where lower(token_address) in ('0xdac17f958d2ee523a2206206994597c13d831ec7', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')

{% if is_incremental() %}
and date > (select max(date) from {{ this }})
{% endif %}

group by 
date, token_address