
/*
===============================================================================
Model: transactions_enriched
Description: Enriches raw Ethereum transactions by joining them with token 
             transfers and categorizing them into distinct buckets 
             (contract creations, token transfers, or plain ETH transfers).
Dependencies: 
  - Source: ETH.TRANSACTIONS
  - Source: ETH.TOKEN_TRANSFERS
===============================================================================
*/

select

t.hash,
t.block_number,
t.date,
t.from_address,
t.to_address,
t.value,
t.receipt_contract_address,
t.input,
tt.token_transfer_count,

case
    when t.receipt_contract_address != '' then 'contract_creation'
    when tt.transaction_hash is not null then 'token_transfer'
    when t.input = '0x' and t.value > 0 then 'plain_eth_transfer'
    else 'other'
end as transaction_category

from {{ source('ETH','TRANSACTIONS')}} t

left join (

	select
	transaction_hash,
	count(*) as token_transfer_count
	from {{ source('ETH','TOKEN_TRANSFERS')}}
	group by transaction_hash
	) tt

on t.hash = tt.transaction_hash

