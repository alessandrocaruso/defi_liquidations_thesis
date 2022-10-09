WITH borrowers as (
select * from lending."borrow"),


liquidated_borrowers_greater_10 as (
SELECT liquidated_borrower, count(*) c from lending."liquidation" where block_time > '2021-01-01' group by 1 having count(*) < 10),

borrowers_liquidated_add as (SELECT project, version, block_time, tx_hash, borrower, asset_address, asset_symbol, token_amount, usd_value  
from borrowers
where project in ('Aave','MakerDAO','Compound') AND version = '2'
and borrower in (SELECT liquidated_borrower from liquidated_borrowers_greater_10)
order by block_time),

collateral_change_liquidated_data as (select * from lending."collateral_change"
where project in ('Aave','MakerDAO','Compound') AND version = '2' and usd_value is not null and borrower in (SELECT liquidated_borrower from liquidated_borrowers_greater_10)
order by block_time),

repayment_liquidated_data as (select * from lending."repay"
where project in ('Aave','MakerDAO','Compound') AND version = '2' and usd_value is not null and borrower in (SELECT liquidated_borrower from liquidated_borrowers_greater_10)
order by block_time),

liquidated_borrowers as (
SELECT * from lending."liquidation" where block_time > '2021-01-01' and liquidated_borrower in (
SELECT liquidated_borrower from liquidated_borrowers_greater_10)),


borrowings_data as (select project, version, block_time, tx_hash, borrower, asset_address, asset_symbol, token_amount, usd_value  
from borrowers
where project in ('Aave','MakerDAO','Compound') AND version = '2'
order by block_time),

collateral_change_data as (select * from lending."collateral_change"
where project in ('Aave','MakerDAO','Compound') AND version = '2' and usd_value is not null and borrower in (select distinct borrower from borrowings_data)
order by block_time),

lenders_position as (select borrower, project,
min(block_time) as first_action_collateral, 
max(block_time) as last_action_collateral,
count(tx_hash) as n_operations_made,
sum(usd_value) as balance,
SUM(CASE WHEN asset_symbol like '%ETH%' then 1 else 0 end) as n_collateral_changes_of_eth,
SUM(CASE WHEN asset_symbol like '%ETH%' then usd_value else 0 end) as usd_changes_of_eth,
SUM(CASE WHEN asset_symbol like '%AAVE%' then usd_value else 0 end) as usd_changes_of_aave,
SUM(CASE WHEN asset_symbol like '%DAI%' then usd_value else 0 end) as usd_changes_of_dai,
SUM(CASE WHEN asset_symbol like '%USDC%' then usd_value else 0 end) as usd_changes_of_usdc,
SUM(CASE WHEN asset_symbol like '%USDT%' then usd_value else 0 end) as usd_changes_of_usdt,
SUM(CASE WHEN asset_symbol like '%BTC%' then usd_value else 0 end) as usd_changes_of_btc,
COUNT(distinct asset_symbol) as distinct_asset_used_as_collateral,
COUNT(distinct project) as n_of_protocols_used
from collateral_change_data
group by 1,2),

borrowers_position as (select 
borrower, project,
min(block_time) as first_borrow, 
max(block_time) as last_borrow,
count(tx_hash) as n_borrowings,
sum(usd_value) as total_borrowed,
SUM(CASE WHEN asset_symbol like '%ETH%' then 1 else 0 end) as n_borrowings_of_eth,
SUM(CASE WHEN asset_symbol like '%ETH%' then usd_value else 0 end) as usd_borrows_of_eth,
SUM(CASE WHEN asset_symbol like '%AAVE%' then usd_value else 0 end) as usd_borrows_of_aave,
SUM(CASE WHEN asset_symbol like '%DAI%' then usd_value else 0 end) as usd_borrows_of_dai,
SUM(CASE WHEN asset_symbol like '%USDC%' then usd_value else 0 end) as usd_borrows_of_usdc,
SUM(CASE WHEN asset_symbol like '%USDT%' then usd_value else 0 end) as usd_borrows_of_usdt,
SUM(CASE WHEN asset_symbol like '%BTC%' then usd_value else 0 end) as usd_borrows_of_btc,
COUNT(distinct asset_symbol) as distinct_asset_borrowed_by_plf,
COUNT(distinct project) as n_of_protocols_used
from borrowings_data
group by 1,2),


repays_data as (select * from lending."repay" where (project != 'Aave' OR version != '1') and usd_value is not null and borrower in (select distinct borrower from borrowings_data)
order by block_time),

repayments_position as (select borrower, project,
MAX(block_time) as last_repayment,
COUNT(distinct asset_symbol) as distinct_asset_repayed_by_plf,
SUM(usd_value) as total_repayment,
SUM(CASE WHEN asset_symbol like '%ETH%' then usd_value else 0 end) as usd_repay_of_eth,
SUM(CASE WHEN asset_symbol like '%sUSD%' then usd_value else 0 end) as usd_repay_of_susd,
SUM(CASE WHEN asset_symbol like '%DAI%' then usd_value else 0 end) as usd_repay_of_dai,
SUM(CASE WHEN asset_symbol like '%USDC%' then usd_value else 0 end) as usd_repay_of_usdc,
SUM(CASE WHEN asset_symbol like '%USDT%' then usd_value else 0 end) as usd_repay_of_usdt,
SUM(CASE WHEN asset_symbol like '%BTC%' then usd_value else 0 end) as usd_repay_of_btc
from repays_data group by 1,2),

borrowers_distinct_asset_borrowed as 
(select borrower, count(distinct asset_symbol) as distinct_asset_borrowed_total 
from borrowings_data 
group by borrower),

borrowers_distinct_asset_used_as_collateral as (select borrower, count(distinct asset_symbol) as distinct_asset_used_as_collateral 
from collateral_change_data
group by borrower),

borrowers_distinct_asset as (select t1.borrower, t1. distinct_asset_borrowed_total, CASE WHEN t2.distinct_asset_used_as_collateral is NULL then 0 else t2.distinct_asset_used_as_collateral END
from borrowers_distinct_asset_borrowed t1 left JOIN
borrowers_distinct_asset_used_as_collateral t2 on t1.borrower = t2.borrower),

other as (select project, version, count(distinct borrower) as borrowers, count(tx_hash) tot_borrows from borrowings_data group by 1,2 limit 10),

unique_borrows_total as (select count(distinct borrower) from borrowings_data)

select * from repayment_liquidated_data 




