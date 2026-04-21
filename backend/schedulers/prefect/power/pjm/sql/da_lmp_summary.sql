-- DA LMP summary for Slack notification
-- Returns hourly LMPs for a given date, WESTERN HUB
-- Python computes onpeak/offpeak/peak hour from the result

SELECT
    datetime_beginning_utc
    ,datetime_beginning_ept
    ,pnode_id
    ,pnode_name
    ,voltage
    ,equipment
    ,type
    ,zone
    ,system_energy_price_da
    ,total_lmp_da
    ,congestion_price_da
    ,marginal_loss_price_da
FROM pjm.da_hrl_lmps
WHERE datetime_beginning_ept::DATE = '{target_date}'::DATE
  AND pnode_name = 'WESTERN HUB'
