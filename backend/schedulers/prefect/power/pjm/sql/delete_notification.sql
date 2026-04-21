DELETE FROM logging.pipeline_runs 

WHERE 
    event_timestamp::DATE = current_date 
    AND event_type = 'SLACK_SENT' 
    AND pipeline_name = 'da_hrl_lmps'