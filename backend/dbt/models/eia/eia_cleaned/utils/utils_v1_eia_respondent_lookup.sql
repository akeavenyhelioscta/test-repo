{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- EIA-930 RESPONDENT LOOKUP
---------------------------

WITH LOOKUP AS (
    SELECT * FROM (
        VALUES
            -- US48
            ('US48', 'Lower 48', 'Eastern', 'US48', FALSE),

            -- NE
            ('ISONE', 'ISO New England', 'Eastern', 'NE', TRUE),

            -- NY
            ('NYISO', 'New York Independent System Operator', 'Eastern', 'NY', TRUE),

            -- MIDW
            ('MISO', 'Midcontinent Independent System Operator, Inc.', 'Eastern', 'MIDW', TRUE),
            ('AECI', 'Associated Electric Cooperative, Inc.', 'Central', 'MIDW', FALSE),
            ('LGEE', 'Louisville Gas and Electric Company and Kentucky Utilities Company', 'Eastern', 'MIDW', FALSE),

            -- MIDA
            ('PJM', 'PJM Interconnection, LLC', 'Eastern', 'MIDA', TRUE),

            -- TEN
            ('TVA', 'Tennessee Valley Authority', 'Central', 'TEN', FALSE),

            -- CAR
            ('CPLE', 'Duke Energy Progress East', 'Eastern', 'CAR', FALSE),
            ('CPLW', 'Duke Energy Progress West', 'Eastern', 'CAR', FALSE),
            ('DUK', 'Duke Energy Carolinas', 'Eastern', 'CAR', FALSE),
            ('SC', 'South Carolina Public Service Authority', 'Eastern', 'CAR', FALSE),
            ('SCEG', 'Dominion Energy South Carolina, Inc.', 'Eastern', 'CAR', FALSE),
            ('YAD', 'Alcoa Power Generating, Inc. - Yadkin Division', 'Eastern', 'CAR', FALSE),

            -- SE
            ('SE', 'Southeast', 'Central', 'SE', FALSE),
            ('SEPA', 'Southeastern Power Administration', 'Central', 'SE', FALSE),
            ('SOCO', 'Southern Company Services, Inc. - Trans', 'Central', 'SE', FALSE),

            -- FLA
            ('FMPP', 'Florida Municipal Power Pool', 'Eastern', 'FLA', FALSE),
            ('FPC', 'Duke Energy Florida, Inc.', 'Eastern', 'FLA', FALSE),
            ('FPL', 'Florida Power & Light Co.', 'Eastern', 'FLA', FALSE),
            ('GVL', 'Gainesville Regional Utilities', 'Eastern', 'FLA', FALSE),
            ('HST', 'City of Homestead', 'Eastern', 'FLA', FALSE),
            ('JEA', 'JEA', 'Eastern', 'FLA', FALSE),
            ('NSB', 'Utilities Commission of New Smyrna Beach', 'Eastern', 'FLA', FALSE),
            ('SEC', 'Seminole Electric Cooperative', 'Eastern', 'FLA', FALSE),
            ('TAL', 'City of Tallahassee', 'Eastern', 'FLA', FALSE),
            ('TEC', 'Tampa Electric Company', 'Eastern', 'FLA', FALSE),

            -- CENT
            ('SPA', 'Southwestern Power Administration', 'Central', 'CENT', FALSE),
            ('SWPP', 'Southwest Power Pool', 'Central', 'CENT', FALSE),

            -- TEX
            ('ERCOT', 'Electric Reliability Council of Texas, Inc.', 'Central', 'TEX', TRUE),

            -- NW
            ('NW', 'Northwest', 'Pacific', 'NW', FALSE),
            ('AVA', 'Avista Corporation', 'Pacific', 'NW', FALSE),
            ('AVRN', 'Avangrid Renewables, LLC', 'Pacific', 'NW', FALSE),
            ('BPAT', 'Bonneville Power Administration', 'Pacific', 'NW', FALSE),
            ('CHPD', 'Public Utility District No. 1 of Chelan County', 'Pacific', 'NW', FALSE),
            ('DOPD', 'PUD No. 1 of Douglas County', 'Pacific', 'NW', FALSE),
            ('GCPD', 'Public Utility District No. 2 of Grant County, Washington', 'Pacific', 'NW', FALSE),
            ('GRID', 'Gridforce Energy Management, LLC', 'Pacific', 'NW', FALSE),
            ('GWA', 'NaturEner Power Watch, LLC', 'Mountain', 'NW', FALSE),
            ('IPCO', 'Idaho Power Company', 'Pacific', 'NW', FALSE),
            ('NEVP', 'Nevada Power Company', 'Pacific', 'NW', FALSE),
            ('NWMT', 'NorthWestern Corporation', 'Mountain', 'NW', FALSE),
            ('PACE', 'PacifiCorp East', 'Mountain', 'NW', FALSE),
            ('PACW', 'PacifiCorp West', 'Pacific', 'NW', FALSE),
            ('PGE', 'Portland General Electric Company', 'Pacific', 'NW', FALSE),
            ('PSCO', 'Public Service Company of Colorado', 'Mountain', 'NW', FALSE),
            ('PSEI', 'Puget Sound Energy, Inc.', 'Pacific', 'NW', FALSE),
            ('SCL', 'Seattle City Light', 'Pacific', 'NW', FALSE),
            ('TPWR', 'City of Tacoma, Department of Public Utilities, Light Division', 'Pacific', 'NW', FALSE),
            ('WACM', 'Western Area Power Administration - Rocky Mountain Region', 'Arizona', 'NW', FALSE),
            ('WAUW', 'Western Area Power Administration - Upper Great Plains West', 'Mountain', 'NW', FALSE),
            ('WWA', 'NaturEner Wind Watch, LLC', 'Mountain', 'NW', FALSE),

            -- SW
            ('SW', 'Southwest', 'Arizona', 'SW', FALSE),
            ('AZPS', 'Arizona Public Service Company', 'Arizona', 'SW', FALSE),
            ('DEAA', 'Arlington Valley, LLC', 'Arizona', 'SW', FALSE),
            ('EPE', 'El Paso Electric Company', 'Arizona', 'SW', FALSE),
            ('HGMA', 'New Harquahala Generating Company, LLC', 'Arizona', 'SW', FALSE),
            ('PNM', 'Public Service Company of New Mexico', 'Arizona', 'SW', FALSE),
            ('SRP', 'Salt River Project Agricultural Improvement and Power District', 'Arizona', 'SW', FALSE),
            ('TEPC', 'Tucson Electric Power', 'Arizona', 'SW', FALSE),
            ('WALC', 'Western Area Power Administration - Desert Southwest Region', 'Arizona', 'SW', FALSE),

            -- CAL
            ('BANC', 'Balancing Authority of Northern California', 'Pacific', 'CAL', FALSE),
            ('CAISO', 'California Independent System Operator', 'Pacific', 'CAL', TRUE),
            ('IID', 'Imperial Irrigation District', 'Pacific', 'CAL', FALSE),
            ('LDWP', 'Los Angeles Department of Water and Power', 'Pacific', 'CAL', FALSE),
            ('TIDC', 'Turlock Irrigation District', 'Pacific', 'CAL', FALSE)

    ) AS t(respondent, balancing_authority_name, time_zone, region, is_iso)
),

FINAL AS (
    SELECT
        respondent::VARCHAR AS respondent,
        balancing_authority_name::VARCHAR AS balancing_authority_name,
        time_zone::VARCHAR AS time_zone,
        region::VARCHAR AS region,
        is_iso::BOOLEAN AS is_iso
    FROM LOOKUP
)

SELECT * FROM FINAL
