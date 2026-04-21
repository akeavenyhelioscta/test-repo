{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- EIA AREA NAME STANDARDIZATION LOOKUP
---------------------------

SELECT * FROM (
    VALUES
        ('CALIFORNIA', 'CALIFORNIA'),
        ('COLORADO', 'COLORADO'),
        ('FLORIDA', 'FLORIDA'),
        ('MASSACHUSETTS', 'MASSACHUSETTS'),
        ('MINNESOTA', 'MINNESOTA'),
        ('NEW YORK', 'NEW YORK'),
        ('OHIO', 'OHIO'),
        ('TEXAS', 'TEXAS'),
        ('U.S.', 'US48'),
        ('USA-AK', 'ALASKA'),
        ('USA-AL', 'ALABAMA'),
        ('USA-AR', 'ARKANSAS'),
        ('USA-AZ', 'ARIZONA'),
        ('USA-CT', 'CONNECTICUT'),
        ('USA-DC', 'DISTRICT OF COLUMBIA'),
        ('USA-DE', 'DELAWARE'),
        ('USA-GA', 'GEORGIA'),
        ('USA-HI', 'HAWAII'),
        ('USA-IA', 'IOWA'),
        ('USA-ID', 'IDAHO'),
        ('USA-IL', 'ILLINOIS'),
        ('USA-IN', 'INDIANA'),
        ('USA-KS', 'KANSAS'),
        ('USA-KY', 'KENTUCKY'),
        ('USA-LA', 'LOUISIANA'),
        ('USA-MD', 'MARYLAND'),
        ('USA-ME', 'MAINE'),
        ('USA-MI', 'MICHIGAN'),
        ('USA-MO', 'MISSOURI'),
        ('USA-MS', 'MISSISSIPPI'),
        ('USA-MT', 'MONTANA'),
        ('USA-NC', 'NORTH CAROLINA'),
        ('USA-ND', 'NORTH DAKOTA'),
        ('USA-NE', 'NEBRASKA'),
        ('USA-NH', 'NEW HAMPSHIRE'),
        ('USA-NJ', 'NEW JERSEY'),
        ('USA-NM', 'NEW MEXICO'),
        ('USA-NV', 'NEVADA'),
        ('USA-OK', 'OKLAHOMA'),
        ('USA-OR', 'OREGON'),
        ('USA-PA', 'PENNSYLVANIA'),
        ('USA-RI', 'RHODE ISLAND'),
        ('USA-SC', 'SOUTH CAROLINA'),
        ('USA-SD', 'SOUTH DAKOTA'),
        ('USA-TN', 'TENNESSEE'),
        ('USA-UT', 'UTAH'),
        ('USA-VA', 'VIRGINIA'),
        ('USA-VT', 'VERMONT'),
        ('USA-WI', 'WISCONSIN'),
        ('USA-WV', 'WEST VIRGINIA'),
        ('USA-WY', 'WYOMING'),
        ('WASHINGTON', 'WASHINGTON')
) AS t(area_name, area_name_standardized)
