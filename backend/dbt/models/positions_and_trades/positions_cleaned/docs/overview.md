{% docs positions_overview %}

# Positions Cleaned

This domain combines Marex and NAV position files into a consistent options/futures
positions mart used by the HeliosCTA desk for daily exposure, risk, and PnL workflow.

## Sources

- `marex_v5.marex_sftp_positions_v2_2026_feb_23`
- `nav_v5.nav_sftp_positions_*_v2_2026_feb_23` (AGR, MOROSS, PNT, TITAN)

## Layer Design

- `source/`: source-specific normalization and typing (ephemeral)
- `staging/`: merge, forward-fill, account/product enrichment, symbol generation (ephemeral)
- `marts/`: grouped and latest grouped analytical views (view)

Only models under `marts/` are exposed as database objects.

{% enddocs %}
