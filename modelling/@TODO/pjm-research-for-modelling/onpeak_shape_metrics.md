# On-Peak (HE8-23) Shape Metrics — Literature Delta

Sister doc to `backtest_eval_metrics.md`. The 24-hour spec there
(variogram `p=0.5`, `w=1/lag`; argmax/argmin extremum errors;
`peak_window_mae`; `first_diff_mae`) is locked. This file records only
**what changes when the evaluation window shrinks to PJM on-peak
(HE8-23, 16 hours, weekdays ex NERC holidays — Ref [P1])**.

## 1. Truncated variogram (max lag 15 instead of 23)

The `1/lag` weighting is a generic "inverse distance between components"
construction, not a 24-specific choice — Scheuerer & Hamill 2015
(Ref [V1]) and the recent re-derivation in Allen et al. ASCMO 2025
(Ref [V2], same paper as `backtest_eval_metrics.md` Ref [4]) define it
over arbitrary index sets, with normalization by `Σ w_{ij}` making the
score scale-comparable across window lengths. Truncating to 16 hours
keeps 120 pairs (= 16·15/2) vs 276 — the relative weighting structure
(short-lag emphasis) is preserved, and the locked sum-of-weights
normalization handles the count change automatically. **Implication:**
reuse the 24-hour formula verbatim on the HE8-23 slice; no re-tuning of
`p` or weight scheme. The only real loss is statistical power per day
(half the pairs), so cross-day aggregation matters more — keep the
deferred per-day `actual.std()` normalization (`backtest_eval_metrics.md`
§5.2) on the table for the 16-hour version specifically.

No published treatment of "truncated variogram score" as a named object
turned up — the search returned only geostatistical truncated-Gaussian
work, unrelated. That's a real finding: there is no special literature,
because there's no special problem. The score is index-set-agnostic by
construction.

## 2. Extremum metrics when the true peak falls outside HE8-23

This is the only question with a non-obvious precedent. Hong et al.'s
**BigDEAL Challenge 2022** (Ref [B1], Shukla et al. IET Smart Grid 2024)
ran exactly this problem on load: separate "peak timing" (Track T) and
"peak shape" (Track S) tracks, with peak shape defined over a fixed
clock window and peak timing defined over the full day. They introduced
**Peak Shape Error** as a window-restricted metric distinct from
**MAE-of-peak-timing** computed over the full 24h. **Implication for
us:** report both. On HE8-23, compute the locked extremum metrics
*restricted to HE8-23* (`window_argmax`, `window_argmin`) — answers
"did we get the on-peak shape right." Separately, on the full 24h,
compute the daily `argmax` and flag any day where
`daily_argmax not in [8, 23]` as `peak_outside_onpeak=True`. Do **not**
expand the window or impute — the on-peak block has a market-defined
boundary (the futures contract settles on it), so reporting on HE0-23 is
a different product. Surface the flag; aggregate the rate
(`mean(peak_outside_onpeak)` per scenario) as a separate diagnostic
column. Winter morning peak HE6-7 days will dominate this flag — that's
the signal, not noise.

## 3. On-peak block scoring conventions in EPF/load benchmarks

Confirmed by direct inspection of `epftoolbox/evaluation/_mae.py`
(Ref [E1]): the canonical EPF benchmark library does **not** ship any
block/peak-hour evaluation primitive. MAE/RMAE/MASE/sMAPE all reduce to
`np.mean(np.abs(...))` over whatever array is passed in — block scoring
is a caller-side filter, not a library concept. GEFCom2014 price track
(Ref [G1], Hong et al. 2016) and GEFCom2017 hierarchical load
(Ref [G2]) both score on pinball loss aggregated over **all** target
hours/quantiles uniformly; no on-peak/off-peak split appears in either
competition's evaluation. The PJM-style HE8-23 block is a **trading
convention** (Ref [P1], futures settle on the simple mean of DA LMPs
over the 16 hours), not an academic forecasting one — academic EPF
treats the day as a 24-vector and lets the modeler stratify ex post.

**Implication:** the right move is to compute **both** (a) block-mean
error `mean(forecast[8:24]) - mean(actual[8:24])` (matches contract
settlement, the only number a trader cares about for the block product)
and (b) shape-within-block metrics from §1-2 (variogram + extremum
errors restricted to HE8-23). These answer different questions. There is
no benchmark to cite for a combined block+shape headline; we are
inventing the convention. Recent EPF work (Janczura & Wójcik 2025,
Ref [E2]) shows level metrics (MAE/RMSE) are weakly correlated with
trading P&L while shape correlation (Corr-f) is nearly linear — supports
keeping shape in the leaderboard regardless of the block-mean number.

## 4. References

| Ref | Citation |
|-----|----------|
| [P1] | PJM. "On-Peak and Off-Peak Definitions — General and Settlements." HE 0800-2300 weekdays EPT, ex NERC holidays. https://pjm.my.site.com/publicknowledge/s/article/On-peak-and-Off-peak-definitions-General-and-Settlements |
| [V1] | Scheuerer & Hamill 2015 (= `backtest_eval_metrics.md` Ref [2]). https://journals.ametsoc.org/view/journals/mwre/143/4/mwr-d-14-00269.1.xml |
| [V2] | Allen et al. "Proper scoring rules for multivariate probabilistic forecasts based on aggregation and transformation." ASCMO 2025. https://ascmo.copernicus.org/articles/11/23/2025/ |
| [B1] | Shukla, Hong et al. "BigDEAL Challenge 2022: Forecasting peak timing of electricity demand." IET Smart Grid 2024. https://ietresearch.onlinelibrary.wiley.com/doi/10.1049/stg2.12162 |
| [E1] | epftoolbox `evaluation/_mae.py` (master). https://github.com/jeslago/epftoolbox/blob/master/epftoolbox/evaluation/_mae.py |
| [E2] | Janczura & Wójcik. "Statistical and economic evaluation of forecasts in electricity markets: beyond RMSE and MAE." arXiv:2511.13616 (2025). https://arxiv.org/abs/2511.13616 |
| [G1] | Hong, Pinson, Fan, Zareipour, Troccoli, Hyndman. "Probabilistic energy forecasting: GEFCom2014 and beyond." Int. J. Forecasting 2016. http://pierrepinson.com/docs/Hongetal2016.pdf |
| [G2] | Hong, Xie, Black. "GEFCom2017: Hierarchical probabilistic load forecasting." Int. J. Forecasting 2019. https://www.sciencedirect.com/science/article/abs/pii/S016920701930024X |
