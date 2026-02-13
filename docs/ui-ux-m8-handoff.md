# M8 Handoff and Metrics

## Scope
This document finalizes the UI/UX milestone rollout for DEM Blend Studio:
- Handoff checklist
- Operator quick-run protocol
- QA acceptance script
- UX metric definitions and collection method

## Delivered Milestones
1. M1: UX architecture and flow defined.
2. M2: Step-based shell + persistent KPI strip.
3. M3: Structured validation issues with blocking/non-blocking logic.
4. M4: Simulation decision visuals (remaining mass and contribution focus).
5. M5: Optimization workbench controls + leaderboard + change summary.
6. M6: Scenario compare + explainability + convergence snapshot.
7. M7: Accessibility and interaction hardening.

## Handoff Checklist
1. Product/ops walkthrough complete:
- Confirm workflow from `Load Sample -> Validate -> Run -> Optimize -> Compare`.

2. Accessibility baseline confirmed:
- Keyboard-only path works.
- Focus indicators visible.
- Live status updates announced (`aria-live` regions).

3. QA sanity script complete:
- Load sample.
- Validate with no blocking errors.
- Run simulation successfully.
- Optimize successfully.
- Confirm candidate table sorting and scenario compare panel.

4. Documentation updated:
- README includes operator quick start and shortcuts.
- Milestone docs committed under `docs/`.

## Operator Quick-Run Protocol
1. Click `Load Sample`.
2. Click `Validate Inputs`.
3. Resolve blocking issues if shown.
4. Click `Run Simulation`.
5. Set target + preset/iterations/seed.
6. Click `Optimize Blend`.
7. Review:
- KPI strip
- Top Candidates
- What Changed
- Scenario Compare
- Explainability and Convergence

## Keyboard Shortcuts
1. `Ctrl/Cmd + Enter`: Run simulation
2. `Ctrl/Cmd + Shift + O`: Optimize blend
3. `Skip to Main Content` link: appears on keyboard focus

## UX Metrics
Track metrics per session and aggregate weekly.

1. `time_to_first_valid_run_sec`
- Definition: seconds from first `Load Sample` to first successful simulation run.
- Target: <= 180 sec for first-time operator.

2. `validation_issue_identification_sec`
- Definition: seconds from validation response render to first user correction action.
- Target: <= 10 sec median.

3. `optimize_decision_time_sec`
- Definition: seconds from optimize completion to user selecting/accepting a candidate.
- Target: <= 20 sec median.

4. `blocking_error_rate`
- Definition: sessions with blocking validation errors / total sessions.
- Target: trend downward week-over-week.

5. `run_failure_rate`
- Definition: failed `/api/run` calls / total `/api/run` calls.
- Target: < 2% in stable environments.

6. `optimize_failure_rate`
- Definition: failed `/api/optimize` calls / total `/api/optimize` calls.
- Target: < 3% in stable environments.

## Data Collection Recommendations
1. Frontend events (minimal):
- `ui_load_sample_clicked`
- `ui_validate_completed`
- `ui_run_started`
- `ui_run_completed`
- `ui_optimize_started`
- `ui_optimize_completed`
- `ui_candidate_sorted`

2. Payload fields:
- timestamp
- elapsed_ms
- blocking_error_count
- warning_count
- run_status / optimize_status

3. Storage:
- Start with local structured logs, then move to centralized analytics.

## Residual Risks
1. No persistent user preferences for optimization controls yet.
2. No screenshot-based visual regression tests yet.
3. Candidate interpretation still text/table-heavy for non-technical audiences.

## Next Iteration (Post-M8)
1. Add export actions for candidate plans and compare summary.
2. Add chart-based visual regression testing.
3. Add richer scenario persistence (save/load named scenarios).
