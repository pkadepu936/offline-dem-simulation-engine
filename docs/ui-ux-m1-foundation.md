# M1 UX Architecture and Flow

## Scope
This document defines the M1 foundation for the DEM Blend Studio UI:
- User journey
- Information architecture
- Screen/wireframe structure
- Component inventory
- Success criteria

## Primary Users
1. Operator
- Goal: quickly produce a valid simulation and recommended blend discharge plan.
- Needs: low-friction flow, clear errors, confidence in result.

2. Process Engineer
- Goal: inspect assumptions, compare scenarios, tune optimization.
- Needs: detailed metrics, transparent objective behavior, raw/debug access.

## Core Jobs To Be Done
1. Validate data health before running.
2. Run simulation and understand current state.
3. Optimize toward target quality parameters.
4. Compare baseline vs optimized outcomes.
5. Export/share decision-ready outputs.

## End-to-End Flow
1. Prepare Inputs
- Load sample or paste/edit payload.
- Surface dataset summary (rows, silos, lots, suppliers).

2. Validate
- Run schema + value checks.
- Show blocking and non-blocking issues.

3. Simulate
- Execute run with chosen fidelity (`steps` preset).
- Present run KPIs and per-silo state.

4. Optimize
- Enter target COA and optimization controls.
- Generate recommended discharge candidates.

5. Compare and Decide
- Compare baseline run vs optimized recommendation.
- Confirm objective score, parameter deltas, and tradeoffs.

6. Export
- Copy/download payload/result artifacts.

## Information Architecture
1. Global Header
- Title, run status, primary action cluster.

2. KPI Strip (persistent)
- Total discharged mass
- Total remaining mass
- Objective score
- Validation status

3. Main Workspace (step-based)
- Step 1: Input and Validation
- Step 2: Simulation Controls and Run
- Step 3: Results Analysis
- Step 4: Optimization Workbench
- Step 5: Scenario Compare

4. Advanced Drawer (collapsed by default)
- Raw payload editor
- Raw API response
- Debug metadata

## Wireframe Structure (desktop)
1. Top row
- App header + action buttons (`Load Sample`, `Validate`, `Run`, `Optimize`)
- Persistent KPI cards

2. Left column (workflow)
- Step cards in vertical order with clear completion state

3. Right column (analysis)
- Result charts, candidate leaderboard, compare panel

4. Bottom area
- Advanced drawer with raw JSON editors/viewers

## Wireframe Structure (mobile)
1. Single-column step sequence
2. Sticky compact KPI ribbon
3. Collapsible tables and chart cards
4. Advanced drawer fully collapsed by default

## Key Interaction Rules
1. Validation gate
- `Run` is disabled until blocking validation errors are resolved.

2. Progressive disclosure
- Advanced controls hidden behind expanders.
- Core operator path remains visible and short.

3. Explainability by default
- Optimization result always shows:
  - objective method
  - top candidates
  - target-vs-actual delta summary

4. Deterministic feedback
- Every user action returns clear status:
  - `idle`, `running`, `success`, `error`

## Component Inventory
1. Status components
- `StatusBadge`
- `ValidationIssueList`
- `RunStatePill`

2. Data components
- `InputSummaryTable`
- `SiloStateTable`
- `CandidateLeaderboardTable`

3. Metric components
- `KpiCard`
- `DeltaBar`
- `SiloContributionBar`

4. Workflow components
- `StepCard`
- `ActionBar`
- `AdvancedDrawer`

5. Feedback components
- `InlineError`
- `EmptyState`
- `LoadingOverlay`

## UX Copy Principles
1. Use domain terms consistently: `discharge`, `remaining`, `target`, `objective`.
2. Prefer action labels over generic labels:
- `Validate Inputs` instead of `Validate`
- `Run Simulation` instead of `Run`
- `Optimize Blend` instead of `Optimize`
3. Every error includes an action hint.

## Success Criteria (M1 Definition of Done)
1. Architecture agreement
- One documented flow from load to compare with no ambiguity.

2. Screen contract
- Each section has defined purpose, required inputs, and outputs.

3. Component contract
- All core UI components listed with intended responsibility.

4. UX metrics baseline defined
- Time to first valid run target: under 3 minutes for first-time operator.
- Validation comprehension target: user can identify first blocking issue in under 10 seconds.
- Optimization comprehension target: user can identify best candidate and reason in under 20 seconds.

## Out of Scope for M1
1. Full visual redesign implementation
2. Chart library integration decisions
3. API schema changes

## M2 Implementation Hand-off Checklist
1. Build step-based layout shell.
2. Add persistent KPI strip.
3. Implement validation issue panel.
4. Implement run result analysis panels.
5. Add optimization leaderboard and compare panel.
6. Add advanced drawer for raw payload/response.
