const payloadEl = document.getElementById("payload");
const rawOutEl = document.getElementById("rawOut");
const validationOutEl = document.getElementById("validationOut");
const validationSummaryEl = document.getElementById("validationSummary");
const optOutEl = document.getElementById("optOut");
const summaryCardsEl = document.getElementById("summaryCards");
const siloTableWrapEl = document.getElementById("siloTableWrap");
const upcomingLotsWrapEl = document.getElementById("upcomingLotsWrap");
const stateLedgerWrapEl = document.getElementById("stateLedgerWrap");
const remainingFocusWrapEl = document.getElementById("remainingFocusWrap");
const contributionWrapEl = document.getElementById("contributionWrap");
const runStatusEl = document.getElementById("runStatus");
const candidateTableWrapEl = document.getElementById("candidateTableWrap");
const changeSummaryWrapEl = document.getElementById("changeSummaryWrap");
const scenarioCompareWrapEl = document.getElementById("scenarioCompareWrap");
const explainabilityWrapEl = document.getElementById("explainabilityWrap");
const convergenceWrapEl = document.getElementById("convergenceWrap");
const optPresetEl = document.getElementById("opt_preset");
const optSeedEl = document.getElementById("opt_seed");
const candidateSortEl = document.getElementById("candidateSort");
const optimizeBtn = document.getElementById("optimizeBtn");
const runBtn = document.getElementById("runBtn");

const kpiValidationEl = document.getElementById("kpiValidation");
const kpiDischargedEl = document.getElementById("kpiDischarged");
const kpiRemainingEl = document.getElementById("kpiRemaining");
const kpiObjectiveEl = document.getElementById("kpiObjective");

const stepInput = document.getElementById("stepInput");
const stepRun = document.getElementById("stepRun");
const stepResults = document.getElementById("stepResults");
const stepOptimize = document.getElementById("stepOptimize");
const statusInput = document.getElementById("statusInput");
const statusRun = document.getElementById("statusRun");
const statusResults = document.getElementById("statusResults");
const statusOptimize = document.getElementById("statusOptimize");
let lastRunResult = null;
let lastOptimizePayload = null;
let isValidating = false;
let isRunning = false;
let isOptimizing = false;

function printRaw(data) {
  rawOutEl.textContent = typeof data === "string" ? data : JSON.stringify(data, null, 2);
}

function setStepState(stepEl, statusEl, stateClass, label) {
  stepEl.classList.remove("is-success", "is-active", "is-warning");
  if (stateClass) stepEl.classList.add(stateClass);
  statusEl.textContent = label;
}

function classifyIssue(message) {
  const lower = String(message || "").toLowerCase();
  let severity = "error";
  if (lower.includes("duplicate") || lower.includes("unknown") || lower.includes("missing")) {
    severity = "error";
  } else if (lower.includes("must have") || lower.includes("between 0 and 1")) {
    severity = "error";
  } else if (lower.includes("expected 3 silos")) {
    severity = "warn";
  }

  let file = "input";
  if (lower.includes("silos.csv")) file = "silos.csv";
  if (lower.includes("layers.csv")) file = "layers.csv";
  if (lower.includes("suppliers.csv")) file = "suppliers.csv";
  if (lower.includes("discharge.csv")) file = "discharge.csv";

  let field = "general";
  if (lower.includes("silo_id")) field = "silo_id";
  if (lower.includes("layer_index")) field = "layer_index";
  if (lower.includes("segment_mass_kg")) field = "segment_mass_kg";
  if (lower.includes("discharge_fraction")) field = "discharge_fraction";
  if (lower.includes("discharge_mass_kg")) field = "discharge_mass_kg";
  if (lower.includes("capacity_kg")) field = "capacity_kg";

  let hint = "Review this field value and rerun validation.";
  if (lower.includes("missing")) hint = "Add the required column in the referenced CSV file.";
  if (lower.includes("duplicate")) hint = "Remove duplicates and keep unique key rows only.";
  if (lower.includes("between 0 and 1")) hint = "Set value within range 0.0 to 1.0.";
  if (lower.includes("must have")) hint = "Ensure all rows satisfy the numeric constraint.";

  return { severity, file, field, message: String(message), hint };
}

function renderValidationIssues(errors = []) {
  const issues = errors.map(classifyIssue);
  if (!issues.length) {
    validationOutEl.innerHTML = "";
    return { blockingCount: 0, warningCount: 0 };
  }
  validationOutEl.innerHTML = issues
    .map(
      (i) => `
      <article class="issue-card">
        <div class="issue-head">
          <span class="badge ${i.severity}">${i.severity}</span>
          <span class="issue-target">${i.file} / ${i.field}</span>
        </div>
        <div class="issue-message">${i.message}</div>
        <div class="issue-hint">Action: ${i.hint}</div>
      </article>
    `
    )
    .join("");
  const blockingCount = issues.filter((i) => i.severity === "error").length;
  const warningCount = issues.filter((i) => i.severity === "warn").length;
  return { blockingCount, warningCount };
}

function setValidationState(valid, blockingCount = 0, warningCount = 0) {
  validationSummaryEl.classList.remove("ready", "blocked", "warn");
  if (valid) {
    kpiValidationEl.textContent = warningCount > 0 ? `Ready (${warningCount} Warnings)` : "Ready";
    validationSummaryEl.textContent =
      warningCount > 0
        ? `Validation passed with ${warningCount} warning(s). Simulation can run.`
        : "Validation passed. Inputs are ready to run.";
    validationSummaryEl.classList.add(warningCount > 0 ? "warn" : "ready");
    setStepState(stepInput, statusInput, warningCount > 0 ? "is-warning" : "is-success", "Ready");
    runBtn.disabled = false;
  } else {
    kpiValidationEl.textContent = blockingCount > 0 ? `${blockingCount} Blocking` : "Not Ready";
    validationSummaryEl.textContent =
      blockingCount > 0
        ? `${blockingCount} blocking issue(s) must be fixed before simulation.`
        : "Validation not executed.";
    validationSummaryEl.classList.add(blockingCount > 0 ? "blocked" : "warn");
    setStepState(stepInput, statusInput, "is-warning", "Needs Attention");
    runBtn.disabled = true;
  }
}

function updateRunKpis(result) {
  kpiDischargedEl.textContent = Number(result.total_discharged_mass_kg || 0).toFixed(3);
  kpiRemainingEl.textContent = Number(result.total_remaining_mass_kg || 0).toFixed(3);
}

function renderSummary(result) {
  const blend = result.total_blended_params || {};
  const total = Number(result.total_discharged_mass_kg || 0).toFixed(3);
  const remaining = Number(result.total_remaining_mass_kg || 0).toFixed(3);

  const cards = [
    { key: "Total Discharged (kg)", value: total },
    { key: "Total Remaining (kg)", value: remaining },
    ...Object.entries(blend).map(([k, v]) => ({ key: k, value: Number(v).toFixed(4) })),
  ];
  summaryCardsEl.innerHTML = cards
    .map(
      (c) =>
        `<div class="card"><div class="card-key">${c.key}</div><div class="card-value">${c.value}</div></div>`
    )
    .join("");
}

function renderSiloTable(result) {
  const perSilo = result.per_silo || {};
  const rows = Object.entries(perSilo)
    .map(
      ([id, v]) => `
    <tr>
      <td>${id}</td>
      <td>${Number(v.discharged_mass_kg).toFixed(3)}</td>
      <td>${Number(v.mass_flow_rate_kg_s).toFixed(3)}</td>
      <td>${Number(v.discharge_time_s).toFixed(3)}</td>
      <td>${Number(v.sigma_m).toFixed(4)}</td>
    </tr>
  `
    )
    .join("");

  siloTableWrapEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Silo</th>
          <th>Discharged (kg)</th>
          <th>Flow (kg/s)</th>
          <th>Time (s)</th>
          <th>Sigma (m)</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderUpcomingLots(payload) {
  const layers = payload.layers || [];
  const map = new Map();
  layers.forEach((r) => {
    const key = `${r.lot_id}__${r.supplier}`;
    const prev = map.get(key) || 0;
    map.set(key, prev + Number(r.segment_mass_kg || 0));
  });
  const rows = Array.from(map.entries())
    .map(([k, mass]) => {
      const [lotId, supplier] = k.split("__");
      return `<tr><td>${lotId}</td><td>${supplier}</td><td>${mass.toFixed(3)}</td></tr>`;
    })
    .join("");
  upcomingLotsWrapEl.innerHTML = `
    <table>
      <thead><tr><th>Lot</th><th>Supplier</th><th>Total Planned (kg)</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderStateLedger(result) {
  const rows = (result.silo_state_ledger || [])
    .map(
      (r) => `<tr>
        <td>${r.silo_id}</td>
        <td>${Number(r.initial_mass_kg).toFixed(3)}</td>
        <td>${Number(r.discharged_mass_kg).toFixed(3)}</td>
        <td>${Number(r.remaining_mass_kg).toFixed(3)}</td>
        <td>${Number(r.remaining_pct).toFixed(2)}</td>
      </tr>`
    )
    .join("");
  stateLedgerWrapEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Silo</th><th>Initial (kg)</th><th>Discharged (kg)</th><th>Remaining (kg)</th><th>Remaining (%)</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderRemainingFocus(result) {
  const rows = result.silo_state_ledger || [];
  if (!rows.length) {
    remainingFocusWrapEl.innerHTML = "";
    return;
  }
  remainingFocusWrapEl.innerHTML = rows
    .map((r) => {
      const pct = Number(r.remaining_pct || 0);
      const pctSafe = Math.max(0, Math.min(100, pct));
      return `
      <article class="gauge-card">
        <div class="gauge-top">
          <span class="gauge-id">${r.silo_id}</span>
          <span class="gauge-pct">${pctSafe.toFixed(1)}%</span>
        </div>
        <div class="gauge-track"><div class="gauge-fill" style="width:${pctSafe.toFixed(2)}%"></div></div>
        <div class="gauge-meta">${Number(r.remaining_mass_kg || 0).toFixed(3)} kg remaining</div>
      </article>
    `;
    })
    .join("");
}

function renderContributionBars(result) {
  const perSilo = result.per_silo || {};
  const entries = Object.entries(perSilo);
  const total = entries.reduce((acc, [, v]) => acc + Number(v.discharged_mass_kg || 0), 0);
  if (!entries.length || total <= 0) {
    contributionWrapEl.innerHTML = "";
    return;
  }
  contributionWrapEl.innerHTML = entries
    .map(([id, v]) => {
      const mass = Number(v.discharged_mass_kg || 0);
      const pct = (mass / total) * 100;
      return `
      <div class="bar-row">
        <div class="bar-label">${id}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${pct.toFixed(2)}%"></div></div>
        <div class="bar-value">${pct.toFixed(1)}%</div>
      </div>
    `;
    })
    .join("");
}

function renderCandidateTable(payload) {
  const candidates = (payload?.top_candidates || []).slice();
  const sortKey = candidateSortEl?.value || "score";
  if (sortKey === "discharged") {
    candidates.sort(
      (a, b) => Number(b.total_discharged_mass_kg || 0) - Number(a.total_discharged_mass_kg || 0)
    );
  } else {
    candidates.sort((a, b) => Number(a.objective_score || 0) - Number(b.objective_score || 0));
  }

  if (!candidates.length) {
    candidateTableWrapEl.innerHTML = "";
    return;
  }
  const rows = candidates
    .map(
      (c, idx) => `
    <tr>
      <td>${idx + 1}</td>
      <td>${Number(c.objective_score).toFixed(6)}</td>
      <td>${Number(c.total_discharged_mass_kg).toFixed(3)}</td>
      <td>${(c.recommended_discharge || []).map((r) => `${r.silo_id}:${Number(r.discharge_fraction).toFixed(3)}`).join(" | ")}</td>
    </tr>
  `
    )
    .join("");
  candidateTableWrapEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Score</th>
          <th>Discharged (kg)</th>
          <th>Recommended Fractions</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderChangeSummary(baselineRun, optimizePayload) {
  if (!baselineRun || !optimizePayload?.recommended_discharge) {
    changeSummaryWrapEl.innerHTML = "Run simulation first, then optimize to view changes.";
    return;
  }
  const baseMap = {};
  for (const [id, v] of Object.entries(baselineRun.per_silo || {})) {
    baseMap[id] = Number(v.discharged_mass_kg || 0);
  }
  const rows = optimizePayload.recommended_discharge.map((r) => {
    const baseMass = Number(baseMap[r.silo_id] || 0);
    const frac = Number(r.discharge_fraction || 0);
    const changedText = `baseline discharged ${baseMass.toFixed(3)}kg -> target fraction ${(frac * 100).toFixed(1)}%`;
    return `<div class="change-line"><span class="change-key">${r.silo_id}</span><span class="change-value">${changedText}</span></div>`;
  });
  rows.push(
    `<div class="change-line"><span class="change-key">Objective</span><span class="change-value">${Number(
      optimizePayload.objective_score || 0
    ).toFixed(6)}</span></div>`
  );
  changeSummaryWrapEl.innerHTML = rows.join("");
}

function renderScenarioCompare(baselineRun, optimizedRun) {
  if (!baselineRun || !optimizedRun) {
    scenarioCompareWrapEl.innerHTML = "Run simulation and optimization to compare scenarios.";
    return;
  }
  const base = baselineRun.total_blended_params || {};
  const opt = optimizedRun.total_blended_params || {};
  const keys = Array.from(new Set([...Object.keys(base), ...Object.keys(opt)])).sort();

  const baseLines = [];
  const optLines = [];
  keys.forEach((k) => {
    const b = Number(base[k] || 0);
    const o = Number(opt[k] || 0);
    const d = o - b;
    const dClass = d >= 0 ? "compare-delta-pos" : "compare-delta-neg";
    baseLines.push(
      `<div class="compare-line"><span class="compare-k">${k}</span><span class="compare-v">${b.toFixed(4)}</span></div>`
    );
    optLines.push(
      `<div class="compare-line"><span class="compare-k">${k}</span><span class="compare-v">${o.toFixed(4)} <span class="${dClass}">(${d >= 0 ? "+" : ""}${d.toFixed(4)})</span></span></div>`
    );
  });

  scenarioCompareWrapEl.innerHTML = `
    <div>
      <div class="compare-col-title">Baseline Run</div>
      ${baseLines.join("")}
    </div>
    <div>
      <div class="compare-col-title">Optimized Run</div>
      ${optLines.join("")}
    </div>
  `;
}

function renderExplainability(optimizePayload) {
  if (!optimizePayload) {
    explainabilityWrapEl.innerHTML = "Optimization explainability details will appear after optimize.";
    return;
  }
  const ranges = optimizePayload.param_ranges || {};
  const lines = [
    `<div class="explain-line"><span>Objective Method</span><span>${optimizePayload.objective_method || "n/a"}</span></div>`,
    `<div class="explain-line"><span>Iterations</span><span>${Number(optimizePayload.iterations || 0)}</span></div>`,
    `<div class="explain-line"><span>Score</span><span>${Number(optimizePayload.objective_score || 0).toFixed(6)}</span></div>`,
  ];
  Object.entries(ranges).forEach(([k, v]) => {
    lines.push(
      `<div class="explain-line"><span>Scale: ${k}</span><span>${Number(v).toFixed(4)}</span></div>`
    );
  });
  explainabilityWrapEl.innerHTML = lines.join("");
}

function renderConvergence(optimizePayload) {
  const top = optimizePayload?.top_candidates || [];
  if (!top.length) {
    convergenceWrapEl.innerHTML = "Convergence snapshot will appear after optimize.";
    return;
  }
  const scores = top.map((c) => Number(c.objective_score || 0)).filter(Number.isFinite);
  if (!scores.length) {
    convergenceWrapEl.innerHTML = "No convergence data available.";
    return;
  }
  const best = Math.min(...scores);
  const worst = Math.max(...scores);
  const mean = scores.reduce((a, b) => a + b, 0) / scores.length;
  convergenceWrapEl.innerHTML = `
    <div class="convergence-line"><span>Top Candidates Count</span><span>${scores.length}</span></div>
    <div class="convergence-line"><span>Best Score</span><span>${best.toFixed(6)}</span></div>
    <div class="convergence-line"><span>Worst Score</span><span>${worst.toFixed(6)}</span></div>
    <div class="convergence-line"><span>Mean Score</span><span>${mean.toFixed(6)}</span></div>
    <div class="convergence-line"><span>Spread</span><span>${(worst - best).toFixed(6)}</span></div>
  `;
}

async function loadSample() {
  setStepState(stepInput, statusInput, "is-active", "Loading");
  const r = await fetch("/api/sample");
  const payload = await r.json();
  payloadEl.value = JSON.stringify(payload, null, 2);
  renderUpcomingLots(payload);
  candidateTableWrapEl.innerHTML = "";
  changeSummaryWrapEl.innerHTML = "Run simulation first, then optimize to view changes.";
  scenarioCompareWrapEl.innerHTML = "Run simulation and optimization to compare scenarios.";
  explainabilityWrapEl.innerHTML = "Optimization explainability details will appear after optimize.";
  convergenceWrapEl.innerHTML = "Convergence snapshot will appear after optimize.";
  lastRunResult = null;
  lastOptimizePayload = null;
  validationOutEl.innerHTML = "";
  validationSummaryEl.textContent = "Sample loaded. Run validation to check readiness.";
  setValidationState(false, 0, 0);
  setStepState(stepInput, statusInput, "is-active", "Loaded");
  printRaw("Sample loaded.");
}

function parsePayload() {
  try {
    return JSON.parse(payloadEl.value);
  } catch (e) {
    throw new Error(`Invalid JSON: ${e.message}`);
  }
}

async function validatePayload() {
  if (isValidating) return;
  try {
    isValidating = true;
    document.getElementById("validateBtn").setAttribute("aria-busy", "true");
    setStepState(stepInput, statusInput, "is-active", "Validating");
    const payload = parsePayload();
    const r = await fetch("/api/validate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    if (data.valid) {
      const counts = renderValidationIssues(data.errors || []);
      setValidationState(true, counts.blockingCount, counts.warningCount);
    } else {
      const counts = renderValidationIssues(data.errors || []);
      setValidationState(false, counts.blockingCount, counts.warningCount);
    }
    printRaw(data);
  } catch (e) {
    renderValidationIssues([String(e)]);
    setValidationState(false, 1, 0);
    printRaw(String(e));
  } finally {
    isValidating = false;
    document.getElementById("validateBtn").removeAttribute("aria-busy");
  }
}

async function runSimulation() {
  if (isRunning) return;
  try {
    isRunning = true;
    runBtn.disabled = true;
    runBtn.textContent = "Running...";
    runBtn.setAttribute("aria-busy", "true");
    stepResults.setAttribute("aria-busy", "true");
    setStepState(stepRun, statusRun, "is-active", "Running");
    runStatusEl.className = "run-status running";
    runStatusEl.textContent = "Simulation is running...";
    const payload = parsePayload();
    const r = await fetch("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    if (!r.ok) {
      setStepState(stepRun, statusRun, "is-warning", "Run Failed");
      runStatusEl.className = "run-status error";
      runStatusEl.textContent = "Simulation failed. See response details for diagnostics.";
      printRaw(data);
      return;
    }
    updateRunKpis(data);
    renderSummary(data);
    renderRemainingFocus(data);
    renderContributionBars(data);
    renderSiloTable(data);
    renderStateLedger(data);
    lastRunResult = data;
    setStepState(stepRun, statusRun, "is-success", "Complete");
    setStepState(stepResults, statusResults, "is-success", "Ready");
    runStatusEl.className = "run-status success";
    runStatusEl.textContent = "Simulation complete. Review remaining mass and contribution visuals.";
    printRaw(data);
  } catch (e) {
    setStepState(stepRun, statusRun, "is-warning", "Run Failed");
    runStatusEl.className = "run-status error";
    runStatusEl.textContent = "Simulation failed before completion.";
    printRaw(String(e));
  } finally {
    isRunning = false;
    runBtn.removeAttribute("aria-busy");
    stepResults.removeAttribute("aria-busy");
    runBtn.textContent = "Run Simulation";
    runBtn.disabled = !kpiValidationEl.textContent.startsWith("Ready");
  }
}

function targetParamsFromUI() {
  return {
    moisture_pct: Number(document.getElementById("t_moisture_pct").value),
    fine_extract_db_pct: Number(document.getElementById("t_fine_extract_db_pct").value),
    wort_pH: Number(document.getElementById("t_wort_pH").value),
    diastatic_power_WK: Number(document.getElementById("t_diastatic_power_WK").value),
    total_protein_pct: Number(document.getElementById("t_total_protein_pct").value),
    wort_colour_EBC: Number(document.getElementById("t_wort_colour_EBC").value),
  };
}

async function optimizeBlend() {
  if (isOptimizing) return;
  try {
    if (!optOutEl) {
      throw new Error("UI element #optOut not found.");
    }
    setStepState(stepOptimize, statusOptimize, "is-active", "Optimizing");
    optOutEl.textContent = "Running optimization...";
    if (optimizeBtn) {
      isOptimizing = true;
      optimizeBtn.disabled = true;
      optimizeBtn.textContent = "Optimizing...";
      optimizeBtn.setAttribute("aria-busy", "true");
    }

    const payload = parsePayload();
    payload.target_params = targetParamsFromUI();
    payload.iterations = Number(document.getElementById("opt_iterations").value || 120);
    payload.seed = Number(optSeedEl?.value || 42);

    const r = await fetch("/api/optimize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    if (!r.ok) {
      optOutEl.textContent = JSON.stringify(data, null, 2);
      setStepState(stepOptimize, statusOptimize, "is-warning", "Failed");
      if (optimizeBtn) optimizeBtn.disabled = false;
      return;
    }
    const bestRun = data.best_run || {};
    updateRunKpis(bestRun);
    renderSummary(bestRun);
    renderRemainingFocus(bestRun);
    renderContributionBars(bestRun);
    renderSiloTable(bestRun);
    renderStateLedger(bestRun);
    renderCandidateTable(data);
    renderChangeSummary(lastRunResult, data);
    renderScenarioCompare(lastRunResult, bestRun);
    renderExplainability(data);
    renderConvergence(data);
    lastOptimizePayload = data;

    const score = Number(data.objective_score);
    kpiObjectiveEl.textContent = Number.isFinite(score) ? score.toFixed(6) : "N/A";

    const top = data.top_candidates || [];
    const lines = [];
    lines.push(`Method: ${data.objective_method}`);
    lines.push(`Best score: ${Number(data.objective_score).toFixed(6)}`);
    lines.push("Top candidates:");
    top.forEach((c, i) => {
      lines.push(
        `${i + 1}. score=${Number(c.objective_score).toFixed(6)} discharged=${Number(c.total_discharged_mass_kg).toFixed(3)}kg`
      );
    });
    lines.push("");
    lines.push(JSON.stringify(data, null, 2));
    optOutEl.textContent = lines.join("\n");
    setStepState(stepOptimize, statusOptimize, "is-success", "Complete");
    if (optimizeBtn) optimizeBtn.disabled = false;
  } catch (e) {
    if (optOutEl) optOutEl.textContent = String(e);
    setStepState(stepOptimize, statusOptimize, "is-warning", "Failed");
    printRaw(`Optimization error: ${String(e)}`);
    if (optimizeBtn) optimizeBtn.disabled = false;
  } finally {
    isOptimizing = false;
    if (optimizeBtn) {
      optimizeBtn.removeAttribute("aria-busy");
      optimizeBtn.textContent = "Optimize Blend";
    }
  }
}

document.getElementById("loadSampleBtn").addEventListener("click", loadSample);
document.getElementById("validateBtn").addEventListener("click", validatePayload);
document.getElementById("runBtn").addEventListener("click", runSimulation);
if (optimizeBtn) optimizeBtn.addEventListener("click", optimizeBlend);
if (optPresetEl) {
  optPresetEl.addEventListener("change", () => {
    const mode = optPresetEl.value;
    const iterationsEl = document.getElementById("opt_iterations");
    if (!iterationsEl) return;
    if (mode === "fast") iterationsEl.value = "40";
    if (mode === "balanced") iterationsEl.value = "120";
    if (mode === "aggressive") iterationsEl.value = "260";
  });
}
if (candidateSortEl) {
  candidateSortEl.addEventListener("change", () => {
    if (lastOptimizePayload) renderCandidateTable(lastOptimizePayload);
  });
}

document.addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "enter") {
    e.preventDefault();
    if (!isRunning && !runBtn.disabled) runSimulation();
  }
  if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key.toLowerCase() === "o") {
    e.preventDefault();
    if (!isOptimizing && optimizeBtn && !optimizeBtn.disabled) optimizeBlend();
  }
});

setValidationState(false, 0, 0);
setStepState(stepRun, statusRun, "", "Pending");
setStepState(stepResults, statusResults, "", "Pending");
setStepState(stepOptimize, statusOptimize, "", "Pending");
runStatusEl.className = "run-status";
runStatusEl.textContent = "Simulation not started.";
changeSummaryWrapEl.innerHTML = "Run simulation first, then optimize to view changes.";
scenarioCompareWrapEl.innerHTML = "Run simulation and optimization to compare scenarios.";
explainabilityWrapEl.innerHTML = "Optimization explainability details will appear after optimize.";
convergenceWrapEl.innerHTML = "Convergence snapshot will appear after optimize.";
kpiObjectiveEl.textContent = "N/A";
printRaw("UI ready. Load sample, validate inputs, run, then optimize.");

loadSample();
