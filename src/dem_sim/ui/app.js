const payloadEl = document.getElementById("payload");
const rawOutEl = document.getElementById("rawOut");
const optOutEl = document.getElementById("optOut");
const summaryCardsEl = document.getElementById("summaryCards");
const siloTableWrapEl = document.getElementById("siloTableWrap");
const upcomingLotsWrapEl = document.getElementById("upcomingLotsWrap");
const stateLedgerWrapEl = document.getElementById("stateLedgerWrap");
const optimizeBtn = document.getElementById("optimizeBtn");

function printRaw(data) {
  rawOutEl.textContent = typeof data === "string" ? data : JSON.stringify(data, null, 2);
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
      (c) => `<div class="card"><div class="card-key">${c.key}</div><div class="card-value">${c.value}</div></div>`
    )
    .join("");
}

function renderSiloTable(result) {
  const perSilo = result.per_silo || {};
  const rows = Object.entries(perSilo).map(([id, v]) => `
    <tr>
      <td>${id}</td>
      <td>${Number(v.discharged_mass_kg).toFixed(3)}</td>
      <td>${Number(v.mass_flow_rate_kg_s).toFixed(3)}</td>
      <td>${Number(v.discharge_time_s).toFixed(3)}</td>
      <td>${Number(v.sigma_m).toFixed(4)}</td>
    </tr>
  `).join("");

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

async function loadSample() {
  const r = await fetch("/api/sample");
  const payload = await r.json();
  payloadEl.value = JSON.stringify(payload, null, 2);
  renderUpcomingLots(payload);
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
  try {
    const payload = parsePayload();
    const r = await fetch("/api/validate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    printRaw(data);
  } catch (e) {
    printRaw(String(e));
  }
}

async function runSimulation() {
  try {
    const payload = parsePayload();
    const r = await fetch("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    if (!r.ok) {
      printRaw(data);
      return;
    }
    renderSummary(data);
    renderSiloTable(data);
    renderStateLedger(data);
    printRaw(data);
  } catch (e) {
    printRaw(String(e));
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
  try {
    if (!optOutEl) {
      throw new Error("UI element #optOut not found.");
    }
    optOutEl.textContent = "Running optimization...";
    if (optimizeBtn) optimizeBtn.disabled = true;

    const payload = parsePayload();
    payload.target_params = targetParamsFromUI();
    payload.iterations = Number(document.getElementById("opt_iterations").value || 120);
    payload.seed = 42;

    const r = await fetch("/api/optimize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await r.json();
    if (!r.ok) {
      optOutEl.textContent = JSON.stringify(data, null, 2);
      if (optimizeBtn) optimizeBtn.disabled = false;
      return;
    }
    const bestRun = data.best_run || {};
    renderSummary(bestRun);
    renderSiloTable(bestRun);
    renderStateLedger(bestRun);
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
    if (optimizeBtn) optimizeBtn.disabled = false;
  } catch (e) {
    if (optOutEl) optOutEl.textContent = String(e);
    printRaw(`Optimization error: ${String(e)}`);
    if (optimizeBtn) optimizeBtn.disabled = false;
  }
}

document.getElementById("loadSampleBtn").addEventListener("click", loadSample);
document.getElementById("validateBtn").addEventListener("click", validatePayload);
document.getElementById("runBtn").addEventListener("click", runSimulation);
if (optimizeBtn) optimizeBtn.addEventListener("click", optimizeBlend);
printRaw("UI ready. Click Load Sample, then Blend Optimize.");

loadSample();
