const casesEl = document.getElementById("cases");
const endpointList = document.getElementById("endpointList");
const runBtn = document.getElementById("runBtn");
const summary = document.getElementById("summary");

let cases = [];

async function loadCases() {
  const res = await fetch("/api/v1/cases");
  const data = await res.json();
  cases = data.cases || [];
  renderEndpoints(data.endpoints || {});
  renderCases(cases.map((c) => ({ ...c, status: "pending" })));
}

function renderEndpoints(ep) {
  const rows = [
    ["Arbitrator", ep.arbitrator],
    ["Active hub", ep.activeHub],
    ["Standby hub", ep.standbyHub],
  ];
  endpointList.innerHTML = rows
    .map(([k, v]) => `<dt>${k}</dt><dd><code>${escapeHtml(v || "—")}</code></dd>`)
    .join("");
}

function renderCases(items) {
  let html = "";
  let lastGroup = "";
  items.forEach((item, i) => {
    if (item.group !== lastGroup) {
      lastGroup = item.group;
      html += `<div class="group-label">${escapeHtml(lastGroup)}</div>`;
    }
    const status = item.status || (item.pass === true ? "pass" : item.pass === false ? "fail" : "pending");
    const badge = status === "pass" ? "PASS" : status === "fail" ? "FAIL" : "QUEUED";
    html += `
      <article class="card ${status}" style="animation-delay:${i * 40}ms">
        <div class="card-top">
          <h3>${escapeHtml(item.name)}</h3>
          <span class="badge ${status}">${badge}</span>
        </div>
        <p class="desc">${escapeHtml(item.description || "")}</p>
        <div class="meta">
          <div><strong>expect</strong><code>${escapeHtml(item.expect || "")}</code></div>
          ${item.got ? `<div><strong>got</strong><code>${escapeHtml(item.got)}</code></div>` : ""}
          ${item.duration ? `<div><strong>duration</strong><code>${escapeHtml(item.duration)}</code></div>` : ""}
          ${item.detail ? `<div><strong>body</strong><code class="detail">${escapeHtml(item.detail)}</code></div>` : ""}
        </div>
      </article>`;
  });
  casesEl.innerHTML = html;
}

async function runAll() {
  runBtn.disabled = true;
  summary.className = "summary idle";
  summary.textContent = "Running…";
  renderCases(cases.map((c) => ({ ...c, status: "pending" })));

  try {
    const res = await fetch("/api/v1/run", { method: "POST" });
    const data = await res.json();
    const results = (data.results || []).map((r) => ({
      ...r,
      description: (cases.find((c) => c.id === r.id) || {}).description || "",
      status: r.pass ? "pass" : "fail",
    }));
    renderCases(results);
    const failed = data.failed || 0;
    if (failed === 0) {
      summary.className = "summary pass";
      summary.textContent = `${data.passed}/${data.passed + data.failed} passed`;
    } else {
      summary.className = "summary fail";
      summary.textContent = `${data.failed} failed · ${data.passed} passed`;
    }
  } catch (err) {
    summary.className = "summary fail";
    summary.textContent = "Run failed";
    console.error(err);
  } finally {
    runBtn.disabled = false;
  }
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

runBtn.addEventListener("click", runAll);
loadCases();
