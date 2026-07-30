(function () {
  const sitesEl = document.getElementById("sites");
  const factsEl = document.getElementById("facts");
  const simBadge = document.getElementById("simBadge");
  const simNote = document.getElementById("simNote");
  const rawEl = document.getElementById("raw");
  const trafficLog = document.getElementById("trafficLog");
  const trafficLive = document.getElementById("trafficLive");
  let lastTrafficSig = "";

  function roleClass(role) {
    if (role === "active" || role === "standby" || role === "unreachable") return role;
    return "unknown";
  }

  function render(overview) {
    const sim = overview.simulation || {};
    const mode = sim.mode || "none";
    simBadge.textContent =
      mode === "none"
        ? "mode: live"
        : mode === "unreachable"
          ? `mode: unreachable (${sim.target || "?"})`
          : mode === "partition"
            ? "mode: Submariner mesh down"
            : `mode: ${mode}`;
    simNote.textContent = sim.note || "";

    sitesEl.innerHTML = "";
    (overview.sites || []).forEach((s) => {
      const card = document.createElement("article");
      card.className = "site";
      card.innerHTML =
        `<div class="site-name">${escapeHtml(s.name || "?")}</div>` +
        `<span class="role ${roleClass(s.role)}">${escapeHtml(s.role || "unknown")}</span>` +
        `<div class="site-reason">acceptTraffic=${s.acceptTraffic} · ${escapeHtml(s.reason || "")}` +
        (s.simulated ? " · simulated" : "") +
        `</div>`;
      sitesEl.appendChild(card);
    });

    const reach = overview.observedReachability || {};
    const reachBits = Object.keys(reach)
      .sort()
      .map((k) => `${k}=${reach[k] ? "ok" : "down"}`)
      .join(", ");
    factsEl.innerHTML =
      `<dt>Observed Submariner</dt><dd>${overview.observedSubmarinerConnected ? "connected" : "disconnected"}</dd>` +
      `<dt>Effective Submariner</dt><dd>${overview.effectiveSubmarinerConnected ? "connected" : "disconnected"}</dd>` +
      `<dt>Observed reachability</dt><dd>${escapeHtml(reachBits || "—")}</dd>` +
      `<dt>Priority</dt><dd>${escapeHtml((overview.priority || []).join(" → "))}</dd>`;

    rawEl.hidden = false;
    rawEl.textContent = JSON.stringify(overview, null, 2);
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  async function refresh() {
    const res = await fetch("/api/v1/overview", { cache: "no-store" });
    if (!res.ok) throw new Error("overview " + res.status);
    render(await res.json());
  }

  async function setSimulation(mode, target) {
    const buttons = document.querySelectorAll("button[data-mode]");
    buttons.forEach((b) => (b.disabled = true));
    try {
      const res = await fetch("/api/v1/simulation", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode, target: target || "" }),
      });
      const body = await res.json();
      if (!res.ok) {
        simNote.textContent = body.message || body.error || "simulation failed";
        return;
      }
      render(body);
    } catch (e) {
      simNote.textContent = String(e);
    } finally {
      buttons.forEach((b) => (b.disabled = false));
    }
  }

  document.querySelectorAll("button[data-mode]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setSimulation(btn.dataset.mode, btn.dataset.target || "");
    });
  });

  function shortTS(ts) {
    if (!ts) return "";
    // Prefer HH:MM:SS.mmm from RFC3339Nano
    const m = String(ts).match(/T(\d{2}:\d{2}:\d{2})(?:\.(\d{1,3}))?/);
    if (!m) return ts;
    return m[2] ? m[1] + "." + m[2].padEnd(3, "0") : m[1];
  }

  function renderTraffic(entries) {
    const list = entries || [];
    const sig = list.length ? list[0].ts + "|" + list.length : "empty";
    if (sig === lastTrafficSig) return;
    lastTrafficSig = sig;
    trafficLog.innerHTML = "";
    if (!list.length) {
      const empty = document.createElement("div");
      empty.className = "log-line empty";
      empty.textContent = "Waiting for site polls…";
      trafficLog.appendChild(empty);
      return;
    }
    list.slice(0, 100).forEach((e) => {
      const row = document.createElement("div");
      row.className = "log-line";
      row.innerHTML =
        `<span class="ts">${escapeHtml(shortTS(e.ts))}</span>` +
        `<span class="method">${escapeHtml(e.method || "")}</span>` +
        `<span class="cluster">${escapeHtml(e.cluster || "—")}</span>` +
        `<span class="summary">${escapeHtml(e.path || "")} ${escapeHtml(e.summary || "")}</span>`;
      trafficLog.appendChild(row);
    });
  }

  async function refreshTraffic() {
    try {
      const res = await fetch("/api/v1/traffic", { cache: "no-store" });
      if (!res.ok) throw new Error(String(res.status));
      const body = await res.json();
      renderTraffic(body.entries || []);
      trafficLive.textContent = "live";
      trafficLive.classList.remove("err");
    } catch (e) {
      trafficLive.textContent = "error";
      trafficLive.classList.add("err");
    }
  }

  refresh().catch((e) => {
    simNote.textContent = "Failed to load overview: " + e;
  });
  refreshTraffic();
  setInterval(() => {
    refresh().catch(() => {});
  }, 3000);
  setInterval(refreshTraffic, 1000);
})();
