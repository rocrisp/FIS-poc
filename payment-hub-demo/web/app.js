(function () {
  const hero = document.getElementById("hero");
  const clusterEl = document.getElementById("cluster");
  const roleLine = document.getElementById("roleLine");
  const sub = document.getElementById("sub");
  const formHint = document.getElementById("formHint");
  const formMsg = document.getElementById("formMsg");
  const submitBtn = document.getElementById("submitBtn");
  const eventsEl = document.getElementById("events");
  const balancesBody = document.querySelector("#balances tbody");
  const balHint = document.getElementById("balHint");
  const form = document.getElementById("payForm");
  const trafficLog = document.getElementById("trafficLog");
  const trafficLive = document.getElementById("trafficLive");
  let lastTrafficSig = "";

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function shortTS(ts) {
    if (!ts) return "";
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
      empty.textContent = "Waiting for arbitrator polls…";
      trafficLog.appendChild(empty);
      return;
    }
    list.slice(0, 100).forEach((e) => {
      const row = document.createElement("div");
      row.className = "log-line";
      const dir = e.dir === "out" ? "OUT" : "IN";
      const dirClass = e.dir === "out" ? "dir-out" : (String(e.summary || "").indexOf("ERROR") >= 0 ? "dir-in err" : "dir-in");
      row.innerHTML =
        `<span class="ts">${escapeHtml(shortTS(e.ts))}</span>` +
        `<span class="${dirClass}">${dir}</span>` +
        `<span class="summary">${escapeHtml(e.summary || "")}</span>`;
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

  function applyHealth(h) {
    const role = (h.role || "unknown").toLowerCase();
    hero.className = "hero " + (role === "active" || role === "standby" || role === "unreachable" ? role : "");
    clusterEl.textContent = h.cluster || "—";
    roleLine.textContent = (h.role || "unknown").toUpperCase();
    const bits = [
      "acceptTraffic=" + !!h.acceptTraffic,
      h.reason ? "reason=" + h.reason : null,
      h.partitioned ? "partitioned" : null,
      h.activeSite ? "active=" + h.activeSite : null,
    ].filter(Boolean);
    sub.textContent = bits.join(" · ");
    submitBtn.disabled = !h.acceptTraffic;
    formHint.textContent = h.acceptTraffic
      ? "This site is active — payments will be accepted and published to Kafka."
      : "This site is not active — submissions are refused (standby / unreachable).";
  }

  function renderBalances(body) {
    const rows = (body && body.balances) || [];
    const cur = (body && body.currency) || "USD";
    const start = body && body.startingBalance != null ? body.startingBalance : 1000;
    balHint.textContent =
      "Starting " + start + " " + cur + " per user; updated from validated Kafka lifecycle events.";
    balancesBody.innerHTML = "";
    if (!rows.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = '<td class="empty" colspan="2">No balances yet — submit a payment.</td>';
      balancesBody.appendChild(tr);
      return;
    }
    rows.forEach((r) => {
      const tr = document.createElement("tr");
      const amt = Number(r.balance);
      const cls = amt < 0 ? "amt neg" : "amt pos";
      tr.innerHTML =
        "<td>" + escapeHtml(r.user) + "</td>" +
        '<td class="' + cls + '">' +
        escapeHtml(r.currency || cur) + " " +
        amt.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) +
        "</td>";
      balancesBody.appendChild(tr);
    });
  }

  function renderEvents(items) {
    eventsEl.innerHTML = "";
    (items || []).forEach((ev) => {
      const li = document.createElement("li");
      const status = ev.status || "accepted";
      li.className = status;
      const origin = ev.origin || "local";
      const tagClass = origin === "replicated" ? "replicated" : origin === "ui" ? "ui" : "local";
      li.innerHTML =
        `<span class="tag ${tagClass}">${escapeHtml(origin)}</span>` +
        escapeHtml(shortTS(ev.ts) || ev.ts || "") +
        "  " +
        escapeHtml(status) +
        "  " +
        escapeHtml(ev.paymentId || "") +
        "  " +
        escapeHtml(ev.detail || "") +
        (ev.source ? "  src=" + escapeHtml(ev.source) : "") +
        (ev.topic ? "  [" + escapeHtml(ev.topic) + "]" : "");
      eventsEl.appendChild(li);
    });
    if (!items || !items.length) {
      const li = document.createElement("li");
      li.textContent = "No transactions yet — submit on the active site.";
      eventsEl.appendChild(li);
    }
  }

  async function refresh() {
    const [healthRes, eventsRes, balRes] = await Promise.all([
      fetch("/health", { cache: "no-store" }),
      fetch("/api/v1/events", { cache: "no-store" }),
      fetch("/api/v1/balances", { cache: "no-store" }),
    ]);
    if (healthRes.ok) applyHealth(await healthRes.json());
    if (eventsRes.ok) {
      const body = await eventsRes.json();
      renderEvents(body.events || []);
    }
    if (balRes.ok) renderBalances(await balRes.json());
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    formMsg.className = "note";
    formMsg.textContent = "Submitting…";
    const fd = new FormData(form);
    const payload = {
      amount: Number(fd.get("amount")),
      currency: String(fd.get("currency") || "USD"),
      from: String(fd.get("from") || ""),
      to: String(fd.get("to") || ""),
    };
    try {
      const res = await fetch("/api/v1/payments", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const body = await res.json();
      if (!res.ok) {
        formMsg.className = "note err";
        formMsg.textContent = body.message || body.error || "refused";
      } else {
        formMsg.className = "note ok";
        formMsg.textContent = "Accepted " + (body.paymentId || "");
      }
      await refresh();
    } catch (err) {
      formMsg.className = "note err";
      formMsg.textContent = String(err);
    }
  });

  refresh().catch((err) => {
    sub.textContent = "Failed to load status: " + err;
  });
  refreshTraffic();
  setInterval(() => {
    refresh().catch(() => {});
  }, 3000);
  setInterval(refreshTraffic, 1000);
})();
