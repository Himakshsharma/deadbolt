/* Deadbolt dashboard.
 *
 * Data flow
 *   Server pushes a tiny "live" frame every 100 ms (cumulative counters) and a
 *   "full" snapshot every 1 s (history for charts, race results, product rows).
 *   Messages only update `model`; nothing touches the DOM in the socket handler.
 *   One requestAnimationFrame loop applies the model to the DOM at most every
 *   100 ms and eases the big numbers toward their targets between applies.
 *   Charts redraw once per second, when new history arrives.
 */
(() => {
  "use strict";

  const STRATEGIES = ["naive", "pessimistic", "optimistic", "batched"];
  const LABEL = { naive: "Naïve", pessimistic: "Lock First", optimistic: "Optimistic", batched: "Micro-Batched", mixed: "Mixed" };
  const DOT = { naive: "#f43f5e", pessimistic: "#e2e8f0", optimistic: "#94a3b8", batched: "#10b981" };
  const NOTE = {
    naive: "<b>Naïve.</b> Reads the stock, checks it, then writes it, with no row locks and no retries. Two buyers can both see the last unit, and carts lock rows in whatever order they list them, so transactions deadlock.",
    pessimistic: "<b>Lock First.</b> <code>SELECT … FOR UPDATE</code> on every product in the cart, always in ascending id order, before writing. No overselling and no deadlock cycles, but every buyer queues behind the hottest row.",
    optimistic: "<b>Optimistic.</b> Writes only if the row's version is unchanged since it was read; otherwise backs off and retries. Cheap when conflicts are rare, wasteful when everyone wants the same row.",
    batched: "<b>Micro-Batched.</b> Buffers orders for up to 50 ms, then commits up to 50 of them in one transaction: lock once in id order, allocate in arrival order, write everything with a single <code>UNNEST</code> statement.",
  };
  const RENDER_INTERVAL_MS = 100;

  const $ = (id) => document.getElementById(id);
  const nf0 = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
  const nf1 = new Intl.NumberFormat(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const fmt = (n) => nf0.format(Math.round(Number(n) || 0));

  // ------------------------------------------------------------------ model
  const model = {
    connected: false,
    live: null,            // latest live-shaped frame (also derived from full snapshots)
    full: null,            // latest full snapshot
    frames: [],            // [{t, req, commits}] for client-side rates over the last ~1.2 s
    peak: 0,
    selected: "batched",
    pendingSelectUntil: 0, // ignore server strategy echo briefly after a click
    dirty: false,
    chartsDirty: false,
  };

  // Turn a full snapshot into the same compact shape as a live frame.
  function liveFromFull(full) {
    const s = {};
    for (const k of STRATEGIES) {
      const st = full.metrics.strategies[k];
      s[k] = [st.requests, st.confirmed, st.db_commits, st.deadlocks_hit, st.dropped];
    }
    const integ = full.integrity || {};
    const run = full.runner || {};
    return {
      ts: full.ts,
      active_strategy: full.active_strategy,
      s,
      run: { state: run.state || "idle", completed: run.completed || 0, target: run.target || 0 },
      q: full.metrics.batch.queue_depth,
      integrity: { ok: integ.ok !== false, oversold_units: integ.oversold_units || 0, negative_rows: integ.negative_rows || 0 },
    };
  }

  function ingest(msg) {
    let live = msg;
    if (msg.type === "full") {
      model.full = msg;
      model.chartsDirty = true;
      live = liveFromFull(msg);
      applyLimits(msg.settings);
    }
    // detect a reset (counters went backwards) and drop stale rate frames
    const req = STRATEGIES.reduce((a, k) => a + live.s[k][0], 0);
    const commits = STRATEGIES.reduce((a, k) => a + live.s[k][2], 0);
    const last = model.frames[model.frames.length - 1];
    if (last && req < last.req) { model.frames = []; model.peak = 0; }
    model.frames.push({ t: live.ts, req, commits });
    while (model.frames.length > 2 && live.ts - model.frames[1].t >= 1.2) model.frames.shift();
    model.live = live;
    model.dirty = true;
  }

  // Keep the sliders inside whatever limits this deployment enforces.
  let limitsApplied = false;
  let paramsTouched = false;
  function applyLimits(settings) {
    if (limitsApplied || !settings || !settings.load_max_users) return;
    limitsApplied = true;
    const clamp = (el, max) => {
      if (Number(el.max) > max) el.max = String(max);
      if (Number(el.value) > max) el.value = String(max);
    };
    clamp($("users"), settings.load_max_users);
    clamp($("requests"), settings.load_max_requests);
    // Start from this deployment's suggested workload unless the visitor already moved a slider.
    if (!paramsTouched) {
      if (settings.load_default_users) $("users").value = String(Math.min(settings.load_default_users, settings.load_max_users));
      if (settings.load_default_requests) $("requests").value = String(Math.min(settings.load_default_requests, settings.load_max_requests));
    }
    $("users").dispatchEvent(new Event("input"));
  }

  function rates() {
    const f = model.frames;
    if (f.length < 2) return { rps: 0, cps: 0 };
    const a = f[0], b = f[f.length - 1];
    const dt = b.t - a.t;
    if (dt <= 0.05) return { rps: 0, cps: 0 };
    return { rps: Math.max(0, (b.req - a.req) / dt), cps: Math.max(0, (b.commits - a.commits) / dt) };
  }

  // ------------------------------------------------------------------ eased numbers
  const tweens = new Map(); // id -> {cur, target}
  function setNumber(id, target) {
    const t = tweens.get(id);
    if (!t) { tweens.set(id, { cur: target, target }); $(id).textContent = fmt(target); return; }
    t.target = target;
  }
  function stepTweens() {
    for (const [id, t] of tweens) {
      if (t.cur === t.target) continue;
      const diff = t.target - t.cur;
      t.cur = Math.abs(diff) < 0.5 ? t.target : t.cur + diff * 0.22;
      const text = fmt(t.cur);
      const el = $(id);
      if (el.textContent !== text) el.textContent = text;
    }
  }

  // ------------------------------------------------------------------ charts
  Chart.defaults.font.family = getComputedStyle(document.body).fontFamily;
  Chart.defaults.font.size = 11;
  Chart.defaults.color = "#64748b";

  const baseOptions = (yLabel) => ({
    maintainAspectRatio: false,
    animation: false,
    normalized: true,
    interaction: { mode: "index", intersect: false },
    layout: { padding: { top: 4 } },
    elements: { point: { radius: 0, hoverRadius: 3 }, line: { borderWidth: 1.75, tension: 0.35, cubicInterpolationMode: "monotone" } },
    scales: {
      x: { grid: { display: false }, border: { display: false }, ticks: { maxTicksLimit: 6, maxRotation: 0 } },
      y: { beginAtZero: true, grace: "8%", grid: { color: "#1e293b" }, border: { display: false },
           ticks: { maxTicksLimit: 5, callback: (v) => nf0.format(v) }, title: { display: false, text: yLabel } },
    },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: "#0f172a", borderColor: "#334155", borderWidth: 1, titleColor: "#e2e8f0",
        bodyColor: "#cbd5e1", padding: 10, displayColors: true, boxWidth: 8, boxHeight: 2,
        callbacks: { label: (c) => ` ${c.dataset.label}: ${nf0.format(c.parsed.y)}/s` },
      },
    },
  });

  const throughputChart = new Chart($("chart-throughput"), {
    type: "line",
    data: { labels: [], datasets: [
      { label: "Requests", data: [], borderColor: "#e2e8f0", backgroundColor: "rgba(226,232,240,0.05)", fill: "origin" },
      { label: "Commits", data: [], borderColor: "#06b6d4", backgroundColor: "transparent", fill: false },
    ] },
    options: baseOptions("per second"),
  });

  const deadlockChart = new Chart($("chart-deadlocks"), {
    type: "line",
    data: { labels: [], datasets: [
      { label: "Naïve", data: [], borderColor: "#f43f5e", backgroundColor: "rgba(244,63,94,0.06)", fill: "origin" },
      { label: "Fixed", data: [], borderColor: "#10b981", backgroundColor: "transparent", fill: false },
    ] },
    options: baseOptions("deadlocks per second"),
  });

  function renderCharts() {
    const full = model.full;
    if (!full) return;
    const byTs = new Map();
    for (const k of STRATEGIES) {
      for (const point of full.metrics.strategies[k].history) {
        const [ts, rps, cps, dps = 0] = point;
        const row = byTs.get(ts) || { rps: 0, cps: 0, naive: 0, fixed: 0 };
        row.rps += rps; row.cps += cps;
        if (k === "naive") row.naive += dps; else row.fixed += dps;
        byTs.set(ts, row);
      }
    }
    const rows = [...byTs.entries()].sort((a, b) => a[0] - b[0]).slice(-90);
    const labels = rows.map(([ts]) => new Date(ts * 1000).toLocaleTimeString([], { minute: "2-digit", second: "2-digit" }));

    throughputChart.data.labels = labels;
    throughputChart.data.datasets[0].data = rows.map(([, r]) => r.rps);
    throughputChart.data.datasets[1].data = rows.map(([, r]) => r.cps);
    throughputChart.update("none");

    deadlockChart.data.labels = labels;
    deadlockChart.data.datasets[0].data = rows.map(([, r]) => r.naive);
    deadlockChart.data.datasets[1].data = rows.map(([, r]) => r.fixed);
    deadlockChart.options.plugins.tooltip.callbacks.label = (c) => ` ${c.dataset.label}: ${nf0.format(c.parsed.y)}/s`;
    deadlockChart.update("none");
  }

  // ------------------------------------------------------------------ render (≤ every 100 ms)
  function setState(el, state) { if (el.dataset.state !== state) el.dataset.state = state; }
  function setText(id, text) { const el = $(id); if (el.textContent !== text) el.textContent = text; }

  function paintStrategy(strategy) {
    document.querySelectorAll("#strategy button").forEach((b) => b.setAttribute("aria-checked", b.dataset.strategy === strategy ? "true" : "false"));
    const note = $("strategy-note");
    if (note.dataset.for !== strategy) { note.innerHTML = NOTE[strategy] || ""; note.dataset.for = strategy; }
  }

  function render() {
    const live = model.live;
    if (!live) return;
    const full = model.full;
    const run = live.run;
    const busy = run.state === "running" || run.state === "comparing";

    // header
    const status = $("status");
    if (!model.connected) { status.dataset.tone = "down"; setText("status-text", "Reconnecting"); }
    else if (run.state === "running") { status.dataset.tone = "active"; setText("status-text", "Rush active"); }
    else if (run.state === "comparing") { status.dataset.tone = "active"; setText("status-text", "Race in progress"); }
    else { status.dataset.tone = "ready"; setText("status-text", "System ready"); }

    if (performance.now() > model.pendingSelectUntil && live.active_strategy !== model.selected) {
      model.selected = live.active_strategy;
      paintStrategy(model.selected);
    }

    // stat 1 + 2: rates derived from counter deltas
    const { rps, cps } = rates();
    model.peak = Math.max(model.peak, rps);
    setNumber("v-rps", rps);
    setText("f-rps", model.peak > 0 ? `Peak ${fmt(model.peak)} req/s` : "Waiting for traffic");
    setNumber("v-cps", cps);
    let cpsFoot = "Waiting for traffic";
    if (cps > 0.5) {
      const perCommit = rps / cps;
      cpsFoot = `${nf1.format(perCommit)} orders per commit`;
      if (live.active_strategy === "batched" && full) cpsFoot += ` · batch ${nf1.format(full.metrics.batch.avg_batch_size)}/${full.settings.batch_max_size}`;
    } else if (STRATEGIES.some((k) => live.s[k][2] > 0)) {
      cpsFoot = `${fmt(STRATEGIES.reduce((a, k) => a + live.s[k][2], 0))} commits since reset`;
    }
    setText("f-cps", cpsFoot);

    // stat 3: deadlocks of the strategy in use
    const active = live.active_strategy;
    const activeDl = (live.s[active] || [0, 0, 0, 0])[3];
    const activeReq = (live.s[active] || [0])[0];
    const naiveDl = live.s.naive[3];
    const fixedDl = live.s.pessimistic[3] + live.s.optimistic[3] + live.s.batched[3];
    setNumber("v-dl", activeDl);
    setText("f-dl", `Naïve ${fmt(naiveDl)} · Fixed strategies ${fmt(fixedDl)}`);
    setState($("card-deadlocks"), activeDl > 0 ? "bad" : (active !== "naive" && activeReq > 0 ? "good" : "neutral"));

    // stat 4: integrity
    const integ = live.integrity;
    const broken = integ.oversold_units > 0 || integ.negative_rows > 0 || integ.ok === false;
    setNumber("v-oversold", integ.oversold_units);
    let integFoot = "Every sale is backed by stock";
    if (full && full.integrity && full.integrity.products) {
      const products = full.integrity.products;
      const lowest = products.reduce((a, p) => (a === null || p.available < a.available ? p : a), null);
      if (broken && lowest) integFoot = `Lowest stock ${fmt(lowest.available)} · ${lowest.name}`;
      else if (products.some((p) => p.sold_units > 0)) {
        const soldOut = products.filter((p) => p.available === 0).length;
        integFoot = `All ${products.length} products ≥ 0 · ${soldOut} sold out`;
      }
    }
    setText("f-integrity", integFoot);
    const anyTraffic = STRATEGIES.some((k) => live.s[k][0] > 0);
    setState($("card-integrity"), broken ? "bad" : anyTraffic ? "good" : "neutral");

    // controls
    const runBtn = $("btn-run");
    const mode = busy ? "stop" : "start";
    if (runBtn.dataset.mode !== mode) {
      runBtn.dataset.mode = mode;
      runBtn.textContent = busy ? (run.state === "comparing" ? "Stop race" : "Stop test") : "Start high-concurrency test";
    }
    $("btn-reset").disabled = busy;
    $("btn-race").disabled = busy;
    const racing = run.state === "comparing";
    document.querySelectorAll("#strategy button").forEach((b) => { b.disabled = racing; });

    let label = "Ready";
    if (run.state === "running") label = `Running · ${LABEL[active]}`;
    else if (run.state === "comparing") {
      const done = full && full.runner ? (full.runner.comparison || []).length : 0;
      label = `Racing · ${LABEL[active]} (${Math.min(done + 1, 4)} of 4)`;
    } else if (run.state === "finished" && full && full.runner && full.runner.last_summary) {
      const s = full.runner.last_summary;
      label = s.label && s.label.startsWith("compare:")
        ? `Race finished · ${(full.runner.comparison || []).length} strategies`
        : `Finished in ${nf1.format(s.elapsed_s)} s`;
    } else if (run.state === "stopped") label = "Stopped";
    else if (run.state === "error") label = "Run failed";
    setText("run-label", label);
    setText("run-count", run.target ? `${fmt(run.completed)} / ${fmt(run.target)}` : "");
    const pct = run.target ? Math.min(100, (100 * run.completed) / run.target) : 0;
    const bar = $("progress-bar");
    bar.style.width = `${pct}%`;
    bar.style.opacity = busy || pct > 0 ? "1" : "0";

    if (full) {
      setText("foot-db", `PostgreSQL · ${full.settings.pool_max_size} connections · batch ${full.settings.batch_max_size} / ${full.settings.batch_max_wait_ms} ms`);
      renderRace(full.runner);
    }
  }

  // ------------------------------------------------------------------ race results
  let raceKey = "";
  function renderRace(runner) {
    const rows = (runner && runner.comparison) || [];
    const key = JSON.stringify([runner && runner.state, rows.map((r) => [r.strategy, r.requests])]);
    if (key === raceKey) return;
    raceKey = key;
    const section = $("race");
    section.hidden = rows.length === 0;
    if (!rows.length) return;

    const body = $("race-body");
    body.replaceChildren();
    for (const r of rows) {
      const tr = document.createElement("tr");
      const name = document.createElement("td");
      const dot = document.createElement("span"); dot.className = "dot"; dot.style.background = DOT[r.strategy] || "#94a3b8";
      name.append(dot, document.createTextNode(LABEL[r.strategy] || r.strategy));
      tr.append(name,
        td(fmt(r.throughput_rps), "r"),
        td(fmt(r.confirmed), "r"),
        td(fmt(r.dropped), r.dropped ? "r bad" : "r dim"),
        td(fmt(r.deadlocks_hit), r.deadlocks_hit ? "r bad" : "r dim"),
        td(fmt(r.db_commits), "r"),
        td(`${fmt(r.latency_ms.p95)} ms`, "r"),
        td(fmt(r.oversold_units), r.oversold_units ? "r bad" : "r ok"));
      body.appendChild(tr);
    }
    renderVerdict(rows, runner.state);
  }

  function td(text, cls) { const el = document.createElement("td"); el.textContent = text; el.className = cls; return el; }

  function renderVerdict(rows, state) {
    const v = $("verdict");
    v.replaceChildren();
    const add = (text, cls) => {
      if (!cls) { v.appendChild(document.createTextNode(text)); return; }
      const s = document.createElement("span"); s.className = cls; s.textContent = text; v.appendChild(s);
    };
    if (state === "comparing") { add(`${rows.length} of 4 strategies finished.`); return; }
    const safe = rows.filter((r) => r.integrity_ok).sort((a, b) => b.throughput_rps - a.throughput_rps);
    if (!safe.length) { add("No strategy kept the books balanced in this race."); return; }
    const best = safe[0], next = safe[1];
    add(`${LABEL[best.strategy]} `); add("led", "ok");
    add(` with ${fmt(best.throughput_rps)} req/s`);
    if (next && next.throughput_rps > 0) add(`, ${nf1.format(best.throughput_rps / next.throughput_rps)}× ${LABEL[next.strategy]}`);
    add(`, ${fmt(best.deadlocks_hit)} deadlocks and ${fmt(best.oversold_units)} oversold.`);
    const naive = rows.find((r) => r.strategy === "naive");
    if (naive && (naive.oversold_units > 0 || naive.dropped > 0)) {
      add(" Naïve ");
      if (naive.oversold_units > 0) { add(`oversold ${fmt(naive.oversold_units)} units`, "bad"); add(" and"); }
      add(` lost ${fmt(naive.dropped)} requests.`);
    }
  }

  // ------------------------------------------------------------------ frame loop
  let lastRender = 0;
  function frame(now) {
    if (model.dirty && now - lastRender >= RENDER_INTERVAL_MS) {
      model.dirty = false;
      lastRender = now;
      render();
    }
    if (model.chartsDirty) { model.chartsDirty = false; renderCharts(); }
    stepTweens();
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);

  // ------------------------------------------------------------------ controls
  function message(text, tone) {
    const el = $("message");
    el.textContent = text || "";
    if (tone) el.dataset.tone = tone; else delete el.dataset.tone;
    clearTimeout(message.t);
    if (text) message.t = setTimeout(() => { el.textContent = ""; }, 6000);
  }

  async function api(path, body) {
    const res = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body || {}) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const detail = typeof data.detail === "string" ? data.detail : res.statusText;
      throw new Error(detail);
    }
    return data;
  }

  const inputs = {
    users: $("users"), requests: $("requests"), contention: $("contention"), stock: $("stock"), items: $("items"),
  };
  function params() {
    return {
      users: Number(inputs.users.value),
      total_requests: Number(inputs.requests.value),
      contention: Number(inputs.contention.value) / 100,
      max_items: Number(inputs.items.value),
      stock: Number(inputs.stock.value),
    };
  }
  function paintParams() {
    const p = params();
    $("o-users").textContent = fmt(p.users);
    $("o-requests").textContent = fmt(p.total_requests);
    $("o-contention").textContent = `${Math.round(p.contention * 100)}%`;
    $("o-stock").textContent = `${nf1.format(p.stock)}×`;
    $("o-items").textContent = String(p.max_items);
    $("params-summary").textContent = `${fmt(p.users)} users · ${fmt(p.total_requests)} requests · ${Math.round(p.contention * 100)}% on hottest · ${nf1.format(p.stock)}× stock`;
  }
  Object.values(inputs).forEach((el) => {
    el.addEventListener("input", paintParams);
    el.addEventListener("pointerdown", () => { paramsTouched = true; });
    el.addEventListener("keydown", () => { paramsTouched = true; });
  });
  paintParams();

  function loadBody(strategy) {
    const p = params();
    return { users: p.users, total_requests: p.total_requests, contention: p.contention, max_items: p.max_items, strategy, seed: 42 };
  }

  document.querySelectorAll("#strategy button").forEach((btn) => btn.addEventListener("click", async () => {
    if (btn.disabled) return;
    const strategy = btn.dataset.strategy;
    model.selected = strategy;
    model.pendingSelectUntil = performance.now() + 800;
    paintStrategy(strategy);
    try { await api("/api/strategy", { strategy }); }
    catch (e) { model.pendingSelectUntil = 0; model.dirty = true; message(e.message, "bad"); }
  }));

  $("btn-run").addEventListener("click", async () => {
    try {
      if ($("btn-run").dataset.mode === "stop") { await api("/api/load/stop"); message("Stopping. Requests already in flight will finish."); }
      else { await api("/api/load/start", loadBody(model.selected)); message(""); }
    } catch (e) { message(e.message, "bad"); }
  });

  $("btn-race").addEventListener("click", async () => {
    try { await api("/api/load/compare", { ...loadBody(model.selected), strategies: STRATEGIES }); message(""); }
    catch (e) { message(e.message, "bad"); }
  });

  $("btn-reset").addEventListener("click", async () => {
    const mult = params().stock;
    const defaults = model.full && model.full.settings && model.full.settings.default_stock;
    const stock = defaults ? Object.fromEntries(Object.entries(defaults).map(([id, qty]) => [id, Math.max(1, Math.round(qty * mult))])) : null;
    try {
      await api("/api/reset", stock ? { stock } : {});
      model.frames = []; model.peak = 0; raceKey = "";
      message(`Stock reset to ${nf1.format(mult)}× and counters cleared.`);
    } catch (e) { message(e.message, "bad"); }
  });

  paintStrategy(model.selected);

  // ------------------------------------------------------------------ socket
  let retry = 0;
  function connect() {
    const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws/stream`);
    ws.onopen = () => { retry = 0; model.connected = true; model.dirty = true; };
    ws.onmessage = (ev) => { try { ingest(JSON.parse(ev.data)); } catch (err) { console.error(err); } };
    ws.onclose = () => {
      model.connected = false; model.dirty = true;
      setTimeout(connect, Math.min(5000, 300 * 2 ** retry++));
    };
    ws.onerror = () => ws.close();
  }
  connect();
})();
