/* Polymarket Whale Tracker - frontend
 *
 * Fetches static JSON produced by the Python pipeline and renders a
 * sortable/filterable leaderboard, market heatmap, and per-whale radar chart.
 */
(() => {
  "use strict";

  const REFRESH_MS = 30 * 60 * 1000; // 30 minutes
  const POLYMARKET_BASE = "https://polymarket.com/event/";

  const state = {
    wallets: [],
    markets: [],
    filtered: [],
    sortKey: "final_score",
    sortDir: "desc",
    expandedAddr: null,
    selectedAddr: null,
    radar: null,
    refreshDeadline: Date.now() + REFRESH_MS,
  };

  // --------------------------------------------------------------------
  // Boot
  // --------------------------------------------------------------------
  document.addEventListener("DOMContentLoaded", () => {
    bindControls();
    initParticles();
    initObservation();
    load();
    setInterval(tickCountdown, 1000);
    setTimeout(() => window.location.reload(), REFRESH_MS);
  });

  // --------------------------------------------------------------------
  // Bioluminescent particle field
  // --------------------------------------------------------------------
  function initParticles() {
    const canvas = document.getElementById("particle-field");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    let w = 0, h = 0, particles = [];

    const resize = () => {
      w = window.innerWidth;
      h = window.innerHeight;
      canvas.width  = Math.floor(w * dpr);
      canvas.height = Math.floor(h * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      const target = Math.round((w * h) / 9500);
      particles = new Array(target).fill(0).map(() => spawn(true));
    };

    const spawn = (initial = false) => ({
      x: Math.random() * w,
      y: initial ? Math.random() * h : h + 10,
      r: Math.random() * 1.1 + 0.25,        // radius
      vx: (Math.random() - 0.5) * 0.08,
      vy: -(Math.random() * 0.25 + 0.05),   // rising
      a: Math.random() * 0.5 + 0.25,        // base alpha
      tw: Math.random() * Math.PI * 2,      // twinkle phase
      tws: Math.random() * 0.02 + 0.005,    // twinkle speed
      big: Math.random() < 0.04,
    });

    let last = performance.now();
    const tick = (t) => {
      const dt = Math.min(40, t - last);
      last = t;
      ctx.clearRect(0, 0, w, h);
      for (let i = 0; i < particles.length; i++) {
        const p = particles[i];
        p.x += p.vx * dt;
        p.y += p.vy * dt;
        p.tw += p.tws * dt;
        if (p.y < -5 || p.x < -20 || p.x > w + 20) {
          particles[i] = spawn();
          continue;
        }
        const tw = (Math.sin(p.tw) + 1) * 0.5;
        const alpha = Math.min(1, p.a * (0.4 + tw * 0.8));
        if (p.big) {
          // soft halo around bright ones
          const grad = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, p.r * 9);
          grad.addColorStop(0, `rgba(255,255,255,${alpha * 0.85})`);
          grad.addColorStop(0.4, `rgba(255,255,255,${alpha * 0.12})`);
          grad.addColorStop(1, "rgba(255,255,255,0)");
          ctx.fillStyle = grad;
          ctx.beginPath();
          ctx.arc(p.x, p.y, p.r * 9, 0, Math.PI * 2);
          ctx.fill();
        }
        ctx.fillStyle = `rgba(255,255,255,${alpha})`;
        ctx.beginPath();
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx.fill();
      }
      requestAnimationFrame(tick);
    };

    window.addEventListener("resize", resize, { passive: true });
    resize();
    requestAnimationFrame(tick);
  }

  // --------------------------------------------------------------------
  // Observation feed: hide fallback once the video plays
  // --------------------------------------------------------------------
  function initObservation() {
    const video = document.querySelector(".observation__video");
    const fallback = document.querySelector(".observation__fallback");
    if (!video || !fallback) return;
    const hide = () => { fallback.style.display = "none"; };
    video.addEventListener("playing", hide, { once: true });
    video.addEventListener("loadeddata", hide, { once: true });
    video.addEventListener("error", () => { video.style.display = "none"; }, { once: true });
    // Check if at least one <source> has a reachable file; if the element
    // has no src resolved after a moment, assume missing.
    setTimeout(() => {
      if (video.networkState === HTMLMediaElement.NETWORK_NO_SOURCE) {
        video.style.display = "none";
      }
    }, 1500);
  }

  async function load() {
    try {
      const [whales, markets, lastUpdated, backtest] = await Promise.all([
        fetchJSON("data/whales.json"),
        fetchJSON("data/markets.json"),
        fetchJSON("data/last_updated.json"),
        fetchJSON("data/backtest_report.json").catch(() => null),
      ]);
      state.wallets = (whales && whales.wallets) || [];
      state.markets = (markets && markets.markets) || [];
      state.backtest = backtest;
      setLastUpdated(lastUpdated || whales);
      document.getElementById("whale-count").textContent = state.wallets.length;
      applyFilters();
      renderSignals();
      renderConsensus();
      renderBacktest();
    } catch (err) {
      console.error("Failed to load data", err);
      document.getElementById("leaderboard-body").innerHTML =
        `<tr><td colspan="9" class="empty">Failed to load data.json — the pipeline may not have run yet.</td></tr>`;
    }
  }

  async function fetchJSON(path) {
    const resp = await fetch(path, { cache: "no-store" });
    if (!resp.ok) throw new Error(`${path} -> HTTP ${resp.status}`);
    return resp.json();
  }

  // --------------------------------------------------------------------
  // Controls / filters
  // --------------------------------------------------------------------
  function bindControls() {
    document.getElementById("min-score").addEventListener("input", applyFilters);
    document.getElementById("min-winrate").addEventListener("input", applyFilters);
    document.getElementById("min-volume").addEventListener("input", applyFilters);
    document.getElementById("search").addEventListener("input", applyFilters);
    document.getElementById("reset-filters").addEventListener("click", () => {
      document.getElementById("min-score").value = 0;
      document.getElementById("min-winrate").value = 0;
      document.getElementById("min-volume").value = 0;
      document.getElementById("search").value = "";
      applyFilters();
    });

    document.querySelectorAll("thead th.sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.dataset.sort;
        if (!key) return;
        if (state.sortKey === key) {
          state.sortDir = state.sortDir === "desc" ? "asc" : "desc";
        } else {
          state.sortKey = key;
          state.sortDir = "desc";
        }
        renderLeaderboard();
      });
    });
  }

  function applyFilters() {
    const minScore = parseFloat(document.getElementById("min-score").value) || 0;
    const minWin = parseFloat(document.getElementById("min-winrate").value) || 0;
    const minVol = parseFloat(document.getElementById("min-volume").value) || 0;
    const q = document.getElementById("search").value.trim().toLowerCase();

    state.filtered = state.wallets.filter((w) => {
      if ((w.final_score || 0) < minScore) return false;
      if ((w.win_rate || 0) < minWin) return false;
      if ((w.total_volume_usdc || 0) < minVol) return false;
      if (q) {
        const hay = `${(w.address || "").toLowerCase()} ${(w.label || "").toLowerCase()}`;
        if (!hay.includes(q)) return false;
      }
      return true;
    });
    renderLeaderboard();
  }

  // --------------------------------------------------------------------
  // Leaderboard
  // --------------------------------------------------------------------
  function renderLeaderboard() {
    const body = document.getElementById("leaderboard-body");
    const rows = [...state.filtered];
    const dir = state.sortDir === "asc" ? 1 : -1;
    rows.sort((a, b) => {
      const av = a[state.sortKey];
      const bv = b[state.sortKey];
      if (av === bv) return 0;
      if (av === undefined || av === null) return 1;
      if (bv === undefined || bv === null) return -1;
      if (typeof av === "string") return av.localeCompare(bv) * dir;
      return (av - bv) * dir;
    });

    document.querySelectorAll("thead th.sortable").forEach((th) => {
      th.classList.remove("sortable-asc", "sortable-desc");
      if (th.dataset.sort === state.sortKey) {
        th.classList.add(state.sortDir === "asc" ? "sortable-asc" : "sortable-desc");
      }
    });

    if (!rows.length) {
      body.innerHTML = `<tr><td colspan="9" class="empty">No whales match the current filters.</td></tr>`;
      return;
    }

    body.innerHTML = "";
    rows.forEach((w) => {
      const tr = document.createElement("tr");
      tr.className = "whale-row";
      tr.dataset.addr = w.address;
      if (w.address === state.selectedAddr) tr.classList.add("selected");
      const pnl = w.total_pnl_usdc;
      const roi = w.overall_roi;
      tr.innerHTML = `
        <td>${w.rank ?? "—"}</td>
        <td>${walletCell(w)}</td>
        <td class="numeric score-cell" style="--score-color:${scoreColor(w.final_score)}">${fmt(w.final_score, 1)}</td>
        <td class="numeric ${signClass(pnl)}"><b>${fmtMoney(pnl, true)}</b></td>
        <td class="numeric ${signClass(roi)}">${fmtPct(roi)}</td>
        <td class="numeric">${fmtPct(w.win_rate)} <span class="muted small">(${(w.wins ?? 0)}/${(w.wins ?? 0) + (w.losses ?? 0)})</span></td>
        <td class="numeric">${w.resolved_markets ?? 0}</td>
        <td class="numeric">${fmtMoney(w.capital_deployed_usdc)}</td>
        <td><button class="btn ghost small" data-act="expand">${state.expandedAddr === w.address ? "Hide" : "Details"}</button></td>
      `;
      tr.addEventListener("click", (ev) => onRowClick(ev, w));
      body.appendChild(tr);
      if (state.expandedAddr === w.address) {
        body.appendChild(buildExpandedRow(w));
      }
    });
  }

  function onRowClick(ev, wallet) {
    const isCopy = ev.target.closest("[data-act='copy']");
    if (isCopy) {
      ev.stopPropagation();
      navigator.clipboard.writeText(wallet.address).catch(() => {});
      const btn = ev.target.closest("button");
      if (btn) {
        const prev = btn.textContent;
        btn.textContent = "✓";
        setTimeout(() => (btn.textContent = prev), 800);
      }
      return;
    }
    state.selectedAddr = wallet.address;
    state.expandedAddr = state.expandedAddr === wallet.address ? null : wallet.address;
    renderLeaderboard();
    renderRadar(wallet);
  }

  function walletCell(w) {
    const short = `${w.address.slice(0, 6)}…${w.address.slice(-4)}`;
    const label = w.label ? `<span class="addr-label">${escapeHtml(w.label)}</span>` : "";
    return `<span class="addr-cell">
      <span class="addr">${short}</span>${label}
      <button class="copy-btn" data-act="copy" title="Copy address">⧉</button>
    </span>`;
  }

  // --------------------------------------------------------------------
  // Expanded row
  // --------------------------------------------------------------------
  function buildExpandedRow(wallet) {
    const tpl = document.getElementById("expanded-row-template");
    const node = tpl.content.firstElementChild.cloneNode(true);

    const open = wallet.open_positions || [];
    const resolved = wallet.resolved_positions || [];

    node.querySelector(".positions").innerHTML = renderPositionsTable(open);
    node.querySelector(".resolved").innerHTML = renderResolvedTable(resolved);
    node.querySelector(".category-mix").innerHTML = renderCategoryMix(wallet.category_breakdown || {});

    const openCt = node.querySelector("[data-slot='open-count']");
    const resCt  = node.querySelector("[data-slot='resolved-count']");
    if (openCt) openCt.textContent = open.length ? `(${open.length})` : "";
    if (resCt)  resCt.textContent  = resolved.length ? `(${resolved.length})` : "";

    return node;
  }

  function renderResolvedTable(positions) {
    if (!positions.length) return `<div class="empty">No resolved positions tracked.</div>`;
    const sorted = [...positions].sort((a, b) => (b.cash_pnl_usdc || 0) - (a.cash_pnl_usdc || 0));
    const rows = sorted.map((p) => {
      const pnl = p.cash_pnl_usdc ?? p.unrealized_pnl ?? 0;
      const pnlClass = signClass(pnl);
      const pct = p.percent_pnl ?? 0;
      const href = p.market_slug ? `${POLYMARKET_BASE}${p.market_slug}` : null;
      const title = p.market_title || p.market_id || "—";
      const titleCell = href
        ? `<a href="${href}" target="_blank" rel="noopener">${escapeHtml(title)}</a>`
        : escapeHtml(title);
      return `<tr>
        <td>${titleCell}</td>
        <td class="${p.side === 'YES' ? 'pos' : p.side === 'NO' ? 'neg' : 'muted'}">${escapeHtml(p.side || "")}</td>
        <td>${fmtMoney(p.initial_value_usdc ?? p.size_usdc)}</td>
        <td class="${pnlClass}"><b>${fmtMoney(pnl, true)}</b></td>
        <td class="${pnlClass}">${fmtPct(pct)}</td>
      </tr>`;
    }).join("");
    return `<table>
      <thead><tr>
        <th>Market</th><th>Side</th><th>Staked</th><th>PnL</th><th>%</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  }

  function renderPositionsTable(positions) {
    if (!positions.length) return `<div class="empty">No open positions tracked.</div>`;
    const rows = positions.map((p) => {
      const pnlClass = signClass(p.unrealized_pnl);
      const slug = p.market_slug ? `${POLYMARKET_BASE}${p.market_slug}` : null;
      const title = p.market_title || p.market_id || "—";
      const titleCell = slug
        ? `<a href="${slug}" target="_blank" rel="noopener">${escapeHtml(title)}</a>`
        : escapeHtml(title);
      return `<tr>
        <td>${titleCell}</td>
        <td class="${p.side === 'YES' ? 'pos' : p.side === 'NO' ? 'neg' : 'muted'}">${escapeHtml(p.side || "")}</td>
        <td>${fmtMoney(p.size_usdc)}</td>
        <td>${fmt(p.entry_price, 3)}</td>
        <td>${fmt(p.current_price, 3)}</td>
        <td class="${pnlClass}">${fmtMoney(p.unrealized_pnl, true)}</td>
      </tr>`;
    }).join("");
    return `<table>
      <thead><tr>
        <th>Market</th><th>Side</th><th>Size</th><th>Entry</th><th>Current</th><th>uPnL</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  }

  function renderTradesTable(trades) {
    if (!trades.length) return `<div class="empty">No recent trades.</div>`;
    const rows = trades.map((t) => `<tr>
      <td>${escapeHtml(t.market_title || t.market_id || "—")}</td>
      <td>${escapeHtml(t.side || "")}</td>
      <td>${fmt(t.price, 3)}</td>
      <td>${fmtMoney(t.size_usdc)}</td>
      <td class="muted small">${formatWhen(t.timestamp)}</td>
    </tr>`).join("");
    return `<table>
      <thead><tr>
        <th>Market</th><th>Side</th><th>Price</th><th>Size</th><th>When</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  }

  function renderCategoryMix(mix) {
    const entries = Object.entries(mix).sort((a, b) => b[1] - a[1]);
    if (!entries.length) return `<div class="empty">No category data.</div>`;
    return entries.map(([cat, n]) =>
      `<span class="cat-pill"><strong>${n}</strong>${escapeHtml(cat)}</span>`
    ).join("");
  }

  // --------------------------------------------------------------------
  // Radar chart
  // --------------------------------------------------------------------
  function renderRadar(wallet) {
    const wrap = document.getElementById("radar-wrap");
    const caption = document.getElementById("radar-caption");
    if (wrap) wrap.hidden = false;
    if (caption) caption.textContent = `Breakdown for ${wallet.address.slice(0, 10)}…${wallet.address.slice(-4)}`;
    const ctx = document.getElementById("radar-chart").getContext("2d");

    const data = {
      labels: ["ROI", "Calibration", "Consistency", "Volume", "Early entry"],
      datasets: [{
        label: `Rank #${wallet.rank}`,
        data: [
          wallet.roi_score ?? 0,
          wallet.calibration_score ?? 0,
          wallet.consistency_score ?? 0,
          wallet.volume_score ?? 0,
          wallet.early_entry_score ?? 0,
        ],
        backgroundColor: "rgba(74, 255, 142, 0.12)",
        borderColor: "rgba(74, 255, 142, 0.9)",
        pointBackgroundColor: "rgba(74, 255, 142, 1)",
        pointBorderColor: "rgba(74, 255, 142, 0.9)",
        pointHoverRadius: 5,
        borderWidth: 1.5,
      }],
    };

    const options = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: "rgba(255,255,255,0.6)", font: { family: "JetBrains Mono, monospace", size: 11 } } },
      },
      scales: {
        r: {
          suggestedMin: 0,
          suggestedMax: 100,
          angleLines: { color: "rgba(255,255,255,0.08)" },
          grid:       { color: "rgba(255,255,255,0.08)" },
          pointLabels: { color: "rgba(255,255,255,0.85)", font: { family: "Inter, sans-serif", size: 11 } },
          ticks: { color: "rgba(255,255,255,0.3)", backdropColor: "transparent", stepSize: 25, font: { size: 10 } },
        },
      },
    };

    if (state.radar) {
      state.radar.data = data;
      state.radar.options = options;
      state.radar.update();
    } else {
      state.radar = new Chart(ctx, { type: "radar", data, options });
    }
  }

  // --------------------------------------------------------------------
  // Smart-money signals — flat list of top whale entries worth watching.
  // A "signal" is an open position held by one of our top-ranked whales,
  // scored by (whale.final_score × size × freshness).
  // --------------------------------------------------------------------
  function renderSignals() {
    const feed = document.getElementById("signals-feed");
    const activeIds = activeMarketSet();
    const signals = deriveSignals(state.wallets, activeIds);
    document.getElementById("signal-count").textContent = signals.length;

    if (!signals.length) {
      feed.innerHTML = `<div class="signal-empty">No active-market signals from ranked whales yet.</div>`;
      return;
    }

    feed.innerHTML = signals.slice(0, 30).map((s) => {
      const href = s.slug ? `${POLYMARKET_BASE}${s.slug}` : null;
      const title = escapeHtml(s.market_title || s.market_id || "—");
      const drift = s.drift;
      const driftClass = drift > 0 ? "drift-pos" : drift < 0 ? "drift-neg" : "muted";
      const driftStr = drift == null ? "—" : `${drift > 0 ? "+" : ""}${(drift * 100).toFixed(1)}%`;
      const sideCls = s.side === "YES" ? "yes" : s.side === "NO" ? "no" : "";
      const rankBadge = s.rank ? `#${s.rank}` : `—`;
      const short = `${s.address.slice(0, 6)}…${s.address.slice(-4)}`;
      const titleEl = href
        ? `<a class="signal-title" href="${href}" target="_blank" rel="noopener">${title}</a>`
        : `<span class="signal-title">${title}</span>`;

      const openedAgo = s.entry_timestamp ? relTimeAgo(s.entry_timestamp) : null;
      const closesIn  = s.end_date ? relTimeUntil(s.end_date) : null;
      const timeBlock = (openedAgo || closesIn)
        ? `<div class="signal-time">
             ${openedAgo ? `<span class="t-opened">Opened ${openedAgo}</span>` : ""}
             ${closesIn  ? `<span class="t-closes">${closesIn.label}</span>` : ""}
           </div>`
        : "";

      return `<div class="signal-row">
        <span class="signal-side ${sideCls}">${escapeHtml(s.side || "?")}</span>
        <div class="signal-body">
          ${titleEl}
          <div class="signal-stats">
            <span>Entry <b>${fmt(s.entry_price, 3)}</b></span>
            <span class="sep">·</span>
            <span>Now <b>${fmt(s.current_price, 3)}</b></span>
            <span class="sep">·</span>
            <span class="${driftClass}"><b>${driftStr}</b></span>
            <span class="sep">·</span>
            <span class="size">${fmtMoney(s.size_usdc)} pos</span>
            <span class="sep">·</span>
            <span class="whale">
              <span class="rank">${rankBadge}</span>
              ${short} · <b>${fmtMoney(s.whale_total_pnl, true)}</b> lifetime
            </span>
          </div>
        </div>
        ${timeBlock}
        ${href ? `<a class="trigger-link" href="${href}" target="_blank" rel="noopener">Trade →</a>` : ""}
      </div>`;
    }).join("");
  }

  function activeMarketSet() {
    // state.markets comes from the active-markets snapshot, so anything in it
    // is currently tradeable (active=true, closed=false, not past end-date).
    const s = new Set();
    state.markets.forEach((m) => { if (m.condition_id) s.add(m.condition_id); });
    return s;
  }

  function deriveSignals(wallets, activeIds) {
    const out = [];
    wallets.forEach((w) => {
      (w.open_positions || []).forEach((p) => {
        if (!p.market_id || !(p.size_usdc > 0)) return;
        if (activeIds.size && !activeIds.has(p.market_id)) return;  // filter expired
        const entry = p.entry_price || null;
        const current = p.current_price || null;
        const drift = (entry && current) ? (current - entry) : null;
        out.push({
          market_id: p.market_id,
          market_title: p.market_title,
          slug: p.market_slug,
          side: p.side,
          entry_price: entry,
          current_price: current,
          drift,
          size_usdc: p.size_usdc,
          unrealized_pnl: p.unrealized_pnl,
          entry_timestamp: p.entry_timestamp,
          end_date: p.end_date,
          address: w.address,
          label: w.label,
          whale_score: w.final_score || 0,
          whale_total_pnl: w.total_pnl_usdc || 0,
          rank: w.rank || null,
        });
      });
    });
    out.sort((a, b) =>
      (b.whale_score * Math.log1p(b.size_usdc)) -
      (a.whale_score * Math.log1p(a.size_usdc))
    );
    return out;
  }

  // --------------------------------------------------------------------
  // Consensus markets — ≥2 tracked whales on the same side, no dissent.
  // --------------------------------------------------------------------
  // --------------------------------------------------------------------
  // Backtest verdict panel
  // --------------------------------------------------------------------
  function renderBacktest() {
    const panel = document.getElementById("backtest-panel");
    const b = state.backtest;
    if (!b) { panel.hidden = true; return; }
    panel.hidden = false;

    const vEl = document.getElementById("backtest-verdict");
    const verdictClass = classifyVerdict(b.verdict || "");
    vEl.className = `backtest-verdict ${verdictClass}`;
    vEl.textContent = b.verdict || "";

    const grid = document.getElementById("backtest-grid");
    const strategies = b.strategies || [];

    if (!strategies.length) {
      grid.innerHTML = `<div class="bt-meta" style="grid-column:1/-1">No strategy results yet — run the pipeline + backtest.</div>`;
      return;
    }

    const header = `
      <div class="bt-head">Strategy</div>
      <div class="bt-head">K</div>
      <div class="bt-head">N</div>
      <div class="bt-head">Hit rate (95% CI)</div>
      <div class="bt-head">p-value</div>
      <div class="bt-head">Net ROI (95% CI)</div>
      <div class="bt-head">OOS ROI</div>
    `;

    const strategyLabels = {
      broad:             "All bets",
      hard_bets:         "Hard bets (30–70%)",
      hard_conv:         "Hard + ≥$5k",
      hard_conv_decorr:  "Hard + ≥$5k + 1×/mkt",
    };

    const rows = strategies.flatMap((s) => {
      const label = strategyLabels[s.name] || s.name;
      const baseline = (s.baseline_win_rate != null)
        ? `${(s.baseline_win_rate * 100).toFixed(1)}%`
        : "—";
      const tks = (s.top_k || []).filter((t) => t.observations > 0);
      if (!tks.length) {
        return [`
          <div class="bt-cell bt-strat">${label}</div>
          <div class="bt-cell bt-span-muted" style="grid-column: 2 / -1; text-align: center;">—</div>
        `];
      }
      return tks.map((t, i) => {
        const p = t.p_value_one_sided;
        const sigClass = p < 0.05 ? "pos" : p < 0.10 ? "warn" : "muted";
        const netClass = t.net_roi_after_costs > 0.01 ? "pos"
                       : t.net_roi_after_costs < -0.01 ? "neg" : "muted";
        const wci = t.hit_rate_ci95 || [0, 0];
        const nci = t.net_roi_ci95  || [0, 0];
        const oos = t.out_of_sample;
        const oosCell = oos
          ? `<span class="${oos.net_roi > 0 ? 'pos' : oos.net_roi < 0 ? 'neg' : 'muted'}">${(oos.net_roi*100).toFixed(1)}% <span class="muted small">(${oos.observations})</span></span>`
          : `<span class="muted">—</span>`;
        return `
          <div class="bt-cell bt-strat">${i === 0 ? `${label}<br><span class="muted small">base ${baseline}</span>` : ""}</div>
          <div class="bt-cell bt-k">K=${t.k}</div>
          <div class="bt-cell">${t.observations}</div>
          <div class="bt-cell">
            <b>${(t.hit_rate * 100).toFixed(1)}%</b>
            <span class="muted small">${(wci[0]*100).toFixed(0)}–${(wci[1]*100).toFixed(0)}%</span>
          </div>
          <div class="bt-cell ${sigClass}">${p < 0.001 ? "<0.001" : p.toFixed(3)}</div>
          <div class="bt-cell ${netClass}">
            <b>${(t.net_roi_after_costs * 100).toFixed(1)}%</b>
            <span class="muted small">${(nci[0]*100).toFixed(0)} to ${(nci[1]*100).toFixed(0)}%</span>
          </div>
          <div class="bt-cell">${oosCell}</div>
        `;
      });
    });

    grid.innerHTML = header + rows.join("");

    const meta = document.createElement("div");
    meta.className = "bt-meta";
    meta.innerHTML = `
      <b>Corpus:</b> ${b.n_events || 0} (wallet × resolved-market) observations.
      <b>Verdict requires:</b> n ≥ 50, p &lt; 0.05, net ROI &gt; 3%.
      <b>Costs:</b> 2% fee + 0.5–3% slippage by size.
      <b>OOS:</b> last 30% of events, ranked using only prior data.
    `;
    grid.appendChild(meta);
  }

  function classifyVerdict(text) {
    if (/TRADEABLE SIGNAL/i.test(text)) return "verdict-good";
    if (/NO TRADEABLE SIGNAL/i.test(text)) return "verdict-warn";
    if (/WEAK SIGNAL/i.test(text))      return "verdict-warn";
    if (/NO SIGNAL/i.test(text))        return "verdict-bad";
    return "verdict-muted";
  }

  function renderConsensus() {
    const body = document.getElementById("consensus-body");
    const activeIds = activeMarketSet();
    const cards = deriveConsensus(state.wallets, activeIds);

    if (!cards.length) {
      body.innerHTML = `<tr><td colspan="7" class="empty">No consensus markets yet — waiting on more whale coverage.</td></tr>`;
      return;
    }

    body.innerHTML = cards.slice(0, 25).map((c) => {
      const href = c.slug ? `${POLYMARKET_BASE}${c.slug}` : null;
      const title = escapeHtml(c.title || c.market_id || "—");
      const titleCell = href
        ? `<a href="${href}" target="_blank" rel="noopener">${title}</a>`
        : title;
      const sideCls = c.side === "YES" ? "side-yes" : "side-no";
      const trade = href ? `<a class="trigger-link" href="${href}" target="_blank" rel="noopener">Trade →</a>` : "";
      return `<tr>
        <td class="market">${titleCell}</td>
        <td class="${sideCls}">${escapeHtml(c.side)}</td>
        <td class="numeric">${c.whales}</td>
        <td class="numeric">${fmt(c.avg_entry, 3)}</td>
        <td class="numeric">${fmt(c.current, 3)}</td>
        <td class="numeric">${fmtMoney(c.total_size)}</td>
        <td>${trade}</td>
      </tr>`;
    }).join("");
  }

  function deriveConsensus(wallets, activeIds) {
    // group positions per market_id → { YES: [...], NO: [...] }
    const byMarket = new Map();
    wallets.forEach((w) => {
      (w.open_positions || []).forEach((p) => {
        if (!p.market_id || !p.side) return;
        if (!(p.size_usdc > 0)) return;
        if (activeIds && activeIds.size && !activeIds.has(p.market_id)) return;
        if (!byMarket.has(p.market_id)) {
          byMarket.set(p.market_id, {
            title: p.market_title,
            slug: p.market_slug,
            YES: [], NO: [],
          });
        }
        const rec = byMarket.get(p.market_id);
        if (p.market_title && !rec.title) rec.title = p.market_title;
        if (p.market_slug && !rec.slug)   rec.slug  = p.market_slug;
        const side = p.side === "YES" ? "YES" : p.side === "NO" ? "NO" : null;
        if (side) rec[side].push(p);
      });
    });

    const cards = [];
    byMarket.forEach((rec, market_id) => {
      const yes = rec.YES.length;
      const no  = rec.NO.length;
      if (yes + no < 2) return;                 // need at least two whales
      if (yes > 0 && no > 0) return;            // dissent kills consensus
      const side = yes > 0 ? "YES" : "NO";
      const positions = side === "YES" ? rec.YES : rec.NO;
      const totalSize = positions.reduce((a, p) => a + (p.size_usdc || 0), 0);
      const weightedEntry = totalSize > 0
        ? positions.reduce((a, p) => a + (p.entry_price || 0) * (p.size_usdc || 0), 0) / totalSize
        : 0;
      const currentAvg = positions
        .filter((p) => p.current_price)
        .reduce((a, p, _, arr) => a + p.current_price / arr.length, 0);
      cards.push({
        market_id,
        title: rec.title,
        slug: rec.slug,
        side,
        whales: positions.length,
        total_size: totalSize,
        avg_entry: weightedEntry,
        current: currentAvg,
      });
    });
    cards.sort((a, b) => (b.whales - a.whales) || (b.total_size - a.total_size));
    return cards;
  }

  // --------------------------------------------------------------------
  // Formatting helpers
  // --------------------------------------------------------------------
  function fmt(v, digits = 2) {
    if (v === null || v === undefined || isNaN(v)) return "—";
    return Number(v).toFixed(digits);
  }

  function fmtPct(v) {
    if (v === null || v === undefined || isNaN(v)) return "—";
    return `${(Number(v) * 100).toFixed(1)}%`;
  }

  function fmtMoney(v, signed = false) {
    if (v === null || v === undefined || isNaN(v)) return "—";
    const n = Number(v);
    const sign = signed && n > 0 ? "+" : "";
    return `${sign}$${shortNum(n)}`;
  }

  function shortNum(n) {
    const abs = Math.abs(n);
    if (abs >= 1e9) return (n / 1e9).toFixed(2) + "B";
    if (abs >= 1e6) return (n / 1e6).toFixed(2) + "M";
    if (abs >= 1e3) return (n / 1e3).toFixed(1) + "k";
    return n.toFixed(0);
  }

  function scoreColor(score) {
    if (score === null || score === undefined || isNaN(score)) return "var(--text)";
    // muted pink (0) -> dim white (50) -> phosphor green (100).
    const t = Math.max(0, Math.min(1, score / 100));
    // Two-stop gradient through a cool-white midpoint.
    const mid = { r: 0xe8, g: 0xec, b: 0xee };
    const lo  = { r: 0xff, g: 0x6b, b: 0x83 };
    const hi  = { r: 0x4a, g: 0xff, b: 0x8e };
    let r, g, b;
    if (t < 0.5) {
      const u = t / 0.5;
      r = lo.r + (mid.r - lo.r) * u;
      g = lo.g + (mid.g - lo.g) * u;
      b = lo.b + (mid.b - lo.b) * u;
    } else {
      const u = (t - 0.5) / 0.5;
      r = mid.r + (hi.r - mid.r) * u;
      g = mid.g + (hi.g - mid.g) * u;
      b = mid.b + (hi.b - mid.b) * u;
    }
    return `rgb(${Math.round(r)},${Math.round(g)},${Math.round(b)})`;
  }

  function signClass(v) {
    if (v === null || v === undefined || isNaN(v)) return "muted";
    if (v > 0) return "pos";
    if (v < 0) return "neg";
    return "muted";
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]
    );
  }

  function relTimeAgo(ts) {
    const d = parseDate(ts);
    if (!d) return null;
    const diff = Date.now() - d.getTime();
    if (diff < 0) return "just now";
    return humanDuration(diff) + " ago";
  }

  function relTimeUntil(ts) {
    const d = parseDate(ts);
    if (!d) return null;
    const diff = d.getTime() - Date.now();
    if (diff <= 0) return { label: "Closed", past: true };
    return { label: `Closes in ${humanDuration(diff)}`, past: false };
  }

  function parseDate(ts) {
    if (!ts) return null;
    const d = typeof ts === "number"
      ? new Date(ts > 1e12 ? ts : ts * 1000)
      : new Date(ts);
    return isNaN(d.getTime()) ? null : d;
  }

  function humanDuration(ms) {
    const mins = Math.round(ms / 60000);
    if (mins < 60)  return `${mins}m`;
    const hrs = Math.round(mins / 60);
    if (hrs < 48)   return `${hrs}h`;
    const days = Math.round(hrs / 24);
    if (days < 60)  return `${days}d`;
    const months = Math.round(days / 30);
    if (months < 24) return `${months}mo`;
    return `${Math.round(months / 12)}y`;
  }

  function formatWhen(ts) {
    if (!ts) return "—";
    let d;
    if (typeof ts === "number") {
      d = new Date(ts > 1e12 ? ts : ts * 1000);
    } else {
      d = new Date(ts);
    }
    if (isNaN(d.getTime())) return String(ts);
    const diff = Date.now() - d.getTime();
    const mins = Math.round(diff / 60000);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.round(mins / 60);
    if (hrs < 48) return `${hrs}h ago`;
    const days = Math.round(hrs / 24);
    return `${days}d ago`;
  }

  function setLastUpdated(meta) {
    const el = document.getElementById("last-updated");
    const ts = meta && (meta.updated_at || meta.timestamp);
    if (!ts) { el.textContent = "unknown"; return; }
    const d = new Date(ts);
    if (isNaN(d.getTime())) { el.textContent = ts; return; }
    el.textContent = d.toLocaleString();
  }

  function tickCountdown() {
    const remain = state.refreshDeadline - Date.now();
    const el = document.getElementById("refresh-countdown");
    if (remain <= 0) { el.textContent = "now…"; return; }
    const h = Math.floor(remain / 3_600_000);
    const m = Math.floor((remain % 3_600_000) / 60_000);
    const s = Math.floor((remain % 60_000) / 1000);
    el.textContent = `${pad(h)}:${pad(m)}:${pad(s)}`;
  }
  function pad(n) { return String(n).padStart(2, "0"); }
})();
