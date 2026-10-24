/* HireSignal static dashboard. Reads data/hiresignal.json written by `make export-site`. */
(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const NF = new Intl.NumberFormat("en-GB");
  const fmt = (n) => (n == null ? "–" : NF.format(Math.round(n)));
  const usd = (n) => (n == null ? "–" : "$" + NF.format(Math.round(n / 1000)) + "k");
  const pct = (n) => (n == null ? "–" : (n >= 10 ? Math.round(n) : Math.round(n * 10) / 10) + "%");
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const safeUrl = (u) => { try { const x = new URL(u); return /^https?:$/.test(x.protocol) ? x.href : null; } catch { return null; } };
  const day = (iso, opts = { day: "numeric", month: "short", year: "numeric" }) =>
    iso ? new Date(iso.length === 10 ? iso + "T00:00:00Z" : iso).toLocaleDateString("en-GB", { timeZone: "UTC", ...opts }) : "";
  const monthLabel = (iso) => day(iso, { month: "short" });

  const SOURCE_NAMES = {
    himalayas: "Himalayas", jobicy: "Jobicy", remotive: "Remotive", arbeitnow: "Arbeitnow",
    adzuna: "Adzuna", jsearch: "JSearch", partner_sftp: "the partner SFTP drop (simulated)",
    crm_postgres: "the recruitment CRM (simulated)",
  };
  const LABELS = {
    full_time: "Full time", part_time: "Part time", contract: "Contract", internship: "Internship",
    temporary: "Temporary", unspecified: "Not stated", intern: "Intern", junior: "Junior", mid: "Mid",
    senior: "Senior", lead: "Lead",
  };
  const label = (k) => LABELS[k] || k;
  const MIX_COLOURS = ["#2BC08E", "#1D8A69", "#8FB8AC", "#E3B15C", "#4E7A6E", "#2A5F51", "#B9D9CE"];
  const listJoin = (items) => items.length < 2 ? items.join("") : items.slice(0, -1).join(", ") + " and " + items.at(-1);

  let data, bySkill, selected, category = "All";

  async function load() {
    try {
      const res = await fetch("data/hiresignal.json", { cache: "no-cache" });
      if (!res.ok) throw new Error(res.status);
      data = await res.json();
    } catch {
      $("main").replaceChildren($("#empty-tpl").content.cloneNode(true));
      $("#lede")?.remove();
      return;
    }
    if (!data.skills?.length) { $("main").replaceChildren($("#empty-tpl").content.cloneNode(true)); return; }
    bySkill = new Map(data.skills.map((s) => [s.skill, s]));
    renderHero();
    renderCategories();
    renderRoles();
    renderPay();
    renderMarket();
    renderLatest();
    renderFooter();
    const fromHash = decodeURIComponent((location.hash.match(/skill=([^&]+)/) || [])[1] || "");
    select(bySkill.has(fromHash) ? fromHash : data.skills[0].skill, { scroll: false });
  }

  /* ---------- hero ---------- */
  function renderHero() {
    const k = data.kpis, top = data.skills[0];
    $("#updated").textContent = "Updated " + day(data.generated_at, { day: "numeric", month: "long", year: "numeric" });
    $("#headline").textContent = `${top.skill} is in ${pct(top.pct)} of tech job postings.`;
    const boards = data.sources.filter((s) => !/simulated/.test(SOURCE_NAMES[s.source] || "")).length;
    $("#lede").textContent =
      `Counted across ${fmt(k.jobs)} postings from ${fmt(k.companies)} companies on ` +
      `${boards} public job board${boards === 1 ? "" : "s"}, posted between ${day(k.first_posted)} and ${day(k.last_posted)}. ` +
      `${fmt(k.skills)} technologies are tracked.`;

    const shown = data.skills.slice(0, window.innerWidth < 620 ? 36 : 72);
    const max = shown[0].pct || 1;
    const wrap = $("#spectrum");
    wrap.replaceChildren(...shown.map((s, i) => {
      const b = document.createElement("button");
      b.className = "bar" + (i < 3 ? " edge-l" : i > shown.length - 4 ? " edge-r" : "");
      b.type = "button";
      b.setAttribute("role", "option");
      b.dataset.skill = s.skill;
      b.style.setProperty("--h", Math.max(1.5, (s.pct / max) * 100) + "%");
      b.style.setProperty("--i", i);
      b.setAttribute("aria-label", `${s.skill}, ${pct(s.pct)} of tech postings`);
      b.innerHTML = `<i></i><span class="tip">${esc(s.skill)} ${pct(s.pct)}</span>`;
      b.addEventListener("click", () => select(s.skill, { scroll: true }));
      return b;
    }));
    wrap.addEventListener("keydown", (e) => {
      if (!["ArrowLeft", "ArrowRight"].includes(e.key)) return;
      const bars = [...wrap.children];
      const i = bars.indexOf(document.activeElement);
      const next = bars[Math.min(bars.length - 1, Math.max(0, i + (e.key === "ArrowRight" ? 1 : -1)))];
      if (next) { e.preventDefault(); next.focus(); select(next.dataset.skill, { scroll: false }); }
    });
    if (!matchMedia("(prefers-reduced-motion: reduce)").matches) wrap.classList.add("tune");
  }

  /* ---------- explorer ---------- */
  function renderCategories() {
    const counts = new Map();
    data.skills.forEach((s) => counts.set(s.category, (counts.get(s.category) || 0) + 1));
    const cats = ["All", ...[...counts.keys()].filter(Boolean).sort((a, b) => counts.get(b) - counts.get(a))];
    $("#categories").replaceChildren(...cats.map((c) => {
      const b = document.createElement("button");
      b.type = "button"; b.className = "chip"; b.textContent = c;
      b.setAttribute("aria-pressed", String(c === category));
      b.addEventListener("click", () => { category = c; renderCategories(); renderList(); });
      return b;
    }));
    renderList();
  }

  function renderList() {
    const q = $("#search").value.trim().toLowerCase();
    const items = data.skills.filter((s) =>
      (category === "All" || s.category === category) && (!q || s.skill.toLowerCase().includes(q)));
    const list = $("#skill-list");
    if (!items.length) {
      list.innerHTML = `<li class="none">No skill matches “${esc(q)}”. Try a shorter name or another category.</li>`;
      return;
    }
    list.innerHTML = items.slice(0, 200).map((s) => `
      <li><button type="button" data-skill="${esc(s.skill)}" ${s.skill === selected ? 'aria-current="true"' : ""}>
        <span class="rank">${s.rank}</span><span>${esc(s.skill)}</span><span class="pct">${pct(s.pct)}</span>
      </button></li>`).join("");
  }
  $("#search").addEventListener("input", renderList);
  $("#skill-list").addEventListener("click", (e) => {
    const b = e.target.closest("button[data-skill]");
    if (b) select(b.dataset.skill, { scroll: false });
  });

  function select(skill, { scroll }) {
    const s = bySkill.get(skill);
    if (!s) return;
    selected = skill;
    document.querySelectorAll(".bar").forEach((b) => b.setAttribute("aria-selected", String(b.dataset.skill === skill)));
    document.querySelectorAll("#skill-list button").forEach((b) =>
      b.dataset.skill === skill ? b.setAttribute("aria-current", "true") : b.removeAttribute("aria-current"));
    history.replaceState(null, "", "#skill=" + encodeURIComponent(skill));
    renderDetail(s);
    if (scroll) $("#explorer").scrollIntoView({ behavior: matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth" });
  }

  function renderDetail(s) {
    const remote = s.jobs ? (100 * s.remote_jobs) / s.jobs : null;
    const max = Math.max(...s.related.map((r) => r.pct), 1);
    const pairs = s.related.length
      ? `<ul class="pairs">${s.related.map((r) => `
          <li><button type="button" data-skill="${esc(r.skill)}" ${bySkill.has(r.skill) ? "" : "disabled"}>
            <span class="name">${esc(r.skill)}</span>
            <span class="track"><i style="--w:${(r.pct / max) * 100}%"></i></span>
            <span class="v">${pct(r.pct)}</span>
          </button></li>`).join("")}</ul>
         <p class="hint">Share of ${esc(s.skill)} postings that also ask for the skill.</p>`
      : `<p class="hint">Not enough postings yet to say what ${esc(s.skill)} is paired with.</p>`;
    const salary = s.median_salary_usd != null && s.salary_n >= 5
      ? `${usd(s.median_salary_usd)} <small>from ${fmt(s.salary_n)} salaries</small>`
      : `– <small>too few salaries stated</small>`;

    $("#detail").innerHTML = `
      <p class="cat">${esc(s.category || "Uncategorised")}</p>
      <h3>${esc(s.skill)}</h3>
      <p class="said">Asked for in <b>${pct(s.pct)}</b> of tech postings: <b>${fmt(s.jobs)}</b> in total,
        ${fmt(s.jobs_30d)} of them in the last 30 days, at ${fmt(s.companies)} companies.</p>
      <dl class="facts">
        <div><dt>Demand rank</dt><dd>${s.rank} <small>of ${fmt(data.skills.length)}</small></dd></div>
        <div><dt>Postings</dt><dd>${fmt(s.jobs)}</dd></div>
        <div><dt>Remote</dt><dd>${pct(remote)}</dd></div>
        <div><dt>Median salary</dt><dd class="money">${salary}</dd></div>
      </dl>
      <div class="detail-grid">
        <div class="trend"><h4>Postings per month</h4>${trendSvg(s)}
          <p class="hint">Postings are counted in the month they were published. Early months only include jobs still listed when collection started.</p></div>
        <div><h4>Often asked for with</h4>${pairs}</div>
      </div>`;
    $("#detail").querySelectorAll(".pairs button").forEach((b) =>
      b.addEventListener("click", () => select(b.dataset.skill, { scroll: false })));
  }

  function trendSvg(s) {
    const months = data.months, v = s.trend;
    if (months.length < 2) return `<p class="hint">One month of data so far.</p>`;
    const W = 320, H = 110, top = 14, bottom = 92, max = Math.max(...v, 1);
    const x = (i) => (i / (months.length - 1)) * W;
    const y = (n) => bottom - (n / max) * (bottom - top);
    const pts = v.map((n, i) => `${x(i).toFixed(1)},${y(n).toFixed(1)}`);
    const every = Math.ceil(months.length / 6);
    return `<svg viewBox="0 -4 ${W} ${H + 8}" preserveAspectRatio="none" role="img"
        aria-label="${esc(s.skill)} postings per month: ${months.map((m, i) => monthLabel(m) + " " + v[i]).join(", ")}">
      <line class="base" x1="0" x2="${W}" y1="${bottom}" y2="${bottom}"/>
      <path class="area" d="M0,${bottom} L${pts.join(" L")} L${W},${bottom} Z"/>
      <path class="line" d="M${pts.join(" L")}"/>
      <circle class="dot" cx="${x(v.length - 1)}" cy="${y(v.at(-1))}" r="4"/>
      <text class="axis" x="${x(v.length - 1)}" y="${y(v.at(-1)) - 9}" text-anchor="end">${fmt(v.at(-1))}</text>
      ${months.map((m, i) => i % every && i !== months.length - 1 ? "" :
        `<text class="axis" x="${x(i)}" y="${H}" text-anchor="${i === 0 ? "start" : i === months.length - 1 ? "end" : "middle"}">${monthLabel(m)}</text>`).join("")}
    </svg>`;
  }

  /* ---------- roles ---------- */
  function renderRoles() {
    const named = data.families.filter((f) => f.family && f.family !== "Other" && f.skills.length);
    const other = data.families.find((f) => f.family === "Other");
    if (!named.length) { $("#family-panel").innerHTML = `<p class="hint">No role families in this snapshot.</p>`; return; }
    const tabs = $("#family-tabs");
    tabs.replaceChildren(...named.map((f, i) => {
      const b = document.createElement("button");
      b.type = "button"; b.className = "tab"; b.setAttribute("role", "tab"); b.id = "tab-" + i;
      b.innerHTML = `${esc(f.family)}<small>${fmt(f.jobs)}</small>`;
      b.addEventListener("click", () => show(i));
      b.addEventListener("keydown", (e) => {
        if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
          const n = (i + (e.key === "ArrowRight" ? 1 : -1) + named.length) % named.length;
          show(n); tabs.children[n].focus();
        }
      });
      return b;
    }));
    function show(i) {
      [...tabs.children].forEach((t, j) => { t.setAttribute("aria-selected", String(i === j)); t.tabIndex = i === j ? 0 : -1; });
      const f = named[i];
      const max = Math.max(...f.skills.map((s) => s.pct), 1);
      const panel = $("#family-panel");
      panel.setAttribute("aria-labelledby", "tab-" + i);
      panel.innerHTML = `<ol class="bars">${f.skills.map((s) => `
        <li><span class="name">${esc(s.skill)}</span>
          <span class="track"><i style="--w:${(s.pct / max) * 100}%"></i></span>
          <span class="v">${pct(s.pct)}</span></li>`).join("")}</ol>
        <p class="family-foot">${fmt(f.jobs)} ${esc(f.family)} postings.${other ? ` Another ${fmt(other.jobs)} postings did not match a named role.` : ""}</p>`;
    }
    show(0);
  }

  /* ---------- salaries ---------- */
  function renderPay() {
    const draw = () => {
      const by = $("#pay-sort").value;
      // Prefer skills with at least 30 stated salaries so a handful of postings cannot top the list.
      const solid = data.salaries.filter((r) => r.n >= 30);
      const pool = solid.length >= 8 ? solid : data.salaries;
      $("#pay-min").textContent = pool === solid ? "Skills with at least 30 stated salaries." : "Skills with at least 5 stated salaries.";
      const rows = [...pool].sort((a, b) => b[by] - a[by] || b.median - a.median).slice(0, 15);
      if (!rows.length) { $("#pay").innerHTML = `<p class="hint">Not enough stated salaries yet.</p>`; return; }
      const lo = Math.min(...rows.map((r) => r.p25)), hi = Math.max(...rows.map((r) => r.p75));
      const min = Math.floor(lo / 20000) * 20000, max = Math.ceil(hi / 20000) * 20000;
      const at = (n) => ((n - min) / (max - min)) * 100;
      $("#pay").innerHTML = rows.map((r) => `
        <span class="name">${esc(r.skill)}</span>
        <span class="range" role="img" aria-label="${esc(r.skill)}: median ${usd(r.median)}, middle half ${usd(r.p25)} to ${usd(r.p75)}">
          <span class="iqr" style="left:${at(r.p25)}%;width:${Math.max(0.6, at(r.p75) - at(r.p25))}%"></span>
          <span class="med" style="left:${at(r.median)}%"></span>
        </span>
        <span class="v">${usd(r.median)}<small>${fmt(r.n)} salaries</small></span>`).join("") +
        `<div class="pay-axis"><span>${usd(min)}</span><span>${usd((min + max) / 2)}</span><span>${usd(max)}</span></div>`;
    };
    $("#pay-sort").addEventListener("change", draw);
    draw();
  }

  /* ---------- market ---------- */
  function renderMarket() {
    $("#companies tbody").innerHTML = data.companies.slice(0, 15).map((c) => `
      <tr><td>${esc(c.company)}</td><td class="num">${fmt(c.jobs)}</td>
      <td class="asks">${esc(c.top_skills.join(", "))}</td></tr>`).join("");

    const k = data.kpis;
    const stack = (title, rows) => {
      const total = rows.reduce((a, r) => a + r.jobs, 0) || 1;
      const shown = rows.slice(0, 6);
      const rest = total - shown.reduce((a, r) => a + r.jobs, 0);
      if (rest > 0) shown.push({ label: "Other", jobs: rest });
      return `<section><h3>${title}</h3>
        <div class="stack" role="img" aria-label="${esc(title)}: ${shown.map((r) => `${esc(label(r.label))} ${pct((100 * r.jobs) / total)}`).join(", ")}">
          ${shown.map((r, i) => `<i style="width:${(100 * r.jobs) / total}%;background:${MIX_COLOURS[i % MIX_COLOURS.length]}"></i>`).join("")}</div>
        <ul class="legend">${shown.map((r, i) => `
          <li><span><i class="sw" style="background:${MIX_COLOURS[i % MIX_COLOURS.length]}"></i>${esc(label(r.label))}</span></li>
          <li class="v">${pct((100 * r.jobs) / total)}</li>`).join("")}</ul></section>`;
    };
    $("#mix").innerHTML =
      stack("Remote or on site", [{ label: "Remote", jobs: k.remote_jobs }, { label: "On site or hybrid", jobs: k.jobs - k.remote_jobs }]) +
      stack("Seniority", data.mix.seniority) +
      stack("Contract type", data.mix.employment);
  }

  /* ---------- latest ---------- */
  function renderLatest() {
    const items = data.latest.slice(0, 12).map((j) => {
      const url = safeUrl(j.url);
      const pay = j.salary_usd_min && !j.salary_is_predicted
        ? `<span class="tag money">${usd(j.salary_usd_min)}${j.salary_usd_max && j.salary_usd_max !== j.salary_usd_min ? "–" + usd(j.salary_usd_max) : ""}</span>` : "";
      const skills = (j.skills || []).slice(0, 4).map((s) => `<span class="tag">${esc(s)}</span>`).join("");
      return `<li>
        ${url ? `<a href="${esc(url)}" target="_blank" rel="noopener nofollow">${esc(j.title)}</a>` : `<b>${esc(j.title)}</b>`}
        <p class="who">${esc(j.company)}${j.location ? ", " + esc(j.location) : ""}. Posted ${day(j.posted_date, { day: "numeric", month: "short" })} on ${esc(SOURCE_NAMES[j.source] || j.source)}</p>
        <div class="meta">${j.remote ? `<span class="tag remote">Remote</span>` : ""}${pay}${skills}</div>
      </li>`;
    });
    $("#latest").innerHTML = items.join("") || `<li class="who">No postings in this snapshot.</li>`;
  }

  function renderFooter() {
    const names = data.sources.map((s) => SOURCE_NAMES[s.source] || s.source);
    $("#foot-sources").textContent =
      `Postings come from ${listJoin(names)}. Every posting links back to the board that published it. ` +
      (data.include_simulated ? "This snapshot includes the project's simulated partner and CRM feeds, whose companies are fictional."
        : "The project's simulated partner and CRM feeds are left out, so every number here describes real postings.") +
      ` Salaries are converted to US dollars with fixed exchange rates.`;
  }

  load();
})();
