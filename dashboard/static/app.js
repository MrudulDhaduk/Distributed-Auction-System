/* Auction cluster dashboard. Polls /api/state 4x a second and animates what
   changed. Every number on the page comes from the cluster: heartbeat pulses
   are drawn only when a node's real AppendEntries counter goes up. */
"use strict";

const POLL_MS = 250;
const MAX_BID = 1e12;
const S = {
  session: safeGet("dash.session"),
  me: null,
  data: null,
  prevStatus: {},        // node_id -> last status (for deltas)
  pollAt: 0,             // performance.now() of the last good poll
  clockOffset: 0,        // server now - browser now (µs)
  filter: "all",
  chats: loadJSON("dash.chats", {}),
  firstSeen: loadJSON("dash.firstSeen", {}),
  seenEvents: new Set(),
  seenUsers: null,
  auctions: new Map(),   // auction_id -> card refs
  nodeCards: new Map(),
  topo: null,
};

/* ---------- small helpers ---------- */
function safeGet(k) { try { return sessionStorage.getItem(k); } catch { return null; } }
function safeSet(k, v) { try { v == null ? sessionStorage.removeItem(k) : sessionStorage.setItem(k, v); } catch {} }
function loadJSON(k, d) { try { return JSON.parse(sessionStorage.getItem(k)) || d; } catch { return d; } }
function saveJSON(k, v) { try { sessionStorage.setItem(k, JSON.stringify(v)); } catch {} }
const $ = (sel, root = document) => root.querySelector(sel);
function el(tag, cls, text) { const e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }
function svg(tag, attrs = {}, parent) {
  const e = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
}
const fmt = n => Number(n).toLocaleString("en-US");
function nowUs() { return Date.now() * 1000 + S.clockOffset; }
function hue(name) { let h = 0; for (const c of name) h = (h * 31 + c.charCodeAt(0)) % 360; return h; }
function avatar(name, cls = "avatar") {
  const a = el("span", cls, name.slice(0, 1).toUpperCase());
  a.style.background = `hsl(${hue(name)} 55% 45%)`;
  return a;
}
function clockTime(us) { return new Date(us / 1000).toLocaleTimeString([], { hour12: false }); }
function dur(ms) {
  if (ms < 0) ms = 0;
  const s = Math.ceil(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60), r = s % 60;
  if (m < 60) return `${m}m ${String(r).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${String(m % 60).padStart(2, "0")}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

function toast(msg, kind = "") {
  const t = el("div", `toast ${kind}`, msg);
  $("#toasts").appendChild(t);
  setTimeout(() => { t.classList.add("out"); setTimeout(() => t.remove(), 300); }, kind === "err" ? 6000 : 3500);
}

async function api(path, body) {
  const headers = { "Content-Type": "application/json" };
  if (S.session) headers["X-Session"] = S.session;
  let r;
  try {
    r = await fetch(path, { method: "POST", headers, body: JSON.stringify(body || {}) });
  } catch {
    throw Object.assign(new Error("The dashboard server is not reachable"), { field: null });
  }
  const j = await r.json().catch(() => ({}));
  if (!r.ok) {
    if (r.status === 401 && S.session) setSession(null, null);
    throw Object.assign(new Error(j.error || `HTTP ${r.status}`), { field: j.field || null });
  }
  return j;
}

async function withBusy(btn, fn) {
  if (btn.classList.contains("busy")) return;
  btn.classList.add("busy"); btn.disabled = true;
  try { return await fn(); } finally { btn.classList.remove("busy"); btn.disabled = false; }
}

function setSession(sid, me) {
  S.session = sid; S.me = me;
  safeSet("dash.session", sid);
  renderAccount();
  for (const ref of S.auctions.values()) ref.dirty = true;
}

/* ---------- polling ---------- */
async function poll() {
  try {
    const sid = S.session;
    const headers = sid ? { "X-Session": sid } : {};
    const r = await fetch("/api/state", { headers, cache: "no-store" });
    const d = await r.json();
    S.clockOffset = d.now - Date.now() * 1000;
    // Only trust "me" if the session did not change while this poll was in flight.
    if (sid === S.session) {
      if (sid && !d.me) { setSession(null, null); toast("Your dashboard session ended (was the dashboard restarted?). Log in again.", "err"); }
      else if (d.me !== S.me) { S.me = d.me; renderAccount(); for (const ref of S.auctions.values()) ref.dirty = true; }
    } else d.me = S.me;
    const pc = $("#pill-conn"); pc.textContent = "live"; pc.className = "pill ok";
    render(d);
  } catch (e) {
    const pc = $("#pill-conn"); pc.textContent = "dashboard offline"; pc.className = "pill bad";
  }
  setTimeout(poll, POLL_MS);
}

function render(d) {
  const first = !S.data;
  S.data = d;
  renderPills(d);
  renderTopology(d);
  renderNodes(d);
  detectHeartbeats(d);
  renderAuctions(d);
  renderUsers(d);
  renderEvents(d, first);
  S.pollAt = performance.now();
  for (const n of d.nodes) S.prevStatus[n.node_id] = n.status ? { ...n.status, process: n.process } : { process: n.process };
}

/* ---------- header pills ---------- */
function verdict(d) {
  const up = d.nodes.map(n => n.status).filter(Boolean);
  if (!up.length) return ["no node answering", "bad"];
  const idx = new Set(up.map(s => s.digest_index));
  if (idx.size > 1) return ["catching up", "warn"];
  const dig = new Set(up.map(s => s.state_digest));
  if (dig.size > 1) return ["DIVERGED", "bad"];
  return [`${up.length}/${d.nodes.length} in sync`, up.length === d.nodes.length ? "ok" : "warn"];
}
function renderPills(d) {
  const L = $("#pill-leader");
  L.textContent = d.leader ? `leader: ${d.leader}` : "no leader";
  L.className = "pill " + (d.leader ? "gold" : "bad");
  const terms = d.nodes.map(n => n.status ? n.status.term : 0);
  $("#pill-term").textContent = `term ${Math.max(0, ...terms)}`;
  const [v, cls] = verdict(d);
  const P = $("#pill-sync"); P.textContent = v; P.className = "pill " + cls;
  const llm = $("#pill-llm");
  const label = { ready: "LLM ready", starting: "LLM starting", down: "LLM down", off: "LLM off" }[d.llm] || d.llm;
  llm.textContent = label + (d.llm_mode === "fake" ? " (fake)" : "");
  llm.className = "pill " + ({ ready: "ok", starting: "warn", down: "bad" }[d.llm] || "pill-dim");
}

/* ---------- topology (SVG) ---------- */
const POS3 = [[320, 80], [130, 280], [510, 280]];
const R = { core: 44, ring: 54, halo: 60, glow: 66 };
const CIRC = 2 * Math.PI * R.ring;

function buildTopology(nodes) {
  const root = $("#topology");
  root.innerHTML = "";
  const pos = {};
  nodes.forEach((n, i) => { pos[n.node_id] = POS3[i] || [100 + i * 150, 170]; });
  const edges = svg("g", {}, root);
  const edgeEls = {};
  const ids = nodes.map(n => n.node_id);
  for (let i = 0; i < ids.length; i++) for (let j = i + 1; j < ids.length; j++) {
    const [a, b] = [ids[i], ids[j]];
    edgeEls[`${a}|${b}`] = edgeEls[`${b}|${a}`] = svg("line", {
      class: "edge", x1: pos[a][0], y1: pos[a][1], x2: pos[b][0], y2: pos[b][1] }, edges);
  }
  const pulses = svg("g", {}, root);
  const groups = {};
  for (const id of ids) {
    const [x, y] = pos[id];
    const g = svg("g", { class: "node-g unknown", transform: `translate(${x} ${y})` }, root);
    svg("circle", { class: "timer-track", r: R.ring }, g);
    const ring = svg("circle", { class: "timer-ring", r: R.ring, transform: "rotate(-90)",
      "stroke-dasharray": `${CIRC} ${CIRC}`, "stroke-dashoffset": 0 }, g);
    const body = svg("g", {}, g);
    svg("circle", { class: "node-core", r: R.core }, body);
    const label = svg("text", { class: "node-label", y: -6 }, body); label.textContent = id;
    const role = svg("text", { class: "node-role", y: 16 }, body);
    svg("line", { class: "cross", x1: -22, y1: -22, x2: 22, y2: 22 }, g);
    svg("line", { class: "cross", x1: 22, y1: -22, x2: -22, y2: 22 }, g);
    const meta = svg("text", { class: "node-meta", y: R.halo + 20 }, g);
    const meta2 = svg("text", { class: "node-meta", y: R.halo + 34 }, g);
    groups[id] = { g, ring, body, role, meta, meta2 };
  }
  S.topo = { pos, edgeEls, pulses, groups, key: ids.join(",") };
}

function nodeClass(n) {
  if (n.process === "dead") return "dead";
  if (n.process === "suspended") return "suspended";
  return n.status ? n.status.role : "unknown";
}

function renderTopology(d) {
  if (!S.topo || S.topo.key !== d.nodes.map(n => n.node_id).join(",")) buildTopology(d.nodes);
  const T = S.topo;
  for (const n of d.nodes) {
    const G = T.groups[n.node_id], s = n.status, cls = nodeClass(n);
    G.g.setAttribute("class", `node-g ${cls}`);
    G.role.textContent = cls === "dead" ? "killed" : cls === "suspended" ? "frozen" : cls === "unknown" ? "no answer" : cls.toLowerCase();
    G.meta.textContent = s ? `term ${s.term} · log ${s.last_log_index}` : (n.process === "dead" ? "process dead" : "not answering");
    G.meta2.textContent = s ? `commit ${s.commit_index} · applied ${s.last_applied}` : "";
    const prev = S.prevStatus[n.node_id];
    if (s && s.role === "CANDIDATE" && (!prev || prev.role !== "CANDIDATE")) electionRipple(n.node_id, d);
  }
  for (const [k, line] of Object.entries(T.edgeEls)) {
    const [a, b] = k.split("|");
    const na = d.nodes.find(n => n.node_id === a), nb = d.nodes.find(n => n.node_id === b);
    const alive = n => n && n.status && n.process === "up";
    const live = d.leader && (a === d.leader || b === d.leader) && alive(na) && alive(nb);
    line.setAttribute("class", "edge" + (live ? " live" : "") + (!alive(na) || !alive(nb) ? " cut" : ""));
  }
  const up = d.nodes.filter(n => n.status).length, need = Math.floor(d.nodes.length / 2) + 1;
  const q = $("#quorum");
  q.className = "quorum" + (up < need ? " bad" : "");
  q.innerHTML = "";
  q.append(el("b", null, up < need ? "No quorum" : `${up}/${d.nodes.length} up`),
           el("span", null, up < need ? `need ${need} of ${d.nodes.length}` : `majority ${need}`));
}

function sendPulse(from, to, kind = "", ms = 380, onArrive) {
  const T = S.topo; if (!T || !T.pos[from] || !T.pos[to]) return;
  const [x1, y1] = T.pos[from], [x2, y2] = T.pos[to];
  const ang = Math.atan2(y2 - y1, x2 - x1);
  // start and stop at the ring, offset sideways so pulses and acks use two lanes
  const off = kind === "ack" ? 7 : -7, ox = -Math.sin(ang) * off, oy = Math.cos(ang) * off;
  const sx = x1 + Math.cos(ang) * R.ring + ox, sy = y1 + Math.sin(ang) * R.ring + oy;
  const ex = x2 - Math.cos(ang) * R.ring + ox, ey = y2 - Math.sin(ang) * R.ring + oy;
  const c = svg("circle", { class: `pulse ${kind}`, r: kind === "ack" ? 4 : 5.5, cx: 0, cy: 0 }, T.pulses);
  const a = c.animate([{ transform: `translate(${sx}px, ${sy}px)`, opacity: .2 },
                       { transform: `translate(${(sx + ex) / 2}px, ${(sy + ey) / 2}px)`, opacity: 1, offset: .5 },
                       { transform: `translate(${ex}px, ${ey}px)`, opacity: .9 }],
                      { duration: ms, easing: "ease-in-out" });
  a.onfinish = () => { c.remove(); if (onArrive) onArrive(); };
}

function beat(id) {
  const G = S.topo && S.topo.groups[id]; if (!G) return;
  G.body.classList.remove("beat"); void G.body.getBoundingClientRect(); G.body.classList.add("beat");
}

function electionRipple(id, d) {
  const T = S.topo; if (!T) return;
  const [x, y] = T.pos[id];
  const c = svg("circle", { class: "ripple", cx: x, cy: y, r: R.core }, T.pulses);
  c.animate([{ r: R.core, opacity: .9 }, { r: 150, opacity: 0 }], { duration: 900, easing: "ease-out" }).onfinish = () => c.remove();
  for (const n of d.nodes) if (n.node_id !== id && n.status) sendPulse(id, n.node_id, "vote", 420);
}

function detectHeartbeats(d) {
  const leader = d.leader;
  const interval = POLL_MS;
  const L = leader && d.nodes.find(n => n.node_id === leader);
  for (const n of d.nodes) {
    const s = n.status, prev = S.prevStatus[n.node_id];
    if (!s || !prev || prev.pid !== s.pid) continue;
    if (s.role === "LEADER") {
      const sent = s.heartbeats_sent - (prev.heartbeats_sent || 0);
      for (const p of s.peers) {
        const pp = (prev.peers || []).find(x => x.peer_id === p.peer_id);
        const delta = pp ? p.replies - pp.replies : 0;
        const fol = d.nodes.find(x => x.node_id === p.peer_id);
        if (delta > 0 && fol && fol.status && fol.process === "up") {
          const k = Math.min(delta, 2);
          for (let i = 0; i < k; i++) setTimeout(() => sendPulse(p.peer_id, n.node_id, "ack", 340), 380 + i * interval / k);
        }
      }
    } else {
      const got = s.heartbeats_received - (prev.heartbeats_received || 0);
      if (got > 0 && L && L.status && L.process === "up" && s.leader_id === leader) {
        const k = Math.min(got, 3);
        for (let i = 0; i < k; i++) setTimeout(() => sendPulse(leader, n.node_id, "", 380, () => beat(n.node_id)), i * interval / k);
      }
    }
  }
}

/* ---------- node cards ---------- */
const DIGEST_COLORS = ["#a46bff", "#4aa3ff", "#ff5d6c", "#3ddc97"];
function buildNodeCard(n) {
  const c = el("div", "ncard");
  c.innerHTML = `
    <div class="ncard-head"><span class="ncard-id"></span><span class="badge role"></span><span class="badge proc"></span><span class="spacer"></span><span class="pid"></span></div>
    <div class="stats">
      <div><span>term</span><span data-k="term"></span></div><div><span>voted for</span><span data-k="voted"></span></div>
      <div><span>leader</span><span data-k="leader"></span></div><div><span>commit</span><span data-k="commit"></span></div>
      <div><span>last log</span><span data-k="log"></span></div><div><span>applied</span><span data-k="applied"></span></div>
      <div style="grid-column:1/-1"><span>state digest</span><span data-k="digest"></span></div>
    </div>
    <div class="hb-row"><span><i class="hb-dot"></i><span data-k="hbl"></span></span><b data-k="hbv"></b></div>
    <div class="etimer"><i></i></div>
    <div class="hb-row"><span data-k="etl">election timer</span><b data-k="etv"></b></div>
    <div class="peers"></div>
    <div class="actions">
      <button class="btn small danger" data-act="kill">Kill -9</button>
      <button class="btn small" data-act="suspend">Suspend</button>
      <button class="btn small" data-act="resume">Resume</button>
      <button class="btn small" data-act="restart">Restart</button>
    </div>`;
  $(".ncard-id", c).textContent = n.node_id;
  c.querySelectorAll("[data-act]").forEach(b => b.addEventListener("click", () => withBusy(b, async () => {
    try { const r = await api(`/api/nodes/${n.node_id}/${b.dataset.act}`); toast(r.message, "ok"); }
    catch (e) { toast(e.message, "err"); }
  })));
  const ref = { c, k: {}, dot: $(".hb-dot", c), bar: $(".etimer i", c), peers: $(".peers", c),
                role: $(".role", c), proc: $(".proc", c), pid: $(".pid", c), btn: {} };
  c.querySelectorAll("[data-k]").forEach(e => ref.k[e.dataset.k] = e);
  c.querySelectorAll("[data-act]").forEach(b => ref.btn[b.dataset.act] = b);
  return ref;
}

function renderNodes(d) {
  const box = $("#nodes");
  const digests = {};
  for (const n of d.nodes) {
    let ref = S.nodeCards.get(n.node_id);
    if (!ref) { ref = buildNodeCard(n); S.nodeCards.set(n.node_id, ref); box.appendChild(ref.c); }
    const s = n.status, cls = nodeClass(n);
    ref.c.className = `ncard ${cls}`;
    ref.role.textContent = s ? s.role : "—"; ref.role.className = `badge role ${s ? s.role : ""}`;
    ref.proc.textContent = n.process; ref.proc.className = `badge proc ${n.process}`;
    ref.pid.textContent = s ? `pid ${s.pid} · start #${n.starts}` : `ports ${n.client_port}/${n.peer_port}`;
    const K = ref.k;
    K.term.textContent = s ? s.term : "—"; K.voted.textContent = s ? (s.voted_for || "—") : "—";
    K.leader.textContent = s ? (s.leader_id || "?") : "—"; K.commit.textContent = s ? s.commit_index : "—";
    K.log.textContent = s ? `${s.last_log_index} (t${s.last_log_term})` : "—"; K.applied.textContent = s ? s.last_applied : "—";
    if (s) {
      if (!(s.state_digest in digests)) digests[s.state_digest] = DIGEST_COLORS[Object.keys(digests).length % DIGEST_COLORS.length];
      K.digest.innerHTML = "";
      const dg = el("span", "digest", `${s.state_digest.slice(0, 10)} @${s.digest_index}`);
      dg.style.background = digests[s.state_digest] + "33"; dg.style.color = digests[s.state_digest];
      K.digest.appendChild(dg);
    } else K.digest.textContent = "—";
    if (s && s.role === "LEADER") {
      K.hbl.textContent = `heartbeats sent (every ${s.heartbeat_interval_ms} ms)`;
      K.hbv.textContent = fmt(s.heartbeats_sent);
      K.etl.textContent = "election timer"; K.etv.textContent = "none (leader)";
      ref.dot.className = "hb-dot on";
      ref.peers.innerHTML = "";
      for (const p of s.peers) {
        const row = el("div");
        const age = p.last_reply_age_ms < 0 ? "no reply yet" : `${p.last_reply_age_ms} ms ago`;
        row.append(el("span", null, `→ ${p.peer_id}: match ${p.match_index}, next ${p.next_index}`), el("b", null, age));
        if (p.last_reply_age_ms < 0 || p.last_reply_age_ms > 1000) row.lastChild.style.color = "var(--bad)";
        ref.peers.appendChild(row);
      }
    } else if (s) {
      K.hbl.textContent = "heartbeats received";
      K.hbv.textContent = `${fmt(s.heartbeats_received)} · last ${s.last_heartbeat_age_ms < 0 ? "never" : s.last_heartbeat_age_ms + " ms ago"}`;
      K.etl.textContent = `election timer (${s.election_timeout_min_ms}–${s.election_timeout_max_ms} ms)`;
      ref.dot.className = "hb-dot" + (s.last_heartbeat_age_ms >= 0 && s.last_heartbeat_age_ms < 300 ? " on" : "");
      ref.peers.innerHTML = "";
    } else {
      K.hbl.textContent = "heartbeats"; K.hbv.textContent = "—"; K.etv.textContent = "—"; K.etl.textContent = "election timer";
      ref.dot.className = "hb-dot";
      ref.peers.innerHTML = "";
    }
    ref.btn.kill.disabled = n.process === "dead";
    ref.btn.suspend.disabled = n.process !== "up";
    ref.btn.resume.disabled = n.process !== "suspended";
    ref.btn.restart.disabled = n.process !== "dead";
  }
}

/* ---------- per-frame updates: election timers, countdowns ---------- */
let lastCountdown = 0;
function frame(t) {
  const d = S.data;
  if (d && S.topo) {
    const since = t - S.pollAt;
    for (const n of d.nodes) {
      const G = S.topo.groups[n.node_id], s = n.status, ref = S.nodeCards.get(n.node_id);
      let frac = 0, leftMs = 0;
      if (s && s.role !== "LEADER" && n.process === "up") {
        leftMs = Math.max(0, s.election_timer_left_ms - since);
        frac = Math.min(1, leftMs / Math.max(1, s.election_timeout_max_ms));
      } else if (s && s.role === "LEADER") frac = 1;
      if (G) {
        G.ring.setAttribute("stroke-dashoffset", String(CIRC * (1 - frac)));
        G.ring.classList.toggle("low", frac < 0.3 && s && s.role !== "LEADER");
        G.ring.style.stroke = s && s.role === "LEADER" ? "var(--leader)" : "";
        G.ring.style.opacity = !s || n.process !== "up" ? 0 : 1;
      }
      if (ref) {
        if (s && s.role !== "LEADER") {
          ref.bar.style.width = `${frac * 100}%`; ref.bar.classList.toggle("low", frac < 0.3);
          ref.k.etv.textContent = n.process === "up" ? `${Math.round(leftMs)} ms left` : "frozen";
        } else ref.bar.style.width = "0";
      }
    }
  }
  if (t - lastCountdown > 200) { lastCountdown = t; tickCountdowns(); }
  requestAnimationFrame(frame);
}

/* ---------- account card ---------- */
const SEEDED = ["mrudul", "nisarg", "karan", "tanmay"];
function renderAccount() {
  const card = $("#account-card");
  const box = $("#me-box");
  box.innerHTML = "";
  card.innerHTML = "";
  if (S.me) {
    box.append(avatar(S.me), el("span", null, S.me));
    const head = el("div", "card-head"); head.appendChild(el("h2", null, "Account"));
    const row = el("div", "signed");
    const info = el("div");
    info.append(el("div", "signed-name", S.me), el("div", "hint", "Signed in in this tab"));
    const out = el("button", "btn small", "Log out");
    out.addEventListener("click", () => withBusy(out, async () => {
      try { await api("/api/logout"); } catch {}
      setSession(null, null); toast("Logged out", "ok");
    }));
    row.append(avatar(S.me), info);
    card.append(head, row);
    const f = el("div", "quick"); f.appendChild(out); card.appendChild(f);
    return;
  }
  box.appendChild(el("span", "hint", "not signed in"));
  const tabs = el("div", "tabs");
  const tLogin = el("button", "active", "Log in"), tReg = el("button", null, "Create account");
  tabs.append(tLogin, tReg);
  card.appendChild(tabs);
  const form = el("form", "form"); form.noValidate = true; form.autocomplete = "off";
  card.appendChild(form);
  let mode = "login";
  const draw = () => {
    form.innerHTML = `
      <label>Username<input name="username" maxlength="20" autocomplete="off" spellcheck="false"><span class="field-err" data-for="username"></span></label>
      <label>Password<input name="password" type="password" maxlength="64"><span class="field-err" data-for="password"></span></label>
      ${mode === "register" ? `<label>Repeat password<input name="confirm" type="password" maxlength="64"><span class="field-err" data-for="confirm"></span></label>` : ""}
      <button class="btn primary" type="submit">${mode === "login" ? "Log in" : "Create account"}</button>
      <div class="form-note"></div>`;
    if (mode === "login") {
      const q = el("div", "quick"); q.appendChild(el("span", "hint", "test users (password pass):"));
      for (const u of SEEDED) {
        const c = el("button", "chip", u); c.type = "button";
        c.addEventListener("click", () => { form.username.value = u; form.password.value = "pass"; form.requestSubmit(); });
        q.appendChild(c);
      }
      form.appendChild(q);
    }
    $("input", form).focus();
  };
  tLogin.onclick = () => { mode = "login"; tLogin.className = "active"; tReg.className = ""; draw(); };
  tReg.onclick = () => { mode = "register"; tReg.className = "active"; tLogin.className = ""; draw(); };
  form.addEventListener("submit", ev => {
    ev.preventDefault();
    clearErrors(form);
    const username = form.username.value.trim(), password = form.password.value;
    const errs = {};
    if (mode === "register") {
      if (!/^[a-z][a-z0-9_]{2,19}$/.test(username)) errs.username = "3–20 characters: lowercase letters, digits or _, starting with a letter";
      if (password.length < 4 || password.length > 64) errs.password = "4–64 characters";
      else if (password !== password.trim()) errs.password = "No spaces at the start or end";
      if (form.confirm.value !== password) errs.confirm = "The two passwords do not match";
    } else {
      if (!username) errs.username = "Enter a username";
      if (!password) errs.password = "Enter the password";
    }
    if (showErrors(form, errs)) return;
    const btn = $("button[type=submit]", form), note = $(".form-note", form);
    withBusy(btn, async () => {
      try {
        if (mode === "register") {
          await api("/api/register", { username, password, confirm: form.confirm.value });
          toast(`Account ${username} created on the cluster`, "ok");
        }
        const r = await api("/api/login", { username, password });
        setSession(r.session, r.username);
        toast(`Signed in as ${r.username}`, "ok");
      } catch (e) {
        if (e.field && showErrors(form, { [e.field]: e.message })) return;
        note.textContent = e.message; note.className = "form-note err";
      }
    });
  });
  draw();
}

function clearErrors(form) {
  form.querySelectorAll(".field-err").forEach(e => e.textContent = "");
  form.querySelectorAll(".invalid").forEach(e => e.classList.remove("invalid"));
  const n = $(".form-note", form); if (n) { n.textContent = ""; n.className = "form-note"; }
}
function showErrors(form, errs) {
  let any = false;
  for (const [field, msg] of Object.entries(errs)) {
    const slot = form.querySelector(`.field-err[data-for="${field}"]`);
    if (!slot) continue;
    slot.textContent = msg; any = true;
    const input = form.querySelector(`[name="${field}"]`); if (input) input.classList.add("invalid");
  }
  return any;
}

/* ---------- create auction form ---------- */
function addAttrRow(k = "", v = "") {
  const row = el("div", "attr-row");
  row.innerHTML = `<input placeholder="name (e.g. condition)" maxlength="30"><input placeholder="value (e.g. mint)" maxlength="100"><button type="button" class="btn small" title="remove">×</button>`;
  const [ki, vi] = row.querySelectorAll("input"); ki.value = k; vi.value = v;
  $("button", row).onclick = () => row.remove();
  $("#attr-rows").appendChild(row);
}
function setupCreateForm() {
  const form = $("#create-form");
  addAttrRow("condition", ""); addAttrRow("brand", "");
  $("#attr-add").onclick = () => { if ($("#attr-rows").children.length < 8) addAttrRow(); else toast("At most 8 attributes", "err"); };
  form.duration.onchange = () => { $(".custom-duration", form).hidden = form.duration.value !== "custom"; };
  form.addEventListener("submit", ev => {
    ev.preventDefault();
    clearErrors(form);
    const note = $("#create-note");
    if (!S.me) { note.textContent = "Log in first: the seller is whoever is signed in."; note.className = "form-note err"; return; }
    const item = form.item.value.trim();
    const seconds = form.duration.value === "custom" ? Number(form.seconds.value) : Number(form.duration.value);
    const attributes = {}; const errs = {};
    for (const row of $("#attr-rows").children) {
      const [k, v] = [...row.querySelectorAll("input")].map(i => i.value.trim());
      if (!k && !v) continue;
      if (!k || !v) errs.attributes = "Every attribute needs both a name and a value (or leave both empty)";
      else if (k in attributes) errs.attributes = `Attribute "${k}" is given twice`;
      else attributes[k] = v;
    }
    if (!item) errs.item = "Give the item a name";
    else if (item.length > 100) errs.item = "At most 100 characters";
    if (!Number.isInteger(seconds) || seconds < 15 || seconds > 2592000) errs.duration_seconds = "A whole number of seconds from 15 to 2,592,000 (30 days)";
    if (showErrors(form, errs)) return;
    withBusy($("#create-btn"), async () => {
      note.textContent = "Creating…";
      note.className = "form-note";
      try {
        const r = await api("/api/auctions", { item, duration_seconds: seconds, attributes });
        note.textContent = `Created auction #${r.auction_id}`; note.className = "form-note ok";
        toast(`Created auction #${r.auction_id}: ${item}`, "ok");
        form.item.value = "";
      } catch (e) {
        if (e.field && showErrors(form, { [e.field]: e.message })) { note.textContent = ""; return; }
        note.textContent = e.message; note.className = "form-note err";
      }
    });
  });
}

/* ---------- auctions ---------- */
function auctionPhase(a) {
  const left = (a.close_time - nowUs()) / 1000;
  if (a.closed) return ["closed", "CLOSED", left];
  if (left <= 0) return ["closing", "CLOSING…", left];
  if (left <= 10000) return ["ending", "ENDING", left];
  return ["open", "OPEN", left];
}

function buildAuctionCard(a) {
  const c = el("article", "acard");
  c.innerHTML = `
    <div class="a-top">
      <div><div class="a-title"><span class="a-id"></span><span class="a-item"></span></div><div class="a-seller"></div></div>
      <span class="status"></span>
    </div>
    <div class="a-desc"></div>
    <div class="a-price">
      <div><div class="price"></div><div class="high-by"></div></div>
      <div class="countdown"></div>
    </div>
    <div class="bar"><i></i></div>
    <ul class="bids"></ul>
    <div class="a-action"></div>
    <div class="a-msg"></div>
    <div class="a-closed"></div>
    <div class="desc-form">
      <div class="hint">Generate a new description from these attributes</div>
      <div class="attr-row"><input placeholder="name" maxlength="30"><input placeholder="value" maxlength="100"><span></span></div>
      <div class="attr-row"><input placeholder="name" maxlength="30"><input placeholder="value" maxlength="100"><span></span></div>
      <button class="btn small" type="button">Generate</button>
    </div>
    <div class="ai">
      <div class="ai-head"><span>Questions about this auction</span></div>
      <div class="ai-thread"></div>
      <div class="ai-suggest"></div>
      <form class="ai-form" autocomplete="off"><input maxlength="500" placeholder="Ask about this auction…"><button class="btn small primary">Ask</button></form>
    </div>
    <div class="a-foot"><button class="btn small ai-toggle">Ask assistant</button><button class="btn small desc-toggle" hidden>Edit description</button></div>`;
  const ref = {
    c, id: a.auction_id, idEl: $(".a-id", c), item: $(".a-item", c), seller: $(".a-seller", c), status: $(".status", c),
    desc: $(".a-desc", c), price: $(".price", c), highBy: $(".high-by", c), countdown: $(".countdown", c), bar: $(".bar i", c),
    bids: $(".bids", c), action: $(".a-action", c), msg: $(".a-msg", c), closedBox: $(".a-closed", c),
    ai: $(".ai", c), thread: $(".ai-thread", c), aiForm: $(".ai-form", c), suggest: $(".ai-suggest", c),
    descForm: $(".desc-form", c), descToggle: $(".desc-toggle", c),
    lastHigh: null, lastBids: 0, mode: null, dirty: true, a,
  };
  ref.idEl.textContent = `#${a.auction_id}`;
  $(".ai-toggle", c).onclick = () => { ref.ai.classList.toggle("open"); if (ref.ai.classList.contains("open")) { drawThread(ref); $("input", ref.aiForm).focus(); } };
  ref.descToggle.onclick = () => ref.descForm.classList.toggle("open");
  for (const s of ["Am I winning?", "What is the lowest bid I can place?", "Can I bid on this?"]) {
    const chip = el("button", "chip", s); chip.type = "button";
    chip.onclick = () => askAI(ref, s);
    ref.suggest.appendChild(chip);
  }
  ref.aiForm.addEventListener("submit", ev => { ev.preventDefault(); const i = $("input", ref.aiForm); const q = i.value.trim(); if (q) { i.value = ""; askAI(ref, q); } });
  $("button", ref.descForm).onclick = ev => generateDescription(ref, ev.target);
  return ref;
}

function setMsg(ref, text, kind) {
  ref.msg.textContent = text; ref.msg.className = `a-msg ${kind || ""}`;
  clearTimeout(ref.msgTimer);
  if (text) ref.msgTimer = setTimeout(() => { ref.msg.textContent = ""; }, 7000);
}

function drawAction(ref) {
  const a = ref.a, box = ref.action, me = S.me;
  const mode = a.closed ? "closed" : !me ? "anon" : a.owner === me ? "owner" : "bidder";
  if (mode === ref.mode && !ref.dirty) return;
  ref.mode = mode; ref.dirty = false;
  box.innerHTML = "";
  if (mode === "closed") return;
  if (mode === "anon") { box.appendChild(el("div", "hint", "Log in to bid on this auction.")); return; }
  if (mode === "owner") {
    const n = el("div", "owner-note");
    n.append("You can't bid on your own auction. ");
    const tryIt = el("button", "link", "Send a bid anyway");
    tryIt.type = "button";
    tryIt.title = "Skips the dashboard's check and sends the bid to the cluster, to show that the replicated state machine refuses it";
    tryIt.onclick = () => placeBid(ref, ref.a.high_bid + 1, true, tryIt);
    n.append(tryIt);
    box.appendChild(n);
    return;
  }
  const form = el("form", "bid-form"); form.noValidate = true; form.autocomplete = "off";
  form.innerHTML = `<input type="number" inputmode="numeric" min="1" step="1" placeholder="your bid"><button class="btn primary small">Place bid</button>`;
  const input = $("input", form);
  input.value = a.high_bid + 1;
  input.addEventListener("input", () => input.classList.remove("invalid"));
  form.addEventListener("submit", ev => {
    ev.preventDefault();
    const raw = input.value.trim();
    input.classList.remove("invalid");
    const v = Number(raw);
    let err = null;
    if (!raw) err = "Enter an amount";
    else if (!Number.isInteger(v)) err = "Bids are whole numbers";
    else if (v <= 0) err = "A bid must be positive";
    else if (v > MAX_BID) err = `At most ${fmt(MAX_BID)}`;
    else if (v <= ref.a.high_bid) err = `Must be higher than the current high bid of ${fmt(ref.a.high_bid)}`;
    if (err) { input.classList.add("invalid"); setMsg(ref, err, "err"); return; }
    placeBid(ref, v, false, $("button", form));
  });
  const quick = el("div", "bid-quick");
  for (const [label, add] of [["+1", 1], ["+10", 10], ["+50", 50], ["+100", 100]]) {
    const b = el("button", "chip", label); b.type = "button";
    b.onclick = () => { input.value = ref.a.high_bid + add; input.focus(); };
    quick.appendChild(b);
  }
  box.append(form, quick);
  ref.bidInput = input;
}

async function placeBid(ref, amount, skip, btn) {
  await withBusy(btn, async () => {
    try {
      const r = await api("/api/bid", { auction_id: ref.id, amount, skip_precheck: skip });
      setMsg(ref, r.message, "ok");
    } catch (e) { setMsg(ref, e.message, "err"); }
  });
}

function chatKey(ref) { return `${S.me || "-"}:${ref.id}`; }
function drawThread(ref) {
  const msgs = S.chats[chatKey(ref)] || [];
  ref.thread.innerHTML = "";
  if (!msgs.length) ref.thread.appendChild(el("div", "hint", S.me ? "Ask about the rules, the price or the time left." : "Log in to ask questions."));
  for (const m of msgs) {
    const b = el("div", `msg ${m.role}${m.err ? " err" : ""}`);
    if (m.pending) { const t = el("span", "typing"); t.append(el("i"), el("i"), el("i")); b.appendChild(t); }
    else b.textContent = m.text;
    if (m.ms) b.appendChild(el("span", "ms", `${(m.ms / 1000).toFixed(2)} s`));
    ref.thread.appendChild(b);
  }
  ref.thread.scrollTop = ref.thread.scrollHeight;
}
async function askAI(ref, q) {
  if (!S.me) { toast("Log in to ask the assistant", "err"); return; }
  if (q.length > 500) { toast("At most 500 characters", "err"); return; }
  const key = chatKey(ref);
  const msgs = S.chats[key] || (S.chats[key] = []);
  msgs.push({ role: "q", text: q });
  const pending = { role: "a", pending: true };
  msgs.push(pending);
  drawThread(ref);
  try {
    const r = await api("/api/ask", { auction_id: ref.id, question: q });
    Object.assign(pending, { pending: false, text: r.answer, ms: r.ms });
  } catch (e) {
    Object.assign(pending, { pending: false, text: e.message, err: true });
  }
  if (msgs.length > 40) msgs.splice(0, msgs.length - 40);
  saveJSON("dash.chats", S.chats);
  drawThread(ref);
}

async function generateDescription(ref, btn) {
  const attributes = {};
  for (const row of ref.descForm.querySelectorAll(".attr-row")) {
    const [k, v] = [...row.querySelectorAll("input")].map(i => i.value.trim());
    if (k && v) attributes[k] = v;
    else if (k || v) { setMsg(ref, "Every attribute needs both a name and a value", "err"); return; }
  }
  if (!Object.keys(attributes).length) { setMsg(ref, "Give at least one attribute", "err"); return; }
  await withBusy(btn, async () => {
    setMsg(ref, "Generating…");
    try { await api("/api/describe", { auction_id: ref.id, attributes }); setMsg(ref, "Description updated", "ok"); ref.descForm.classList.remove("open"); }
    catch (e) { setMsg(ref, e.message, "err"); }
  });
}

function updateAuctionCard(ref, a) {
  ref.a = a;
  const me = S.me, mine = me && a.owner === me;
  ref.item.textContent = a.item;
  ref.seller.innerHTML = "";
  ref.seller.append("seller ", el("b", null, a.owner));
  if (mine) ref.seller.appendChild(el("span", "you", "YOU"));
  ref.desc.textContent = a.description || "";
  const high = a.high_bid;
  ref.price.innerHTML = "";
  ref.price.append(high ? fmt(high) : "—", el("small", null, high ? "high bid" : "no bids yet"));
  if (ref.lastHigh !== null && high !== ref.lastHigh) { ref.price.classList.remove("flash"); void ref.price.offsetWidth; ref.price.classList.add("flash"); }
  ref.lastHigh = high;
  ref.highBy.textContent = a.high_bidder ? `by ${a.high_bidder}${a.high_bidder === me ? " (you)" : ""} · ${a.bids.length} bid${a.bids.length === 1 ? "" : "s"}` : "";
  if (a.bids.length !== ref.lastBids) {
    const recent = a.bids.slice(-8).reverse();
    ref.bids.innerHTML = "";
    recent.forEach((b, i) => {
      const li = el("li", (i === 0 ? "top " : "") + (i < a.bids.length - ref.lastBids && ref.lastBids !== 0 ? "new" : ""));
      const who = el("span", null, b.bidder === me ? `${b.bidder} (you)` : b.bidder);
      const right = el("span", null, fmt(b.amount)); right.appendChild(el("span", "t", clockTime(b.time)));
      li.append(who, right);
      ref.bids.appendChild(li);
    });
    ref.lastBids = a.bids.length;
  }
  ref.c.classList.toggle("mine", !!mine);
  ref.c.classList.toggle("closed", a.closed);
  ref.descToggle.hidden = !(mine && !a.closed);
  if (!(mine && !a.closed)) ref.descForm.classList.remove("open");
  drawAction(ref);
  if (ref.bidInput && document.activeElement !== ref.bidInput && Number(ref.bidInput.value) <= a.high_bid) ref.bidInput.value = a.high_bid + 1;
  // closed box
  const ckey = `${a.closed}|${a.winner}|${a.summary}|${a.escrow.status}`;
  if (ckey !== ref.closedKey) {
    ref.closedKey = ckey;
    ref.closedBox.innerHTML = "";
    if (a.closed) {
      const w = el("div", "winner" + (a.winner ? "" : " none"));
      if (a.winner) w.append("Won by ", el("b", null, a.winner + (a.winner === me ? " (you)" : "")), ` at ${fmt(a.high_bid)}`);
      else w.append("Closed with no bids.");
      ref.closedBox.appendChild(w);
      if (a.summary) {
        const sm = el("div", "summary"); sm.append(el("span", "lbl", "Summary"), a.summary);
        ref.closedBox.appendChild(sm);
      } else if (S.data && S.data.llm !== "off") ref.closedBox.appendChild(el("div", "hint", "Summary pending…"));
      if (a.escrow && a.escrow.status !== "NONE") ref.closedBox.appendChild(el("div", "escrow", `escrow: ${a.escrow.status} · ${fmt(a.escrow.amount)} from ${a.escrow.payer}`));
    }
  }
}

function tickCountdowns() {
  for (const ref of S.auctions.values()) {
    const a = ref.a; if (!a) continue;
    const [cls, label, left] = auctionPhase(a);
    ref.status.className = `status ${cls}`; ref.status.textContent = label;
    ref.countdown.innerHTML = "";
    if (a.closed) {
      ref.countdown.append(a.closed_at ? clockTime(a.closed_at) : "", el("small", null, "closed at"));
      ref.bar.style.width = "0";
    } else if (left > 0) {
      ref.countdown.append(dur(left), el("small", null, "left"));
      const total = Math.max(left, (a.close_time - (S.firstSeen[a.auction_id] || a.close_time)) / 1000);
      ref.bar.style.width = `${Math.max(0, Math.min(100, left / total * 100))}%`;
    } else {
      ref.countdown.append("0s", el("small", null, "closing"));
      ref.bar.style.width = "0";
    }
    ref.c.classList.toggle("hidden", !passesFilter(a));
  }
}

function passesFilter(a) {
  switch (S.filter) {
    case "open": return !a.closed;
    case "closed": return a.closed;
    case "mine": return S.me && (a.owner === S.me || a.bids.some(b => b.bidder === S.me));
    default: return true;
  }
}

function renderAuctions(d) {
  const box = $("#auctions");
  const list = [...d.auctions].sort((x, y) => (x.closed - y.closed) ||
    (x.closed ? (y.closed_at || 0) - (x.closed_at || 0) : x.close_time - y.close_time));
  let fsChanged = false;
  for (const a of list) {
    if (!S.firstSeen[a.auction_id]) { S.firstSeen[a.auction_id] = nowUs(); fsChanged = true; }
    let ref = S.auctions.get(a.auction_id);
    if (!ref) { ref = buildAuctionCard(a); S.auctions.set(a.auction_id, ref); }
    updateAuctionCard(ref, a);
  }
  if (fsChanged) saveJSON("dash.firstSeen", S.firstSeen);
  const order = list.map(a => S.auctions.get(a.auction_id).c);
  const cur = [...box.children].filter(c => c.classList.contains("acard"));
  if (order.length !== cur.length || order.some((c, i) => cur[i] !== c)) {
    const focused = document.activeElement;
    box.innerHTML = "";
    if (!order.length) box.appendChild(el("div", "empty", "No auctions yet."));
    for (const c of order) box.appendChild(c);
    if (focused && box.contains(focused)) focused.focus();
  }
  tickCountdowns();
}

/* ---------- users and events ---------- */
function renderUsers(d) {
  const box = $("#users");
  const names = d.users.map(u => u.username);
  const key = names.join(",") + "|" + S.me;
  if (key === box.dataset.key) return;
  box.dataset.key = key;
  const fresh = S.seenUsers ? names.filter(n => !S.seenUsers.has(n)) : [];
  S.seenUsers = new Set(names);
  box.innerHTML = "";
  for (const u of d.users) {
    const chip = el("span", "user" + (fresh.includes(u.username) ? " new" : "") + (u.username === S.me ? " me" : ""));
    chip.append(avatar(u.username), u.username);
    if (u.seeded) chip.appendChild(el("small", null, "test"));
    chip.title = u.seeded ? "test user (password: pass)" : "registered user";
    box.appendChild(chip);
  }
  $("#user-count").textContent = `${d.users.length}`;
}

const EV_ICON = { kill: "x", down: "-", up: "+", leader: "L", election: "E", restart: "R", suspend: "S", resume: "R",
  bid: "$", auction: "A", user: "U", login: "U", close: "C", escrow: "P", llm: "Q", system: "·" };
function renderEvents(d, first) {
  const box = $("#events");
  const top = d.events.length ? d.events[d.events.length - 1].seq : 0;
  if (String(top) === box.dataset.top) return;
  box.dataset.top = top;
  const frag = document.createDocumentFragment();
  for (const e of [...d.events].reverse().slice(0, 120)) {
    const li = el("li", `k-${e.kind}` + (!first && !S.seenEvents.has(e.seq) ? " new" : ""));
    li.append(el("span", "ico", EV_ICON[e.kind] || "•"), el("span", null, e.text), el("time", null, clockTime(e.at)));
    frag.appendChild(li);
    S.seenEvents.add(e.seq);
  }
  box.innerHTML = "";
  box.appendChild(frag);
}

/* ---------- boot ---------- */
document.querySelectorAll("#filters .chip").forEach(c => c.addEventListener("click", () => {
  document.querySelectorAll("#filters .chip").forEach(x => x.classList.remove("active"));
  c.classList.add("active"); S.filter = c.dataset.filter; tickCountdowns();
}));
renderAccount();
setupCreateForm();
poll();
requestAnimationFrame(frame);
