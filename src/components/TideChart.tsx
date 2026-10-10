// The hero chart: every change is a dot placed at its *event time*. The
// magenta line is the watermark (the "tide line"): windows to its left have
// closed and show their counts like soundings on a chart; the blue water to
// its right is still open. Dots that land behind the line arrived late.

import { useEffect, useRef } from "react";
import { live } from "../lib/live.ts";
import { PROJECTS, PROJECT_COLORS } from "../lib/projects.ts";
import { clock } from "../lib/format.ts";

const WINDOW = 10_000;
const SHORT = ["Wikidata", "Commons", "English WP", "Other WPs", "Other"];

function css(name: string) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

export function TideChart() {
  const wrap = useRef<HTMLDivElement>(null);
  const canvas = useRef<HTMLCanvasElement>(null);
  const tip = useRef<HTMLDivElement>(null);
  const notice = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = canvas.current!;
    const box = wrap.current!;
    const g = el.getContext("2d")!;
    const C = {
      paper: css("--paper"),
      shoal: css("--shoal"),
      shoal2: css("--shoal-2"),
      ink: css("--ink"),
      ink2: css("--ink-2"),
      ink3: css("--ink-3"),
      rule: css("--rule"),
      rule2: css("--rule-2"),
      magenta: css("--magenta"),
      serif: css("--serif"),
      cond: css("--cond"),
      sans: css("--sans"),
    };
    let raf = 0;
    let right = 0;
    let last = performance.now();
    let hover: { x: number; y: number } | null = null;
    // Positions drawn this frame, for hover hit-testing.
    let hits: { x: number; y: number; i: number }[] = [];
    const reduce = matchMedia("(prefers-reduced-motion: reduce)").matches;

    const frame = (now: number) => {
      raf = requestAnimationFrame(frame);
      const dt = Math.min(100, now - last);
      last = now;
      const s = live.get();
      const W = box.clientWidth;
      const H = box.clientHeight;
      const dpr = Math.min(2, window.devicePixelRatio || 1);
      if (el.width !== Math.round(W * dpr) || el.height !== Math.round(H * dpr)) {
        el.width = Math.round(W * dpr);
        el.height = Math.round(H * dpr);
      }
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      const narrow = W < 640;
      const L = narrow ? 76 : 148;
      const R = 14;
      const T = 40;
      const B = 34;
      const pw = W - L - R;
      const ph = H - T - B;
      const span = narrow ? 45_000 : 90_000;
      const wm = s.watermark;
      const maxTs = s.maxTs;
      const target = (maxTs ?? Date.now()) + 2500;
      if (!right || Math.abs(target - right) > span || reduce) right = target;
      else right += (target - right) * Math.min(1, (dt / 1000) * 3.5);
      const left = right - span;
      const x = (t: number) => L + ((t - left) / span) * pw;
      const laneH = ph / PROJECTS.length;

      g.clearRect(0, 0, W, H);
      g.fillStyle = C.paper;
      g.fillRect(0, 0, W, H);

      // Open water: everything after the watermark.
      const xw = wm !== null ? Math.max(L, Math.min(L + pw, x(wm))) : L;
      g.fillStyle = C.shoal;
      g.fillRect(xw, T, L + pw - xw, ph);

      // Lanes
      g.strokeStyle = C.rule2;
      g.lineWidth = 1;
      for (let i = 1; i < PROJECTS.length; i++) {
        const y = Math.round(T + i * laneH) + 0.5;
        g.beginPath();
        g.moveTo(L, y);
        g.lineTo(L + pw, y);
        g.stroke();
      }
      g.textBaseline = "middle";
      g.textAlign = "left";
      PROJECTS.forEach((p, i) => {
        const y = T + i * laneH + laneH / 2;
        g.fillStyle = PROJECT_COLORS[i];
        g.beginPath();
        g.arc(10, y, 4, 0, Math.PI * 2);
        g.fill();
        g.fillStyle = C.ink;
        g.font = `italic 400 ${narrow ? 13 : 16}px ${C.serif}`;
        g.fillText(narrow ? SHORT[i] : p, 21, y);
      });

      // Window meridians, graticule bars and time labels.
      const first = Math.floor(left / WINDOW) * WINDOW;
      g.font = `500 12px ${C.cond}`;
      g.textAlign = "center";
      g.textBaseline = "alphabetic";
      for (let t = first; t <= right; t += WINDOW) {
        const x0 = x(t);
        const x1 = x(t + WINDOW);
        const closed = wm !== null && wm >= t + WINDOW - 1;
        if (x1 < L || x0 > L + pw) continue;
        const a = Math.max(L, x0);
        const b = Math.min(L + pw, x1);
        // Border bars: closed windows alternate ink/paper like a chart's
        // latitude scale; open ones are drawn as hollow water.
        for (const yy of [T - 9, T + ph + 2]) {
          if (closed) {
            g.fillStyle = (t / WINDOW) % 2 === 0 ? C.ink : C.paper;
            g.fillRect(a, yy, b - a, 7);
            g.strokeStyle = C.ink;
            g.strokeRect(a + 0.5, yy + 0.5, b - a - 1, 6);
          } else {
            g.fillStyle = C.shoal2;
            g.fillRect(a, yy, b - a, 7);
          }
        }
        if (x0 >= L && x0 <= L + pw) {
          g.strokeStyle = C.rule;
          g.setLineDash([2, 4]);
          g.beginPath();
          g.moveTo(Math.round(x0) + 0.5, T);
          g.lineTo(Math.round(x0) + 0.5, T + ph);
          g.stroke();
          g.setLineDash([]);
          const every = narrow ? 2 : 1;
          if (Math.round(t / WINDOW) % every === 0 && x0 > L + 24 && x0 < L + pw - 24) {
            g.fillStyle = C.ink2;
            g.fillText(clock(t), x0, T - 14);
          }
        }
      }

      // Soundings: the count of each closed window, per lane.
      g.textAlign = "center";
      g.textBaseline = "top";
      const winPx = (WINDOW / span) * pw;
      for (const w of s.pulse) {
        const cx = x(w.start + WINDOW / 2);
        if (cx < L + 10 || cx > L + pw - 10) continue;
        if (winPx > 34) {
          g.font = `italic 400 ${narrow ? 11 : 13}px ${C.serif}`;
          g.fillStyle = C.ink2;
          w.counts.forEach((n, i) => {
            if (n) g.fillText(String(n), cx, T + i * laneH + 3);
          });
        }
      }

      // Dots
      hits = [];
      const dots = s.dots;
      for (let i = 0; i < dots.length; i++) {
        const d = dots[i];
        // Event times only have one-second precision: spread each dot inside its second.
        const tx = d.ts + ((d.y * 7.31) % 1) * 950;
        if (tx < left - 1000 || tx > right) continue;
        const px = x(tx);
        if (px < L) continue;
        const age = now - d.arrived;
        if (age < 0) continue;
        const k = reduce ? 1 : Math.min(1, age / 380);
        const ease = 1 - Math.pow(1 - k, 3);
        const py = T + d.lane * laneH + 16 + d.y * (laneH - 24) - (1 - ease) * 10;
        g.globalAlpha = 0.15 + 0.85 * ease;
        g.fillStyle = PROJECT_COLORS[d.lane];
        g.beginPath();
        g.arc(px, py, narrow ? 2.1 : 2.6, 0, Math.PI * 2);
        g.fill();
        if (d.late) {
          g.strokeStyle = C.magenta;
          g.lineWidth = 1.4;
          g.beginPath();
          g.arc(px, py, 5.2, 0, Math.PI * 2);
          g.stroke();
          g.lineWidth = 1;
        }
        hits.push({ x: px, y: py, i });
      }
      g.globalAlpha = 1;

      // Newest event and the watermark (the tide line).
      g.textBaseline = "alphabetic";
      if (maxTs !== null) {
        const xm = x(maxTs + 1000);
        if (xm > L && xm < L + pw) {
          g.strokeStyle = C.ink3;
          g.setLineDash([4, 3]);
          g.beginPath();
          g.moveTo(Math.round(xm) + 0.5, T);
          g.lineTo(Math.round(xm) + 0.5, T + ph);
          g.stroke();
          g.setLineDash([]);
        }
      }
      if (wm !== null && xw > L && xw < L + pw) {
        g.strokeStyle = C.magenta;
        g.lineWidth = 2;
        g.beginPath();
        g.moveTo(Math.round(xw), T - 2);
        g.lineTo(Math.round(xw), T + ph + 2);
        g.stroke();
        g.lineWidth = 1;
        const label = `watermark ${clock(wm)}`;
        g.font = `600 12px ${C.cond}`;
        const tw = g.measureText(label).width + 10;
        const lx = Math.min(L + pw - tw, Math.max(L, xw - tw - 4));
        g.fillStyle = C.paper;
        g.fillRect(lx, T + ph - 22, tw, 17);
        g.fillStyle = C.magenta;
        g.textAlign = "left";
        g.fillText(label, lx + 5, T + ph - 9);
        if (maxTs !== null) {
          const xm = x(maxTs + 1000);
          if (xm - xw > 40) {
            g.fillStyle = C.ink3;
            g.font = `500 12px ${C.cond}`;
            g.textAlign = "center";
            g.fillText(`${Math.round(s.delay / 1000)} s`, (xw + xm) / 2, T + 13);
            g.strokeStyle = C.ink3;
            g.beginPath();
            g.moveTo(xw + 3, T + 18);
            g.lineTo(xm - 3, T + 18);
            g.stroke();
          }
        }
      }

      // Hover
      const tipEl = tip.current!;
      if (hover) {
        let best = -1;
        let bd = 64;
        for (const h of hits) {
          const d2 = (h.x - hover.x) ** 2 + (h.y - hover.y) ** 2;
          if (d2 < bd) {
            bd = d2;
            best = h.i;
          }
        }
        if (best >= 0) {
          const d = dots[best];
          const hx = hits.find((h) => h.i === best)!;
          tipEl.style.display = "block";
          tipEl.innerHTML = "";
          const b = document.createElement("b");
          b.textContent = d.title;
          const meta = document.createElement("div");
          meta.textContent = `${d.type === "categorize" ? "Category change" : d.type === "new" ? "New page" : d.type === "log" ? "Log entry" : "Edit"} on ${d.server}${d.bot ? ", by a bot" : ""}`;
          const who = document.createElement("div");
          who.textContent = `By ${d.user} at ${clock(d.ts)} (event time)`;
          tipEl.append(b, meta, who);
          if (d.late) {
            const late = document.createElement("div");
            late.style.color = C.magenta;
            late.textContent = "Arrived behind the watermark: window queries drop it.";
            tipEl.append(late);
          }
          const tx = hx.x + 14 + 280 > W ? hx.x - 14 - Math.min(280, tipEl.offsetWidth) : hx.x + 14;
          tipEl.style.left = `${tx}px`;
          tipEl.style.top = `${Math.max(4, Math.min(H - tipEl.offsetHeight - 4, hx.y - 20))}px`;
        } else tipEl.style.display = "none";
      } else tipEl.style.display = "none";

      // Empty and error states
      const n = notice.current!;
      const st = s.status;
      let msg = "";
      let small = "";
      if (!s.dots.length && maxTs === null) {
        if (st?.state === "retrying") {
          msg = st.message || "Can't reach the stream";
          small = `Retrying (attempt ${st.reconnects}). The page keeps trying in the background.`;
        } else {
          msg = "Connecting to Wikipedia's edit stream…";
          small = "stream.wikimedia.org/v2/stream/recentchange";
        }
      }
      if (n.dataset.msg !== msg + small) {
        n.dataset.msg = msg + small;
        n.innerHTML = "";
        if (msg) {
          const p = document.createElement("div");
          p.textContent = msg;
          const sm = document.createElement("small");
          sm.textContent = small;
          p.append(sm);
          n.append(p);
        }
      }
    };
    raf = requestAnimationFrame(frame);
    const move = (e: PointerEvent) => {
      const r = el.getBoundingClientRect();
      hover = { x: e.clientX - r.left, y: e.clientY - r.top };
    };
    const leave = () => (hover = null);
    el.addEventListener("pointermove", move);
    el.addEventListener("pointerleave", leave);
    return () => {
      cancelAnimationFrame(raf);
      el.removeEventListener("pointermove", move);
      el.removeEventListener("pointerleave", leave);
    };
  }, []);

  return (
    <div className="tide" ref={wrap}>
      <canvas ref={canvas} role="img" aria-label="Live chart of Wikipedia changes by event time, with the watermark line and 10-second windows" />
      <div className="tip" ref={tip} style={{ display: "none" }} />
      <div className="notice" ref={notice} />
    </div>
  );
}
