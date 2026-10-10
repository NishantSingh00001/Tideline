import { lazy, Suspense, useEffect, useState } from "react";
import { NavLink, Route, Routes, useLocation } from "react-router-dom";
import { Rose } from "./components/Rose.tsx";
import { useLive } from "./lib/live.ts";
import Live from "./pages/Live.tsx";

const Sql = lazy(() => import("./pages/Sql.tsx"));
const Kafka = lazy(() => import("./pages/Kafka.tsx"));
const Recovery = lazy(() => import("./pages/Recovery.tsx"));
const About = lazy(() => import("./pages/About.tsx"));
const NotFound = lazy(() => import("./pages/NotFound.tsx"));

function StreamStatus() {
  const state = useLive((s) => s.status?.state || "connecting");
  const paused = useLive((s) => s.paused);
  const rate = useLive((s) => s.rate);
  const message = useLive((s) => s.status?.message || "");
  const label = paused ? "Paused" : state === "open" ? `${rate.toFixed(0)} changes/s` : state === "retrying" ? "Reconnecting" : "Connecting";
  return (
    <div className={`status ${paused ? "paused" : state}`} title={message || "Wikimedia EventStreams"} role="status">
      <span className="dot" aria-hidden="true" />
      <span className="label">{label}</span>
    </div>
  );
}

export default function App() {
  const [menu, setMenu] = useState(false);
  const loc = useLocation();
  useEffect(() => {
    setMenu(false);
    window.scrollTo(0, 0);
  }, [loc.pathname]);
  const links: [string, string][] = [
    ["/", "Live"],
    ["/sql", "SQL"],
    ["/kafka", "Kafka"],
    ["/recovery", "Recovery"],
    ["/how", "How it works"],
  ];
  return (
    <>
      <a className="sr-only" href="#main">
        Skip to content
      </a>
      <header className="bar">
        <div className="wrap">
          <NavLink to="/" className="brand" aria-label="Tideline home">
            <Rose />
            Tideline
          </NavLink>
          <nav className={`nav${menu ? " open" : ""}`} aria-label="Main">
            {links.map(([to, label]) => (
              <NavLink key={to} to={to} end={to === "/"}>
                {label}
              </NavLink>
            ))}
          </nav>
          <StreamStatus />
          <button className="btn small menu-btn" aria-expanded={menu} onClick={() => setMenu((m) => !m)}>
            Menu
          </button>
        </div>
      </header>
      <main id="main">
        <Suspense fallback={<div className="wrap waiting">Unrolling the chart…</div>}>
          <Routes>
            <Route path="/" element={<Live />} />
            <Route path="/sql" element={<Sql />} />
            <Route path="/q/:id" element={<Sql />} />
            <Route path="/kafka" element={<Kafka />} />
            <Route path="/recovery" element={<Recovery />} />
            <Route path="/how" element={<About />} />
            <Route path="*" element={<NotFound />} />
          </Routes>
        </Suspense>
      </main>
      <footer className="footer">
        <div className="wrap">
          <span>
            Live data: <a href="https://stream.wikimedia.org/?doc">Wikimedia EventStreams</a>. Built by{" "}
            <a href="https://nishant-singhaa0240274.netlify.app">Nishant Singh</a>.
          </span>
          <span>
            <a href="https://github.com/NishantSingh00001/Tideline">Source on GitHub</a>
          </span>
        </div>
      </footer>
    </>
  );
}
