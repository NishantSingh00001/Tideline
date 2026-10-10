import { Link } from "react-router-dom";

export default function NotFound() {
  return (
    <div className="wrap" style={{ padding: "80px 0 120px" }}>
      <h1 style={{ font: "500 44px/1.1 var(--serif)", margin: 0 }}>Uncharted water</h1>
      <p style={{ maxWidth: "52ch", color: "var(--ink-2)", fontSize: 18 }}>
        There's no page at this address. The live chart is on the <Link to="/">home page</Link>, and you can query the stream on the <Link to="/sql">SQL page</Link>.
      </p>
    </div>
  );
}
