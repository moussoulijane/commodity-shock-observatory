import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  ArrowDownRight,
  ArrowUpRight,
  ArrowSquareOut,
  ChartLine,
  CircleNotch,
  Database,
  DownloadSimple,
  Funnel,
  Gauge,
  Lightning,
  MagnifyingGlass,
  Notebook,
  Waves,
} from "@phosphor-icons/react";
import "./style.css";

type Mode = "REPLAY" | "LIVE" | "SYNTHETIC";
type Point = {
  event_id: string;
  instrument: string;
  name?: string;
  value: number;
  event_time: string;
  observed_at: string;
  change_pct: number | null;
  robust_z: number | null;
  robust_alert: boolean;
  direction: string;
  unit: string;
  frequency: string;
  provider: string;
  delta: number | null;
  revision: string;
};
type Workspace = {
  mode: Mode;
  instruments: Point[];
  observations: number;
  alerts: number;
  scope: string;
  sources: any;
  server_time: string;
};
type History = {
  instrument: string;
  points: Point[];
  truncated: boolean;
  limit: number;
  spec: {
    name: string;
    unit: string;
    frequency: string;
    source: string;
    kind: string;
  };
};
const fmt = (n: number | null | undefined, digits = 2) =>
  n == null
    ? "—"
    : new Intl.NumberFormat("en-US", {
        maximumFractionDigits: digits,
        minimumFractionDigits: digits,
      }).format(n);
const date = (v: string) =>
  new Date(v).toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
const signed = (n: number | null | undefined) =>
  n == null ? "—" : `${n > 0 ? "+" : ""}${fmt(n)}%`;

function useApi<T>(path: string) {
  const [data, setData] = useState<T | null>(null),
    [error, setError] = useState(""),
    [loading, setLoading] = useState(true),
    [generation, setGeneration] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError("");
    fetch(path, { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok)
          throw new Error(
            `Request failed (${response.status}). Check the local API and database.`,
          );
        return response.json();
      })
      .then(setData)
      .catch((e) => {
        if (e.name !== "AbortError") setError(e.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [path, generation]);
  return { data, error, loading, reload: () => setGeneration((v) => v + 1) };
}

function Status({
  error,
  loading,
  retry,
}: {
  error: string;
  loading: boolean;
  retry: () => void;
}) {
  if (error)
    return (
      <div className="error" role="alert">
        <strong>Unable to load this view</strong>
        <p>{error}</p>
        <button onClick={retry}>Try again</button>
      </div>
    );
  return loading ? (
    <div className="skeleton" aria-label="Loading data">
      <div />
      <div />
      <div />
    </div>
  ) : null;
}

function PriceChart({ points, range }: { points: Point[]; range: string }) {
  const [hover, setHover] = useState<number | null>(null);
  const container = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(1000);
  const series = useMemo(() => {
    if (!points.length) return [];
    const end = new Date(points.at(-1)!.event_time);
    const cut = new Date(end);
    cut.setUTCFullYear(end.getUTCFullYear() - (range === "1Y" ? 1 : 5));
    return range === "ALL"
      ? points
      : points.filter((p) => new Date(p.event_time) >= cut);
  }, [points, range]);
  useEffect(() => setHover(null), [series]);
  const hasPoints = series.length > 0;
  useEffect(() => {
    if (!container.current) return;
    const observer = new ResizeObserver(([entry]) =>
      setWidth(Math.max(220, entry.contentRect.width)),
    );
    observer.observe(container.current);
    return () => observer.disconnect();
  }, [hasPoints]);
  if (!series.length)
    return <div className="empty">No observations in this period.</div>;
  const height = width < 500 ? 250 : 290,
    pad = { left: 58, right: 18, top: 20, bottom: 36 };
  const prices = series.map((p) => p.value),
    lo = Math.min(...prices),
    hi = Math.max(...prices),
    span = Math.max(hi - lo, Math.abs(hi) * 0.02, 1);
  const min = lo - span * 0.1,
    max = hi + span * 0.1;
  const first = Date.parse(series[0].event_time),
    last = Date.parse(series.at(-1)!.event_time);
  const x = (p: Point) =>
    pad.left +
    ((Date.parse(p.event_time) - first) / Math.max(last - first, 1)) *
      (width - pad.left - pad.right);
  const y = (v: number) =>
    pad.top + ((max - v) / (max - min)) * (height - pad.top - pad.bottom);
  const path = series
    .map(
      (p, i) => `${i ? "L" : "M"}${x(p).toFixed(2)},${y(p.value).toFixed(2)}`,
    )
    .join(" ");
  const selected =
    series[Math.min(hover ?? series.length - 1, series.length - 1)];
  const move = (event: React.MouseEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const target = ((event.clientX - rect.left) / rect.width) * width;
    let index = 0,
      best = Infinity;
    series.forEach((p, i) => {
      const d = Math.abs(x(p) - target);
      if (d < best) {
        index = i;
        best = d;
      }
    });
    setHover(index);
  };
  return (
    <div className="price-chart" ref={container}>
      <div className="chart-readout">
        <span>{date(selected.event_time)}</span>
        <strong>
          {fmt(selected.value)} <small>{selected.unit}</small>
        </strong>
        <span className={selected.direction === "UP" ? "up" : "down"}>
          {signed(selected.change_pct)} <small>vs previous observation</small>
        </span>
      </div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`${series[0].instrument} historical price chart, ${date(series[0].event_time)} to ${date(series.at(-1)!.event_time)}`}
        onMouseMove={move}
        onMouseLeave={() => setHover(null)}
      >
        {[0, 0.25, 0.5, 0.75, 1].map((t) => {
          const value = min + (max - min) * t;
          return (
            <g key={t}>
              <line
                x1={pad.left}
                x2={width - pad.right}
                y1={y(value)}
                y2={y(value)}
                stroke="#e7e8e2"
                strokeDasharray="3 4"
              />
              <text x={pad.left - 12} y={y(value) + 4} textAnchor="end">
                {fmt(value, 0)}
              </text>
            </g>
          );
        })}
        {min < 0 && max > 0 && (
          <line
            x1={pad.left}
            x2={width - pad.right}
            y1={y(0)}
            y2={y(0)}
            stroke="#8b8e7a"
          />
        )}
        <path
          d={path}
          fill="none"
          stroke="#596e3b"
          strokeWidth="2"
          vectorEffect="non-scaling-stroke"
        />
        {series
          .filter((p) => p.robust_alert)
          .map((p) => (
            <circle
              key={p.event_id}
              cx={x(p)}
              cy={y(p.value)}
              r="3.3"
              fill="#fafbf7"
              stroke="#596e3b"
              strokeWidth="1.4"
            />
          ))}
        {hover !== null && (
          <>
            <line
              x1={x(selected)}
              x2={x(selected)}
              y1={pad.top}
              y2={height - pad.bottom}
              stroke="#929b85"
              strokeDasharray="4 4"
            />
            <circle
              cx={x(selected)}
              cy={y(selected.value)}
              r="4"
              fill="#596e3b"
            />
          </>
        )}
        {(width < 500 ? [0, 0.5, 1] : [0, 0.33, 0.66, 1]).map((t) => {
          const p = series[Math.round(t * (series.length - 1))];
          return (
            <text
              key={t}
              x={x(p)}
              y={height - 9}
              textAnchor={t === 0 ? "start" : t === 1 ? "end" : "middle"}
            >
              {date(p.event_time)}
            </text>
          );
        })}
      </svg>
      <p className="chart-caption">
        Outlined points mark unusual changes. Daily and monthly observations
        retain their original frequency.
      </p>
    </div>
  );
}

function Market({
  workspace,
  mode,
  onShock,
}: {
  workspace: Workspace;
  mode: Mode;
  onShock: () => void;
}) {
  const [instrument, setInstrument] = useState("BRENT"),
    [range, setRange] = useState("5Y");
  const history = useApi<History>(
    `/api/history/${instrument}?mode=${mode}&limit=20000`,
  );
  const latest = workspace.instruments.find((p) => p.instrument === instrument);
  return (
    <>
      <div className="section-title">
        <div>
          <span className="eyebrow">MARKET SURVEILLANCE</span>
          <h1>Commodity monitor</h1>
        </div>
        <p>
          Energy, metals and agriculture.
          <br />
          Historical observations and change signals.
        </p>
      </div>
      <section className="market-layout">
        <div className="chart-panel">
          <div className="panel-title">
            <div>
              <span className="eyebrow">SELECTED INSTRUMENT</span>
              <h2>
                {history.data?.spec.name ?? instrument}
                <span className="subtle-tag">
                  {latest?.frequency ?? "daily"}
                </span>
              </h2>
            </div>
            <div className="segmented" aria-label="Chart period">
              {["1Y", "5Y", "ALL"].map((r) => (
                <button
                  key={r}
                  className={range === r ? "selected" : ""}
                  onClick={() => setRange(r)}
                >
                  {r}
                </button>
              ))}
            </div>
          </div>
          <Status {...history} retry={history.reload} />
          {!history.loading && !history.error && history.data && (
            <PriceChart points={history.data.points} range={range} />
          )}
          {history.data?.truncated && (
            <p className="chart-caption">
              Showing the latest {history.data.limit.toLocaleString()}{" "}
              observations. Older records remain in the archive.
            </p>
          )}
          {latest && (
            <div className="provenance-line">
              <Database size={15} />
              <span>
                {latest.provider} · {latest.unit} · {history.data?.spec.kind}
              </span>
              <span>Observation {date(latest.event_time)}</span>
            </div>
          )}
        </div>
        <aside className="context-panel">
          <span className="eyebrow">AT THE LAST OBSERVATION</span>
          <h3>
            {latest?.robust_alert
              ? "A move worth examining."
              : "Put the latest move in perspective."}
          </h3>
          <p>
            {latest
              ? `${latest.name} changed ${signed(latest.change_pct)} against its previous ${latest.frequency} observation.`
              : "Select an instrument with observations to inspect its context."}
          </p>
          <dl>
            <div>
              <dt>Change score</dt>
              <dd>
                {fmt(latest?.robust_z)} <small>robust z</small>
              </dd>
            </div>
            <div>
              <dt>Alert threshold</dt>
              <dd>±4.00</dd>
            </div>
            <div>
              <dt>Reference window</dt>
              <dd>
                {latest?.frequency === "monthly"
                  ? "36 months"
                  : "60 observations"}
              </dd>
            </div>
          </dl>
          <p className="small">
            The score uses earlier price changes only. It describes an unusual
            move; it does not establish its cause.
          </p>
          <button className="primary" onClick={onShock}>
            Review shock history <ArrowUpRight size={17} />
          </button>
        </aside>
      </section>
      <div className="panel-title mt-8">
        <div>
          <span className="eyebrow">COVERAGE</span>
          <h2>Commodity watchlist</h2>
        </div>
        <span className="muted">
          {workspace.instruments.length} instruments
        </span>
      </div>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Instrument</th>
              <th>Last observation</th>
              <th className="numeric">Price</th>
              <th className="numeric">Change</th>
              <th className="numeric">Score</th>
              <th>Frequency / unit</th>
            </tr>
          </thead>
          <tbody>
            {workspace.instruments.map((p) => (
              <tr
                key={p.instrument}
                className={instrument === p.instrument ? "active-row" : ""}
              >
                <td>
                  <button
                    className="instrument-button"
                    onClick={() => setInstrument(p.instrument)}
                  >
                    <span className="ticker">{p.instrument.slice(0, 2)}</span>
                    <span>
                      <strong>{p.name}</strong>
                      <small>{p.instrument}</small>
                    </span>
                  </button>
                </td>
                <td>{date(p.event_time)}</td>
                <td className="numeric">{fmt(p.value)}</td>
                <td
                  className={`numeric ${p.direction === "UP" ? "up" : "down"}`}
                >
                  {signed(p.change_pct)}
                </td>
                <td className="numeric">{fmt(p.robust_z)}</td>
                <td>
                  <span className="frequency">{p.frequency}</span>
                  <small>{p.unit}</small>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!workspace.instruments.length && (
        <div className="empty">
          <Database size={28} />
          <h3>No {mode.toLowerCase()} observations yet.</h3>
          <p>
            {mode === "LIVE"
              ? "A licensed market feed must be configured before live quotes appear."
              : "Run the ingestion and replay commands in the local guide to populate this view."}
          </p>
        </div>
      )}
    </>
  );
}

function Shocks({ mode, onInspect }: { mode: Mode; onInspect: () => void }) {
  const [direction, setDirection] = useState(""),
    [filter, setFilter] = useState("");
  const query = useApi<{ items: Point[] }>(
    `/api/alerts?mode=${mode}&limit=300${direction ? "&direction=" + direction : ""}`,
  );
  const items = (query.data?.items ?? []).filter((p) =>
    p.instrument.toLowerCase().includes(filter.toLowerCase()),
  );
  const exportCsv = () => {
    const lines = [
      "instrument,date,value,unit,change_pct,robust_z,mode",
      ...items.map((p) =>
        [
          p.instrument,
          p.event_time,
          p.value,
          p.unit,
          p.change_pct,
          p.robust_z,
          mode,
        ].join(","),
      ),
    ];
    const url = URL.createObjectURL(
      new Blob([lines.join("\n")], { type: "text/csv" }),
    );
    const a = document.createElement("a");
    a.href = url;
    a.download = `commodity-shocks-${mode.toLowerCase()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };
  return (
    <>
      <div className="section-title">
        <div>
          <span className="eyebrow">EVENT REVIEW</span>
          <h1>Shock review</h1>
        </div>
        <button onClick={exportCsv} disabled={!items.length}>
          <DownloadSimple size={17} />
          Export selection
        </button>
      </div>
      <div className="filters">
        <label>
          <MagnifyingGlass size={18} />
          <input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter by instrument"
            aria-label="Filter by instrument"
          />
        </label>
        <label>
          <Funnel size={17} />
          <select
            aria-label="Direction"
            value={direction}
            onChange={(e) => setDirection(e.target.value)}
          >
            <option value="">Both directions</option>
            <option value="UP">Upward moves</option>
            <option value="DOWN">Downward moves</option>
          </select>
        </label>
        <span>{items.length} events shown · latest 300 maximum</span>
      </div>
      <Status {...query} retry={query.reload} />
      <div className="shock-list">
        {items.map((p) => (
          <article key={p.event_id}>
            <div className="shock-direction">
              {p.direction === "UP" ? (
                <ArrowUpRight size={24} />
              ) : (
                <ArrowDownRight size={24} />
              )}
            </div>
            <div className="shock-main">
              <div className="flex items-center gap-3">
                <h3>{p.instrument}</h3>
                <span className="frequency">{p.frequency}</span>
              </div>
              <p>
                {fmt(p.value)} {p.unit} · {date(p.event_time)}
              </p>
              <small>
                {p.provider} · {mode.toLowerCase()} · revision{" "}
                {p.revision.slice(0, 8)}
              </small>
            </div>
            <div className="shock-metric">
              <strong>{signed(p.change_pct)}</strong>
              <small>price change</small>
            </div>
            <div className="shock-metric">
              <strong>{fmt(p.robust_z)}</strong>
              <small>robust score</small>
            </div>
          </article>
        ))}
      </div>
      {!query.loading && !items.length && (
        <div className="empty">
          <Lightning size={28} />
          <h3>No events match these filters.</h3>
          <p>
            Change the direction or instrument filter to explore another
            selection.
          </p>
        </div>
      )}
      <div className="method-note">
        <strong>How to read an alert</strong>
        <p>
          A score beyond ±4 compares the latest price difference in original
          units with the median and dispersion of earlier changes. This is a
          screening rule with a warm-up period. Crisis examples are context,
          rather than verified labels of every economic shock.
        </p>
        <button onClick={onInspect}>
          Read the research protocol <ArrowUpRight size={16} />
        </button>
      </div>
    </>
  );
}

function Research() {
  const query = useApi<any>("/api/research");
  const report = query.data?.strict ?? query.data?.exploratory;
  return (
    <>
      <div className="section-title">
        <div>
          <span className="eyebrow">RESEARCH WORKSPACE</span>
          <h1>
            Does oil add
            <br />
            <span>forecasting information?</span>
          </h1>
        </div>
        <Notebook size={36} weight="light" />
      </div>
      <Status {...query} retry={query.reload} />
      <div className="research-intro">
        <h2>US inflation · expanding-window comparison</h2>
        <p>
          Predict year-on-year CPI inflation one and three months ahead. Each
          model receives the same evaluation origins. Errors are expressed in
          percentage points.
        </p>
      </div>
      {report ? (
        <>
          <div className="notice">
            <strong>
              {report.evaluation_type === "initial_release_asof"
                ? "Initial-release evaluation"
                : "Exploratory results · revised historical data"}
            </strong>
            <p>{report.information_policy}</p>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Model</th>
                  <th>Horizon</th>
                  <th className="numeric">MAE / pp</th>
                  <th className="numeric">RMSE / pp</th>
                  <th className="numeric">Origins</th>
                </tr>
              </thead>
              <tbody>
                {report.metrics.map((r: any) => (
                  <tr key={r.model + r.horizon}>
                    <td>
                      <strong>{r.model}</strong>
                    </td>
                    <td>
                      {r.horizon} month{r.horizon > 1 ? "s" : ""}
                    </td>
                    <td className="numeric">{fmt(r.mae_pp, 3)}</td>
                    <td className="numeric">{fmt(r.rmse_pp, 3)}</td>
                    <td className="numeric">{r.n}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="method-note">
            <h3>Interpretation boundaries</h3>
            <ul>
              {report.limitations.map((s: string) => (
                <li key={s}>{s}</li>
              ))}
            </ul>
          </div>
        </>
      ) : (
        !query.loading && (
          <div className="empty">
            <h3>No macro experiment has been run.</h3>
            <p>
              Run the macro evaluation command. A strict initial-release study
              requires a FRED API key.
            </p>
          </div>
        )
      )}
      <div className="research-grid">
        <div>
          <span className="eyebrow">01 / INFORMATION SET</span>
          <h3>What was available then?</h3>
          <p>
            Strict mode uses initial-release vintages with conservative next-day
            availability. Exploratory mode retains revision leakage and is
            labelled accordingly.
          </p>
        </div>
        <div>
          <span className="eyebrow">02 / MODEL COMPARISON</span>
          <h3>Start with a strong reference.</h3>
          <p>
            Persistence and an autoregression are compared with oil-augmented
            ridge regression and XGBoost. Complexity earns its place through
            out-of-sample results.
          </p>
        </div>
      </div>
    </>
  );
}

function Sources() {
  const query = useApi<any>("/api/catalog");
  return (
    <>
      <div className="section-title">
        <div>
          <span className="eyebrow">DATA CATALOG</span>
          <h1>Sources & instruments</h1>
        </div>
        <Database size={36} weight="light" />
      </div>
      <Status {...query} retry={query.reload} />
      {query.data?.sources.map((s: any) => (
        <article className="source-row" key={s.name}>
          <div>
            <h2>{s.name}</h2>
            <p>{s.publication}</p>
          </div>
          <a href={s.url} target="_blank" rel="noreferrer">
            Official source <ArrowSquareOut size={16} />
          </a>
        </article>
      ))}
      <div className="method-note">
        <h3>Three isolated data modes</h3>
        <p>
          <strong>REPLAY</strong> preserves historical observation dates while
          delivering snapshots through Kafka. <strong>LIVE</strong> accepts
          entitled provider intraday quotes. <strong>SYNTHETIC</strong> is
          reserved for engineering tests. These modes use separate topics and
          observation keys.
        </p>
        <p>
          Spot prices, monthly benchmarks and provider quotes remain separate
          instruments. A download timestamp is never presented as the original
          economic publication time.
        </p>
      </div>
    </>
  );
}

function Operations() {
  const query = useApi<any>("/api/operations");
  const d = query.data;
  return (
    <main className="operations">
      <a className="back-link" href="/">
        ← Return to analyst workspace
      </a>
      <div className="section-title">
        <div>
          <span className="eyebrow">ENGINEERING CONSOLE</span>
          <h1>Pipeline & evidence</h1>
        </div>
        <button onClick={query.reload}>Refresh</button>
      </div>
      <Status {...query} retry={query.reload} />
      {d && (
        <>
          <p className="notice">{d.deployment}</p>
          <div className="ops-stats">
            {[
              ["Durable deliveries", d.deliveries],
              ["Rejected", d.rejected],
              ["Window rows", d.window_count],
            ].map(([label, value]) => (
              <div key={label}>
                <span>{label}</span>
                <strong>{Number(value).toLocaleString()}</strong>
              </div>
            ))}
          </div>
          <h2>Canonical observations by mode</h2>
          <pre>{JSON.stringify(d.canonical_counts, null, 2)}</pre>
          <h2>Reliability exercise</h2>
          <pre>
            {JSON.stringify(d.reliability ?? { status: "not run" }, null, 2)}
          </pre>
          <h2>Measured load exercise</h2>
          <pre>{JSON.stringify(d.load ?? { status: "not run" }, null, 2)}</pre>
          <h2>Spark event-time progress</h2>
          <pre>
            {JSON.stringify(
              d.spark_windows ?? { status: "waiting for progress" },
              null,
              2,
            )}
          </pre>
          <h2>Recent committed batches</h2>
          <pre>{JSON.stringify(d.batches, null, 2)}</pre>
        </>
      )}
    </main>
  );
}

function App() {
  const [mode, setMode] = useState<Mode>("REPLAY"),
    [view, setView] = useState("market");
  const query = useApi<Workspace>(`/api/workspace?mode=${mode}`);
  if (location.pathname === "/ops") return <Operations />;
  const nav = [
    { id: "market", label: "Market overview", icon: ChartLine },
    { id: "shocks", label: "Shock history", icon: Lightning },
    { id: "sources", label: "Data sources", icon: Database },
  ];
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a href="/" className="brand">
          <Waves size={31} weight="light" />
          <span>
            Commodity
            <br />
            <strong>observatory</strong>
          </span>
        </a>
        <span className="sidebar-label">ANALYST WORKSPACE</span>
        <nav>
          {nav.map((n) => (
            <button
              key={n.id}
              className={view === n.id ? "nav-active" : ""}
              onClick={() => setView(n.id)}
            >
              <n.icon
                size={20}
                weight={view === n.id ? "duotone" : "regular"}
              />
              {n.label}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <button
            className={view === "research" ? "nav-active" : ""}
            onClick={() => setView("research")}
          >
            <Notebook size={20} />
            Research notebook
          </button>
          <a href="/ops">
            <Gauge size={18} />
            Engineering console <ArrowUpRight size={14} />
          </a>
          <p>
            LOCAL RESEARCH PLATFORM
            <br />
            VERSION 0.1
          </p>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <div className="breadcrumb">
            Commodities <span>/</span>{" "}
            {view === "market"
              ? "Overview"
              : view === "shocks"
                ? "Shock history"
                : view === "research"
                  ? "Research"
                  : "Sources"}
          </div>
          <div className="header-actions">
            <label className="mode-switch">
              <span>Data mode</span>
              <select
                value={mode}
                onChange={(e) => setMode(e.target.value as Mode)}
                aria-label="Data mode"
              >
                <option>REPLAY</option>
                <option>LIVE</option>
                <option>SYNTHETIC</option>
              </select>
            </label>
            <button
              className="icon-button"
              aria-label="Refresh market data"
              onClick={query.reload}
            >
              <CircleNotch size={18} />
            </button>
          </div>
        </header>
        <main className="main-content">
          <div className="mode-banner">
            <span className="mode-dot" />
            <strong>
              {mode === "REPLAY"
                ? "Historical replay"
                : mode === "SYNTHETIC"
                  ? "Synthetic test data"
                  : "Intraday provider feed"}
            </strong>
            <span>
              {mode === "REPLAY"
                ? "Observation dates are historical. Values may include later revisions."
                : mode === "LIVE"
                  ? "Availability depends on the provider connection and your entitlements."
                  : "Artificial observations. Excluded from economic research."}
            </span>
          </div>
          <Status {...query} retry={query.reload} />
          {!query.loading &&
            !query.error &&
            query.data &&
            (view === "market" ? (
              <Market
                workspace={query.data}
                mode={mode}
                onShock={() => setView("shocks")}
              />
            ) : view === "shocks" ? (
              <Shocks mode={mode} onInspect={() => setView("research")} />
            ) : view === "research" ? (
              <Research />
            ) : (
              <Sources />
            ))}
          <footer>
            <span>Commodity Shock Observatory</span>
            <span>Screening signals · evidence before interpretation</span>
          </footer>
        </main>
      </div>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
