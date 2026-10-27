import { useEffect, useRef, useState, type FormEvent } from "react";
import {
  ArrowRight,
  Calculator,
  DownloadSimple,
  Database,
} from "@phosphor-icons/react";

type ExposureLine = {
  instrument: string;
  quantity: number;
  unit: string;
  commodity_hedged_fraction: number;
  fx_hedged_fraction: number;
  fixed_basis_usd_per_unit: number;
};
type Profile = {
  id: string;
  name: string;
  sector: string;
  description: string;
  lines: ExposureLine[];
};
type Profiles = {
  profiles: Profile[];
  fx_reference_default_mad_per_usd: number;
  profile_version: string;
};
type Inputs = {
  profile_id: string;
  commodity_shock_pct: number;
  fx_shock_pct: number;
  fx_reference_mad_per_usd: number;
  as_of: string;
};
type Reference = {
  value: number;
  unit: string;
  frequency: string;
  provider: string;
  event_time: string;
  observed_at: string;
  revision: string;
};
type Totals = {
  baseline_cost_mad: number;
  stressed_cost_mad: number;
  commodity_effect_mad: number;
  fx_effect_mad: number;
  interaction_effect_mad: number;
  delta_cost_mad: number;
  unhedged_stressed_cost_mad: number;
  hedge_effect_mad: number;
};
type Scenario = {
  profile: Profile;
  profile_version: string;
  inputs: Inputs;
  totals: Totals;
  delta_pct: number;
  reference_selection_policy: string;
  limitations: string[];
  lines: (ExposureLine & {
    reference: Reference;
    observation_age_days: number;
  })[];
};
const number = (n: number, digits = 0) =>
  new Intl.NumberFormat("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(n);
const change = (n: number, digits = 0) =>
  `${n > 0 ? "+" : ""}${number(n, digits)}`;
const when = (v: string) =>
  new Date(v).toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
const unit = (v: string) =>
  v.replaceAll("metric_ton", "metric tonne").replaceAll("troy_oz", "troy oz");

async function responseBody(response: Response) {
  const body = await response.json();
  if (!response.ok)
    throw new Error(
      typeof body.detail === "string"
        ? body.detail
        : `Request rejected (${response.status}). Check the scenario inputs.`,
    );
  return body;
}

function Attribution({ totals }: { totals: Totals }) {
  const effects = [
    { label: "Commodity price", value: totals.commodity_effect_mad },
    { label: "USD/MAD assumption", value: totals.fx_effect_mad },
    { label: "Price × FX interaction", value: totals.interaction_effect_mad },
  ];
  const largest = Math.max(...effects.map((e) => Math.abs(e.value)), 1);
  return (
    <section className="attribution-panel">
      <div className="panel-title">
        <div>
          <span className="eyebrow">COST ATTRIBUTION</span>
          <h2>What drives the difference?</h2>
        </div>
        <span className="muted">MAD</span>
      </div>
      {effects.map((effect) => (
        <div className="effect-row" key={effect.label}>
          <span>{effect.label}</span>
          <div className="effect-track" aria-hidden="true">
            <span
              className={effect.value > 0 ? "adverse" : "beneficial"}
              style={{
                transform: `scaleX(${Math.abs(effect.value) / largest})`,
              }}
            />
          </div>
          <strong className={effect.value > 0 ? "down" : "up"}>
            {change(effect.value)}
          </strong>
        </div>
      ))}
      <div className="effect-total">
        <span>Change in purchasing cost</span>
        <strong>{change(totals.delta_cost_mad)} MAD</strong>
      </div>
      <p className="small">
        Contributions incorporate the stated hedge fractions and sum to the cost
        change. FX forwards cover fixed baseline USD notional; the price × FX
        interaction remains exposed to the full FX move. The hedge comparison
        below is separate.
      </p>
    </section>
  );
}

function ScenarioResults({ result }: { result: Scenario }) {
  const t = result.totals;
  const save = () => {
    const link = document.createElement("a");
    const url = URL.createObjectURL(
      new Blob([JSON.stringify(result, null, 2)], { type: "application/json" }),
    );
    link.href = url;
    link.download = `purchasing-scenario-${result.profile.id}.json`;
    link.click();
    URL.revokeObjectURL(url);
  };
  return (
    <div className="scenario-results" aria-live="polite">
      <div className="panel-title">
        <div>
          <span className="eyebrow">CALCULATED SCENARIO</span>
          <h2>{result.profile.name}</h2>
        </div>
        <button
          className="icon-button"
          aria-label="Download calculated scenario"
          onClick={save}
        >
          <DownloadSimple size={18} />
        </button>
      </div>
      <div className="cost-comparison">
        <div>
          <span>Reference purchasing cost</span>
          <strong>
            {number(t.baseline_cost_mad)}
            <small>MAD</small>
          </strong>
        </div>
        <ArrowRight className="cost-arrow" size={23} />
        <div>
          <span>Under your scenario</span>
          <strong>
            {number(t.stressed_cost_mad)}
            <small>MAD</small>
          </strong>
        </div>
      </div>
      <div className="scenario-impact">
        <strong className={t.delta_cost_mad > 0 ? "down" : "up"}>
          {change(t.delta_cost_mad)} MAD
        </strong>
        <span>
          {change(result.delta_pct, 2)}% versus reference · one assumed
          purchasing month
        </span>
      </div>
      <Attribution totals={t} />
      <section className="hedge-comparison">
        <span className="eyebrow">IDEALIZED HEDGE COMPARISON</span>
        <div>
          <p>Fully unhedged stressed cost</p>
          <strong>{number(t.unhedged_stressed_cost_mad)} MAD</strong>
        </div>
        <div>
          <p>Unhedged cost minus profile cost</p>
          <strong>{change(t.hedge_effect_mad)} MAD</strong>
        </div>
        <p className="small">
          A positive difference means the assumed hedge fractions reduce this
          scenario's cost. A negative difference means they forego part of a
          favourable move. Premiums and settlement details are excluded.
        </p>
      </section>
      <section className="reference-section">
        <div className="panel-title">
          <div>
            <span className="eyebrow">BENCHMARK EVIDENCE</span>
            <h2>References used</h2>
          </div>
          <Database size={20} />
        </div>
        <p className="reference-caveat">
          Economic-date cutoff only. These snapshots can contain revisions
          acquired after the selected date; they do not reconstruct what was
          published then.
        </p>
        {result.lines.map((line) => (
          <article className="benchmark-reference" key={line.instrument}>
            <div>
              <strong>{line.instrument}</strong>
              <span className="frequency">{line.reference.frequency}</span>
              <span>
                {number(line.reference.value, 2)} {unit(line.reference.unit)}
              </span>
            </div>
            <dl>
              <div>
                <dt>Source</dt>
                <dd>{line.reference.provider}</dd>
              </div>
              <div>
                <dt>Economic observation</dt>
                <dd>{when(line.reference.event_time)}</dd>
              </div>
              <div>
                <dt>Snapshot acquired</dt>
                <dd>{when(line.reference.observed_at)}</dd>
              </div>
              <div>
                <dt>Age at calculation</dt>
                <dd>
                  {line.observation_age_days} days · {line.reference.frequency}{" "}
                  series
                </dd>
              </div>
            </dl>
          </article>
        ))}
      </section>
      <details className="scenario-limitations">
        <summary>Assumptions and interpretation limits</summary>
        <ul>
          {result.limitations.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
        <p>
          Profile specification: {result.profile_version}. Scenario calculations
          and inputs are not stored by the service.
        </p>
      </details>
    </div>
  );
}

export default function ExposureWorkspace() {
  const [profiles, setProfiles] = useState<Profiles | null>(null),
    [profileError, setProfileError] = useState(""),
    [generation, setGeneration] = useState(0);
  const [inputs, setInputs] = useState<Inputs>({
    profile_id: "energy-importer",
    commodity_shock_pct: 10,
    fx_shock_pct: 3,
    fx_reference_mad_per_usd: 10,
    as_of: "",
  });
  const [result, setResult] = useState<Scenario | null>(null),
    [error, setError] = useState(""),
    [calculating, setCalculating] = useState(false);
  const request = useRef<AbortController | null>(null);
  useEffect(() => {
    const controller = new AbortController();
    setProfileError("");
    fetch("/api/exposures/profiles", { signal: controller.signal })
      .then(responseBody)
      .then((data) => {
        setProfiles(data);
        setInputs((previous) => ({
          ...previous,
          profile_id: data.profiles[0]?.id ?? "",
          fx_reference_mad_per_usd: data.fx_reference_default_mad_per_usd,
        }));
      })
      .catch((e) => {
        if (e.name !== "AbortError") setProfileError(e.message);
      });
    return () => controller.abort();
  }, [generation]);
  useEffect(() => () => request.current?.abort(), []);
  const profile = profiles?.profiles.find((p) => p.id === inputs.profile_id);
  const edit = <K extends keyof Inputs>(key: K, value: Inputs[K]) => {
    setInputs((previous) => ({ ...previous, [key]: value }));
    setResult(null);
    setError("");
  };
  const calculate = async (event: FormEvent) => {
    event.preventDefault();
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setCalculating(true);
    setError("");
    setResult(null);
    try {
      const body = {
        ...inputs,
        as_of: inputs.as_of ? `${inputs.as_of}T23:59:59.999Z` : null,
      };
      const response = await fetch("/api/exposures/scenario", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      const data = await responseBody(response);
      if (!controller.signal.aborted) setResult(data);
    } catch (e) {
      if (!controller.signal.aborted)
        setError(
          e instanceof Error ? e.message : "Scenario calculation failed.",
        );
    } finally {
      if (!controller.signal.aborted) setCalculating(false);
    }
  };
  return (
    <>
      <div className="section-title">
        <div>
          <span className="eyebrow">MOROCCAN IMPORTER SCENARIOS</span>
          <h1>Purchasing exposure</h1>
        </div>
        <p>
          Commodity prices and currency assumptions.
          <br />
          An explicit cost bridge, in MAD.
        </p>
      </div>
      <div className="notice exposure-notice">
        <strong>
          Fictional profiles · historical benchmarks · user-assumed FX
        </strong>
        <p>
          Calculate a mechanical purchasing-cost scenario. Quantities, hedges
          and basis costs are demonstration inputs; results are neither company
          forecasts nor credit losses.
        </p>
      </div>
      {profileError ? (
        <div className="error" role="alert">
          <strong>Profiles unavailable</strong>
          <p>{profileError}</p>
          <button onClick={() => setGeneration((v) => v + 1)}>Try again</button>
        </div>
      ) : !profiles ? (
        <div className="skeleton" aria-label="Loading exposure profiles">
          <div />
          <div />
          <div />
        </div>
      ) : (
        <div className="exposure-layout">
          <form className="exposure-controls" onSubmit={calculate}>
            <fieldset disabled={calculating}>
              <legend className="eyebrow">SCENARIO INPUTS</legend>
              <label htmlFor="exposure-profile">
                Fictional purchasing profile
              </label>
              <select
                id="exposure-profile"
                value={inputs.profile_id}
                onChange={(e) => edit("profile_id", e.target.value)}
              >
                {profiles.profiles.map((item) => (
                  <option key={item.id} value={item.id}>
                    {item.name}
                  </option>
                ))}
              </select>
              {profile && (
                <div className="profile-specification">
                  <p>{profile.description}</p>
                  {profile.lines.map((line) => (
                    <dl key={line.instrument}>
                      <div>
                        <dt>Quantity / month</dt>
                        <dd>
                          {number(line.quantity)} {unit(line.unit)}
                        </dd>
                      </div>
                      <div>
                        <dt>Benchmark</dt>
                        <dd>{line.instrument}</dd>
                      </div>
                      <div>
                        <dt>Commodity hedge</dt>
                        <dd>{number(100 * line.commodity_hedged_fraction)}%</dd>
                      </div>
                      <div>
                        <dt>FX hedge / baseline USD</dt>
                        <dd>{number(100 * line.fx_hedged_fraction)}%</dd>
                      </div>
                      <div>
                        <dt>Fixed basis assumption</dt>
                        <dd>
                          {number(line.fixed_basis_usd_per_unit, 2)} USD /{" "}
                          {unit(line.unit)}
                        </dd>
                      </div>
                    </dl>
                  ))}
                </div>
              )}
              <div className="scenario-field">
                <label htmlFor="commodity-shock">Commodity price shock</label>
                <div className="unit-input">
                  <input
                    id="commodity-shock"
                    type="number"
                    min="-90"
                    max="200"
                    step="0.1"
                    required
                    value={inputs.commodity_shock_pct}
                    onChange={(e) =>
                      edit("commodity_shock_pct", Number(e.target.value))
                    }
                  />
                  <span>%</span>
                </div>
              </div>
              <div className="scenario-field">
                <label htmlFor="fx-shock">FX shock</label>
                <div className="unit-input">
                  <input
                    id="fx-shock"
                    type="number"
                    min="-90"
                    max="100"
                    step="0.1"
                    required
                    value={inputs.fx_shock_pct}
                    onChange={(e) =>
                      edit("fx_shock_pct", Number(e.target.value))
                    }
                  />
                  <span>%</span>
                </div>
                <p>Positive means more MAD per USD.</p>
              </div>
              <div className="scenario-field fx-assumption">
                <label htmlFor="fx-reference">
                  FX reference <span>User assumption</span>
                </label>
                <div className="unit-input">
                  <input
                    id="fx-reference"
                    type="number"
                    min="0.0001"
                    max="100"
                    step="any"
                    required
                    value={inputs.fx_reference_mad_per_usd}
                    onChange={(e) =>
                      edit("fx_reference_mad_per_usd", Number(e.target.value))
                    }
                  />
                  <span>MAD / USD</span>
                </div>
                <p>
                  The default 10.00 is an editable demonstration assumption. No
                  market FX quote is downloaded.
                </p>
              </div>
              <div className="scenario-field">
                <label htmlFor="reference-cutoff">
                  Benchmark cutoff <span>Optional</span>
                </label>
                <input
                  id="reference-cutoff"
                  type="date"
                  max={new Date().toISOString().slice(0, 10)}
                  value={inputs.as_of}
                  onChange={(e) => edit("as_of", e.target.value)}
                />
                <p>
                  Latest economic observation on or before this date. Leave
                  empty to use the latest stored reference.
                </p>
              </div>
              <button className="primary calculate-button" type="submit">
                <Calculator size={18} />
                {calculating ? "Calculating…" : "Calculate scenario"}
              </button>
            </fieldset>
            <p className="calculation-privacy">
              Calculation only. This service does not save your scenario inputs.
            </p>
          </form>
          <div className="exposure-output">
            {calculating ? (
              <div
                className="skeleton"
                aria-label="Calculating exposure scenario"
              >
                <div />
                <div />
                <div />
              </div>
            ) : error ? (
              <div className="error" role="alert">
                <strong>Scenario unavailable</strong>
                <p>{error}</p>
                <p>
                  Check the selected date and confirm that the profile's
                  historical benchmark has been ingested.
                </p>
              </div>
            ) : result ? (
              <ScenarioResults result={result} />
            ) : (
              <div className="exposure-empty">
                <span className="eyebrow">
                  REFERENCE → SCENARIO → ATTRIBUTION
                </span>
                <h2>
                  Connect a market move
                  <br />
                  to a purchasing programme.
                </h2>
                <p>
                  Choose a fictional profile and your commodity and FX
                  assumptions. The calculation shows the reference cost,
                  stressed cost and each contribution, with the benchmark's date
                  and source.
                </p>
                <div className="exposure-steps">
                  <span>01 / Select exposure</span>
                  <span>02 / Set assumptions</span>
                  <span>03 / Examine the bridge</span>
                </div>
                <p className="small">
                  Historical data are revised snapshots. A date cutoff is not a
                  point-in-time publication backtest.
                </p>
              </div>
            )}
          </div>
        </div>
      )}
    </>
  );
}
