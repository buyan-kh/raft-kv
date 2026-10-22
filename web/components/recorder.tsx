"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import {
  frameAt,
  isolated,
  markers,
  roleColor,
  roleLabel,
  type Scenario,
  type Trace,
  type TraceEvent,
} from "@/lib/trace";

const SPEEDS = ["0.25", "1", "4", "16"] as const;
const RPC_WINDOW_MS = 80;
const ROW = 132;

export function Recorder() {
  const [scenarios, setScenarios] = useState<Scenario[] | null>(null);
  const [indexError, setIndexError] = useState<string | null>(null);
  const [scenarioId, setScenarioId] = useState<string>("election");
  const [trace, setTrace] = useState<Trace | null>(null);
  const [traceError, setTraceError] = useState<string | null>(null);
  const [loadingTrace, setLoadingTrace] = useState(true);
  const [time, setTime] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState("1");
  const timeRef = useRef(0);

  useEffect(() => {
    timeRef.current = time;
  }, [time]);

  useEffect(() => {
    let cancelled = false;
    fetch("/traces/index.json")
      .then((response) => {
        if (!response.ok) {
          throw new Error("index " + response.status);
        }
        return response.json();
      })
      .then((body: { scenarios: Scenario[] }) => {
        if (cancelled) {
          return;
        }
        setScenarios(body.scenarios);
        if (body.scenarios[0]) {
          setScenarioId(body.scenarios[0].id);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setIndexError("The trace index did not load.");
          setLoadingTrace(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!scenarios) {
      return;
    }
    const scenario = scenarios.find((item) => item.id === scenarioId) ?? scenarios[0];
    if (!scenario) {
      setLoadingTrace(false);
      return;
    }
    let cancelled = false;
    setLoadingTrace(true);
    setTraceError(null);
    setPlaying(false);
    setTime(0);
    fetch("/traces/" + scenario.file)
      .then((response) => {
        if (!response.ok) {
          throw new Error("trace " + response.status);
        }
        return response.json();
      })
      .then((body: Trace) => {
        if (!cancelled) {
          setTrace(body);
          setLoadingTrace(false);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setTrace(null);
          setTraceError("That scenario file did not load.");
          setLoadingTrace(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [scenarioId, scenarios]);

  useEffect(() => {
    if (!playing || !trace) {
      return;
    }
    let frame = 0;
    let last = performance.now();
    const rate = Number(speed);
    const step = (now: number) => {
      const dt = now - last;
      last = now;
      const next = Math.min(trace.durationMs, timeRef.current + dt * rate);
      timeRef.current = next;
      setTime(next);
      if (next >= trace.durationMs) {
        setPlaying(false);
        return;
      }
      frame = requestAnimationFrame(step);
    };
    frame = requestAnimationFrame(step);
    return () => cancelAnimationFrame(frame);
  }, [playing, speed, trace]);

  const seek = useCallback(
    (next: number) => {
      if (!trace) {
        return;
      }
      const clamped = Math.max(0, Math.min(trace.durationMs, next));
      timeRef.current = clamped;
      setTime(clamped);
      if (clamped >= trace.durationMs) {
        setPlaying(false);
      }
    },
    [trace],
  );

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target && target.closest("button, input, textarea, select, [role='listbox']")) {
        return;
      }
      if (event.key === " ") {
        event.preventDefault();
        setPlaying((value) => !value);
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        seek(timeRef.current + 20);
      } else if (event.key === "ArrowLeft") {
        event.preventDefault();
        seek(timeRef.current - 20);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [seek]);

  const frame = useMemo(() => (trace ? frameAt(trace, time) : null), [trace, time]);
  const ticks = useMemo(() => (trace ? markers(trace) : []), [trace]);
  const recent = useMemo(() => {
    if (!trace) {
      return [];
    }
    return trace.events
      .map((event, index) => ({ event, index }))
      .filter((item) => item.event.kind !== "state" && item.event.t <= time)
      .slice(-8);
  }, [trace, time]);
  const inflight = useMemo(() => {
    if (!trace) {
      return [];
    }
    return trace.events
      .map((event, index) => ({ event, index }))
      .filter(
        (item) =>
          item.event.kind === "rpc" && item.event.t <= time && time - item.event.t < RPC_WINDOW_MS,
      );
  }, [trace, time]);

  if (indexError) {
    return (
      <Shell>
        <p className="text-destructive">{indexError}</p>
        <p className="mt-2 text-sm text-muted-foreground">
          From the repository root, run <code className="font-mono">./gradlew traces</code> and refresh.
        </p>
      </Shell>
    );
  }

  if (!scenarios) {
    return (
      <Shell>
        <p className="text-sm text-muted-foreground">Loading recorded scenarios…</p>
      </Shell>
    );
  }

  if (scenarios.length === 0) {
    return (
      <Shell>
        <p>No scenarios have been recorded yet.</p>
      </Shell>
    );
  }

  return (
    <Shell>
      <header className="flex flex-col gap-4 border-b border-border pb-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="max-w-2xl">
          <p className="font-mono text-[11px] tracking-[0.18em] text-primary uppercase">
            raftkv flight recorder
          </p>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight">{trace?.title ?? "Cluster trace"}</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {trace?.blurb ?? "Pick a recorded run."}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Select
            value={scenarioId}
            onValueChange={(value) => {
              if (value) {
                setScenarioId(value);
              }
            }}
          >
            <SelectTrigger aria-label="Scenario" className="min-w-52">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {scenarios.map((scenario) => (
                <SelectItem key={scenario.id} value={scenario.id}>
                  {scenario.title}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={speed} onValueChange={(value) => value && setSpeed(value)}>
            <SelectTrigger aria-label="Playback speed" className="w-24">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {SPEEDS.map((item) => (
                <SelectItem key={item} value={item}>
                  {item}×
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </header>

      {loadingTrace && <p className="mt-6 text-sm text-muted-foreground">Loading trace…</p>}
      {traceError && <p className="mt-6 text-sm text-destructive">{traceError}</p>}

      {trace && frame && !loadingTrace && (
        <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_280px]">
          <section className="min-w-0">
            <div className="mb-3 flex flex-wrap items-center gap-2">
              <Button
                type="button"
                onClick={() => {
                  if (time >= trace.durationMs) {
                    seek(0);
                  }
                  setPlaying((value) => !value);
                }}
                aria-pressed={playing}
              >
                {playing ? "Pause" : "Play"}
              </Button>
              <Button type="button" variant="outline" onClick={() => jump(ticks, time, -1, seek)}>
                Prev mark
              </Button>
              <Button type="button" variant="outline" onClick={() => jump(ticks, time, 1, seek)}>
                Next mark
              </Button>
              <span className="font-mono text-sm text-muted-foreground">
                {Math.round(time)} / {trace.durationMs} ms
                <span className="ml-3">seed {trace.seed}</span>
              </span>
            </div>

            <div className="relative mb-2 h-8">
              {ticks.map((event, index) => (
                <button
                  key={index}
                  type="button"
                  className="absolute top-1 size-2.5 -translate-x-1/2 rounded-full"
                  style={{
                    left: `${(event.t / Math.max(1, trace.durationMs)) * 100}%`,
                    background: markerColor(event),
                  }}
                  title={describe(event)}
                  aria-label={describe(event)}
                  onClick={() => seek(event.t)}
                />
              ))}
            </div>
            <Slider
              aria-label="Virtual time"
              min={0}
              max={trace.durationMs}
              step={1}
              value={[time]}
              onValueChange={(value) => {
                const next = Array.isArray(value) ? value[0] : value;
                if (typeof next === "number") {
                  seek(next);
                }
              }}
            />
            <div className="mt-2 flex flex-wrap gap-3 font-mono text-[11px] text-muted-foreground">
              <Legend swatch="#e3a008" label="Leader" />
              <Legend swatch="#d64545" label="Candidate" />
              <Legend swatch="#e07a3d" label="Pre-candidate" />
              <Legend swatch="#8aa0b8" label="Follower" />
              <Legend swatch="#5c6770" label="Down" />
              <span className="hatch rounded px-1.5">partition</span>
            </div>

            {frame.groups && (
              <p className="mt-3 font-mono text-xs text-primary">
                Partition {frame.groups.map((group) => group.join(" ")).join("  |  ")}
              </p>
            )}

            <div className="relative mt-3">
              <svg
                className="pointer-events-none absolute inset-y-0 left-0 hidden w-10 sm:block"
                style={{ height: trace.nodes.length * ROW }}
                aria-hidden
              >
                {inflight.map(({ event, index }) => {
                  const from = trace.nodes.indexOf(event.from ?? "");
                  const to = trace.nodes.indexOf(event.to ?? "");
                  if (from < 0 || to < 0) {
                    return null;
                  }
                  const progress = Math.min(1, (time - event.t) / RPC_WINDOW_MS);
                  const y = (from + (to - from) * progress) * ROW + ROW / 2;
                  return (
                    <circle
                      key={index}
                      cx="16"
                      cy={y}
                      r="4"
                      fill={event.ok === false ? "#d64545" : "#e3a008"}
                    />
                  );
                })}
              </svg>
              <div className="sm:pl-10">
                {trace.nodes.map((id) => {
                  const view = frame.nodes[id];
                  const role = view?.crashed ? "down" : view?.role ?? "follower";
                  const cut = isolated(frame.groups, id);
                  return (
                    <article
                      key={id}
                      className={`mb-2 rounded-lg border border-border bg-card p-3 ${cut ? "hatch" : ""}`}
                      style={{ minHeight: ROW - 8, borderLeft: `4px solid ${roleColor(role)}` }}
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <h2 className="font-mono text-sm font-medium">{id}</h2>
                        <Badge
                          variant="outline"
                          style={{ borderColor: roleColor(role), color: roleColor(role) }}
                        >
                          {roleLabel(role)}
                        </Badge>
                        <span className="font-mono text-[11px] text-muted-foreground">
                          term {view?.term ?? 0} · commit {view?.commit ?? 0} · applied {view?.applied ?? 0}
                          {view && view.snapshot > 0 ? ` · snap ${view.snapshot}` : ""}
                          {view?.votedFor ? ` · voted ${view.votedFor}` : ""}
                        </span>
                      </div>
                      <LogStrip view={view} />
                    </article>
                  );
                })}
              </div>
            </div>

            {inflight.length > 0 && (
              <ul className="mt-2 space-y-1 font-mono text-[11px] text-muted-foreground sm:hidden">
                {inflight.slice(-4).map(({ event, index }) => (
                  <li key={index}>{describe(event)}</li>
                ))}
              </ul>
            )}
          </section>

          <aside className="rounded-lg border border-border bg-card p-3">
            <h2 className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
              Events through {Math.round(time)} ms
            </h2>
            <ol className="mt-3 space-y-2">
              {recent.length === 0 && (
                <li className="text-sm text-muted-foreground">Nothing has happened yet.</li>
              )}
              {recent.map(({ event, index }) => (
                <li key={index}>
                  <button
                    type="button"
                    className="w-full rounded-md px-1 py-1 text-left hover:bg-muted"
                    onClick={() => seek(event.t)}
                  >
                    <span className="font-mono text-[11px] text-primary">{event.t} ms</span>
                    <span className="mt-0.5 block text-sm">{describe(event)}</span>
                  </button>
                </li>
              ))}
            </ol>
          </aside>
        </div>
      )}
    </Shell>
  );
}

function Shell({ children }: { children: React.ReactNode }) {
  return <main className="mx-auto min-h-screen max-w-6xl px-4 py-6 sm:px-6">{children}</main>;
}

function Legend({ swatch, label }: { swatch: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className="inline-block size-2 rounded-full" style={{ background: swatch }} />
      {label}
    </span>
  );
}

function LogStrip({ view }: { view: { commit: number; log?: { i: number; t: number; op: string; key?: string; value?: string }[] } | undefined }) {
  const cells = view?.log ?? [];
  if (cells.length === 0) {
    return <p className="mt-3 font-mono text-[11px] text-muted-foreground">Log empty</p>;
  }
  return (
    <div className="mt-3 flex gap-1 overflow-x-auto pb-1">
      {cells.map((cell) => {
        const committed = cell.i <= (view?.commit ?? 0);
        return (
          <div
            key={cell.i}
            className={`min-w-16 rounded border px-1.5 py-1 font-mono text-[10px] leading-tight ${
              committed ? "border-border bg-background" : "border-dashed border-muted-foreground/50"
            }`}
            title={`index ${cell.i} term ${cell.t} ${cell.op}${cell.key ? " " + cell.key : ""}${cell.value ? "=" + cell.value : ""}`}
          >
            <div className="text-muted-foreground">
              {cell.i}
              <span className="float-right">t{cell.t}</span>
            </div>
            <div style={{ color: cell.op === "DELETE" ? "#d64545" : cell.op === "NOOP" ? "#93a3b5" : "#e7edf2" }}>
              {cell.op === "PUT" ? cell.key : cell.op === "DELETE" ? cell.key : "noop"}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function describe(event: TraceEvent): string {
  switch (event.kind) {
    case "role":
      return `${event.node} became ${event.role} in term ${event.term}`;
    case "partition":
      return "Network partition " + (event.groups ?? []).map((group) => group.join("+")).join(" | ");
    case "heal":
      return "Partition healed";
    case "crash":
      return `${event.node} crashed`;
    case "restart":
      return `${event.node} restarted from disk`;
    case "snapshot":
      return `${event.node} installed a snapshot through index ${event.index}`;
    case "client":
      return `${event.client} ${event.op} ${event.key}${event.value != null ? "=" + event.value : ""} → ${event.status}`;
    case "rpc": {
      const extra = event.entries ? ` · ${event.entries} entries` : "";
      const verdict = event.ok == null ? "" : event.ok ? " ok" : " rejected";
      return `${event.rpc} ${event.from} → ${event.to}${verdict}${extra}`;
    }
    default:
      return event.kind;
  }
}

function markerColor(event: TraceEvent): string {
  if (event.kind === "role") {
    return roleColor(event.role ?? "follower");
  }
  if (event.kind === "partition" || event.kind === "crash") {
    return "#d64545";
  }
  if (event.kind === "heal" || event.kind === "restart") {
    return "#8aa0b8";
  }
  if (event.kind === "snapshot") {
    return "#e3a008";
  }
  return "#93a3b5";
}

function jump(events: TraceEvent[], time: number, direction: 1 | -1, seek: (next: number) => void) {
  const ordered = direction > 0 ? events : [...events].reverse();
  const hit = ordered.find((event) => (direction > 0 ? event.t > time + 0.5 : event.t < time - 0.5));
  if (hit) {
    seek(hit.t);
  }
}
