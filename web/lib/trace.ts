export type LogCell = {
  i: number;
  t: number;
  op: string;
  key?: string;
  value?: string;
};

export type NodeView = {
  role: string;
  term: number;
  commit: number;
  applied: number;
  lastIndex: number;
  snapshot: number;
  votedFor: string | null;
  leader: string | null;
  crashed: boolean;
  log?: LogCell[];
};

export type TraceEvent = {
  t: number;
  kind: string;
  nodes?: Record<string, NodeView>;
  node?: string;
  role?: string;
  term?: number;
  groups?: string[][];
  index?: number;
  op?: string;
  key?: string;
  status?: string;
  client?: string;
  value?: string;
  found?: boolean;
  rpc?: string;
  from?: string;
  to?: string;
  ok?: boolean;
  entries?: number;
};

export type Trace = {
  id: string;
  title: string;
  blurb: string;
  seed: number;
  nodes: string[];
  durationMs: number;
  events: TraceEvent[];
};

export type Scenario = {
  id: string;
  title: string;
  blurb: string;
  file: string;
};

export type Frame = {
  nodes: Record<string, NodeView>;
  groups: string[][] | null;
};

export function frameAt(trace: Trace, time: number): Frame {
  let nodes: Record<string, NodeView> = {};
  let groups: string[][] | null = null;
  for (const event of trace.events) {
    if (event.t > time) {
      break;
    }
    if (event.kind === "state" && event.nodes) {
      nodes = event.nodes;
    } else if (event.kind === "partition" && event.groups) {
      groups = event.groups;
    } else if (event.kind === "heal") {
      groups = null;
    }
  }
  return { nodes, groups };
}

export function markers(trace: Trace): TraceEvent[] {
  return trace.events.filter((event) => event.kind !== "state" && event.kind !== "rpc");
}

export function roleColor(role: string): string {
  switch (role) {
    case "leader":
      return "#e3a008";
    case "candidate":
      return "#d64545";
    case "pre-candidate":
      return "#e07a3d";
    case "down":
      return "#5c6770";
    default:
      return "#8aa0b8";
  }
}

export function roleLabel(role: string): string {
  switch (role) {
    case "leader":
      return "Leader";
    case "candidate":
      return "Candidate";
    case "pre-candidate":
      return "Pre-candidate";
    case "down":
      return "Down";
    default:
      return "Follower";
  }
}

export function isolated(groups: string[][] | null, id: string): boolean {
  if (!groups) {
    return false;
  }
  const mine = groups.find((group) => group.includes(id));
  return mine != null && mine.length === 1;
}
