export type Direction = "in" | "out";

export interface TauMessage {
  id: number;
  direction: Direction;
  peer: string | null;
  peer_name: string;
  text: string;
  sent_at: number;
  received_at: number;
  status: string;
  rtt_ms?: number;
}

export interface Peer {
  destination: string;
  name: string;
  last_seen: number;
  age: number;
  hops: number;
  protocol: number;
}

export interface InterfaceInfo {
  name: string;
  online: boolean;
}

export interface NodeInfo {
  name: string;
  destination: string;
  protocol: number;
  version: string;
  rns_version: string;
  uptime: number;
  peers: number;
  interfaces: InterfaceInfo[];
}

export interface Counts {
  messages: number;
  peers: number;
}

export interface BridgeInfo {
  version: string;
  web: boolean;
  url: string;
}

export interface Snapshot {
  node: NodeInfo;
  peers: Peer[];
  messages: TauMessage[];
  counts: Counts;
  bridge: BridgeInfo;
}

export interface PingResult {
  ok: boolean;
  peer: Peer;
  server?: string;
  rtt_ms: number | null;
}

export interface SendResponse {
  ok: boolean;
  message: TauMessage;
  response: Record<string, unknown>;
}

export interface ApiErrorBody {
  error?: { message?: string };
}
