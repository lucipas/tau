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
  // Outbox entry id, present while the message is queued for delivery.
  mail_id?: string;
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


export interface TanReplyParent {
  hash: string;
  author: string;
  name: string;
  text: string;
  deleted: boolean;
}

export interface TanPost {
  v: number;
  author: string;
  key: string;
  name: string;
  text: string;
  created_at: number;
  prev: string;
  hash: string;
  sig: string;
  kind?: string;
  target?: string;
  reply_to?: TanReplyParent | null;
  likes?: number;
  liked?: boolean;
  own?: boolean;
}

export interface TanFollow {
  author: string;
  name: string;
  added_at: number;
  posts?: number;
  seq?: number;
  last_post?: number;
}

export interface TanTimelineResponse {
  posts: TanPost[];
  count: number;
}

export interface TanFollowingResponse {
  following: TanFollow[];
  count: number;
}

export interface TanPostResponse {
  ok: boolean;
  post: TanPost;
}

export interface TanSyncResponse {
  ok: boolean;
  sync: {
    peers: number;
    authors: number;
    stored: number;
    duplicates: number;
    errors: Array<{ peer: string; author: string; error: string }>;
  };
}
