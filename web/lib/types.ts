export type Direction = "in" | "out";

export interface MessageReaction {
  // Reactor's destination hash.
  reactor: string;
  // Reactor's display name at reaction time.
  name: string;
  // One emoji or short text token; never an image.
  reaction: string;
}

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
  // Reticulum wire id: how replies name this message, and how
  // reactions are matched to it.
  wire_id?: string;
  // Latest reaction per reactor, oldest first.
  reactions?: MessageReaction[];
  // Short hash of this message's text, for checking a reply's quoted
  // excerpt against it; see resolveQuote in message-feed.tsx.
  excerpt_hash?: string;
  // Quoted message reference: wire id + short excerpt hash of the text
  // the sender quoted. The quote is rendered from local history only;
  // see resolveQuote in message-feed.tsx.
  in_reply_to?: { id: string; excerpt: string | null } | null;
}

export interface Peer {
  destination: string;
  name: string;
  last_seen: number;
  age: number;
  hops: number;
  protocol: number;
  // LXMF delivery destination hash, derived from the peer's public key.
  lxmf?: string | null;
  // Local trust state, shown only to this node's owner (never sent to peers).
  verified?: boolean;
  verified_at?: number | null;
  key_change_at?: number | null;
  // True when the peer is on this node's block-list: their DMs are
  // dropped and their Tan posts hidden.
  blocked?: boolean;
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

// Per-conversation unread counts from the node inbox: how many
// received DMs sit after each sender's read cursor. Read state is
// local to the node; the console only displays and clears it.
export interface UnreadCounts {
  per_peer: Record<string, number>;
  total: number;
  // Stamped by the node; a count with an older revision than one
  // already applied is stale and is ignored.
  revision?: number;
}

export interface MarkReadResponse {
  ok: boolean;
  unread: UnreadCounts;
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
  unread: UnreadCounts;
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

export interface OutboxEntry {
  id: string;
  to: string;
  to_name: string;
  text: string;
  created_at: number;
  expires_at: number;
  attempts: number;
  last_attempt_at: number | null;
  last_error: string | null;
  held_by: string[];
}

export interface OutboxResponse {
  outbox: OutboxEntry[];
  count: number;
  held_for_others: number;
}

export interface CancelOutboxResponse {
  ok: boolean;
  cancelled: OutboxEntry;
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
  // True when the parent is from a blocked author: its text is blanked.
  blocked?: boolean;
  muted?: boolean;
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
  quoted?: TanReplyParent | null;
  likes?: number;
  liked?: boolean;
  quotes?: number;
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
