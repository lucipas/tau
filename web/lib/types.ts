// types.ts — TypeScript shapes for the tau web bridge (tau/webapi.py).
//
// Every type here is derived from the bridge's JSON: `WebBridge.snapshot`,
// `Peer.local_dict`, `_clean_stored_message`, the Tan store annotations in
// `tau/tangents/tan.py`, channel rows in `tau/channels.py`, page/profile
// display views in `tau/pages.py` / `tau/profile.py`, and the mailbox entry
// dicts in `tau/mailbox.py`. Timestamps are unix seconds (floats).
// Fields the bridge only sometimes sends are optional; fields it never
// documents are typed loosely (`unknown` / index signatures) rather than
// guessed.

export interface ApiErrorBody {
  error?: { message?: string; code?: string; queued?: number };
  message?: string;
}

// ---------------------------------------------------------------- node ---

export interface NodeInterface {
  name: string;
  online: boolean;
}

export interface MailboxCounts {
  outbox: number;
  held: number;
  seen: number;
  sent: number;
  receipts: number;
  retracted: number;
  sent_retracted: number;
}

export interface NodeInfo {
  name: string;
  destination: string;
  protocol: number;
  version: string;
  rns_version: string;
  uptime: number;
  peers: number;
  mailbox: MailboxCounts;
  interfaces: NodeInterface[];
  lxmf: { enabled: boolean; delivery: string | null; propagation: string | null };
}

export interface SnapshotCounts {
  messages: number;
  peers: number;
}

export interface UnreadCounts {
  per_peer: Record<string, number>;
  per_group: Record<string, number>;
  per_channel: Record<string, number>;
  total: number;
  revision: number;
}

export interface Snapshot {
  node: NodeInfo;
  peers: Peer[];
  groups: Group[];
  messages: ChatMessage[];
  counts: SnapshotCounts;
  unread: UnreadCounts;
  bridge: {
    version: string;
    web: boolean;
    url: string;
    notes_editable: boolean;
  };
  social: { enabled: boolean; posts: number };
}

// ---------------------------------------------------------------- peers --

export interface PeerProfile {
  author: string;
  name: string;
  bio: string;
  avatar: string;
  links: string[];
  version: number;
  created_at: number;
}

export interface Peer {
  /** 32-char lowercase hex destination hash. */
  destination: string;
  name: string;
  last_seen: number;
  /** Seconds since the peer was last seen, rounded to 0.1. */
  age: number;
  hops: number | null;
  protocol: number | null;
  /** LXMF delivery hash, or null. */
  lxmf: string | null;
  /** Owner-only local alias ("" when none). */
  alias: string;
  /** Private note; only present for loopback clients. */
  note?: string;
  verified: boolean;
  verified_at: number | null;
  key_change_at: number | null;
  name_change_at: number | null;
  previous_name: string | null;
  /** Display name of the verified peer / saved contact this unverified
   *  peer may be impersonating, or null. Added by the bridge. */
  lookalike_of: string | null;
  blocked: boolean;
  /** True when this conversation is pinned to the top of the list. */
  pinned?: boolean;
  profile: PeerProfile | null;
  /** Live path-table reachability (from /api/mesh, merged into /api/peers). */
  reachable?: boolean;
  live_hops?: number | null;
}

export interface MeshRow {
  destination: string;
  name: string;
  hops: number | null;
  has_path: boolean;
  reachable: boolean;
  last_seen: number;
  verified: boolean;
}

export interface HeardRow {
  /** Shape is whatever the node's foreign-announce record carries:
   *  destination hash, name and last-seen at minimum. Kept loose —
   *  the bridge returns the stored records verbatim. */
  destination?: string;
  name?: string;
  last_seen?: number;
  [key: string]: unknown;
}

export interface PeerInfo {
  name?: string;
  version?: string;
  rns_version?: string;
  protocol?: number;
  uptime?: number;
  peers?: number;
  lxmf_enabled?: boolean;
}

export interface PingResult {
  /** Shape returned by WebBridge.ping. */
  ok: boolean;
  /** Peer's public_dict (destination, name, last_seen, age, hops...). */
  peer: {
    destination: string;
    name: string;
    last_seen: number;
    age: number;
    hops: number | null;
    protocol: number | null;
    lxmf: string | null;
  };
  server?: unknown;
  rtt_ms: number | null;
}

export interface SafetyNumberResult {
  peer: string;
  name: string;
  safety_number: string;
}

export interface VerifyResult {
  peer: string;
  name: string;
  safety_number: string;
  verified_at: number | null;
  reverified: boolean;
  fingerprint_changed: boolean;
}

export interface BlockResult {
  peer: { destination: string; name: string };
  blocked: boolean;
  changed: boolean;
}

export interface ArchiveResult {
  destination: string;
  name: string;
  archived: boolean;
  changed: boolean;
}

// --------------------------------------------------------------- groups --

export interface GroupMember {
  hash: string;
  name: string;
}

export interface Group {
  id: string;
  name: string;
  members: GroupMember[];
  created_at: number;
  topic: string;
  created_by_me: boolean;
  can_leave: boolean;
  ignored: boolean;
  /** True when this conversation is pinned to the top of the list. */
  pinned?: boolean;
}

export interface GroupChangeResult {
  group: Group;
  previous_id: string;
}

export interface LeftGroup {
  id: string;
  left_at?: number;
  [key: string]: unknown;
}

export interface LeftGroups {
  left: LeftGroup[];
  ignored: { id: string; name: string }[];
}

// ---------------------------------------------------------- broadcasts --
// A broadcast list is a named set of peers for one-to-many DMs: a send
// fans out as one ordinary DM per member, so recipients never see each
// other or the list. Lists are local-only, like groups, but carry no
// shared context and no protocol of their own.

export interface BroadcastMember {
  hash: string;
  name: string;
}

export interface BroadcastList {
  name: string;
  members: BroadcastMember[];
  created_at: number;
}

export interface BroadcastSendResult {
  hash: string;
  name: string;
  status: "delivered" | "queued" | "failed";
  error?: string;
}

export interface BroadcastSendResponse {
  ok: boolean;
  name: string;
  results: BroadcastSendResult[];
  delivered: number;
  queued: number;
  failed: number;
}

export interface JoinRequest {
  group: string;
  peer: string;
  peer_name: string;
  group_id: string;
  created_at?: number;
  [key: string]: unknown;
}

export interface GroupPollTally {
  options: string[];
  counts: number[];
  total: number;
  closed: boolean;
  voters: { author: string; name: string; option: number; voted_at: number }[];
  my_vote: number | null;
}

// ------------------------------------------------------------- messages --

export interface MessageReaction {
  reactor: string;
  name: string;
  reaction: string;
}

export interface StoredFile {
  name: string;
  size: number;
  sha256: string;
}

export interface ReplyRef {
  /** Wire id of the quoted message. */
  wire_id?: string;
  /** Console message id of the quoted message, when resolvable. */
  id?: number;
  excerpt?: string;
  excerpt_hash?: string;
  peer_name?: string;
  [key: string]: unknown;
}

export interface LocationPin {
  latitude: number;
  longitude: number;
  label?: string;
  [key: string]: unknown;
}

export interface LiveBeacon {
  latitude: number;
  longitude: number;
  expires_at?: number;
  [key: string]: unknown;
}

export interface GroupPollVote {
  poll: string;
  option: number;
  [key: string]: unknown;
}

export interface GroupPollClose {
  poll: string;
  [key: string]: unknown;
}

export type MessageStatus =
  | "queued"
  | "sent"
  | "delivered"
  | "read"
  | "failed"
  | string;

export interface ChatMessage {
  id: number;
  direction: "in" | "out";
  /** Destination hash of the other side (null for some system entries). */
  peer: string | null;
  peer_name: string;
  text: string;
  sent_at: number;
  received_at: number;
  status: MessageStatus;
  expires_at?: number;
  /** Unix time the message is scheduled to send (still queued). */
  send_after?: number;
  rtt_ms?: number;
  read_at?: number;
  edited_at?: number;
  mail_id?: string;
  group?: string;
  group_name?: string;
  wire_id?: string;
  delivered_via?: "direct" | "lxmf";
  reactions: MessageReaction[];
  in_reply_to?: ReplyRef;
  location?: LocationPin;
  beacon?: LiveBeacon;
  /** Group or DM poll question carried on the message itself. */
  poll_options?: string[];
  poll_vote?: GroupPollVote;
  poll_close?: GroupPollClose;
  /** Group or DM poll tally, attached by the bridge from the node inbox. */
  poll?: GroupPollTally;
  file?: StoredFile;
  excerpt_hash?: string;
  excerpt_hashes?: string[];
  /** True when the node inbox has this message pinned. */
  pinned?: boolean;
}

export interface SendResult {
  ok: boolean;
  message: ChatMessage;
  /** Node send response (delivery/queue detail); shape varies by transport. */
  response: Record<string, unknown>;
}

export interface DmRequest {
  id: string;
  sender: string;
  sender_name: string;
  text?: string;
  received_at?: number;
  [key: string]: unknown;
}

export interface DisappearingDefaults {
  peers: Record<string, number>;
  groups: Record<string, number>;
  channels: Record<string, number>;
}

export interface QuietHoursSettings {
  enabled: boolean;
  start: string;
  end: string;
  active: boolean;
  minutes_until_end: number | null;
}

export interface AwayMessageSettings {
  enabled: boolean;
  message: string;
  since: number | null;
  replied_to: string[];
  replied_count: number;
}

export interface MutedConversations {
  peers: string[];
  groups: string[];
}

// --------------------------------------------------------------- outbox --

export interface OutboxGroup {
  id: string;
  name: string;
  creator: string;
  members: string[];
}

export interface OutboxEntry {
  id: string;
  to: string;
  to_name: string;
  text: string;
  kind: string;
  created_at: number;
  expires_at: number;
  attempts: number;
  last_attempt_at: number | null;
  last_error: string | null;
  next_attempt_at: number | null;
  /** Held-by map (peer hash -> count) in the mailbox dict; the bridge
   *  re-sorts it into a list for the console. */
  held_by: string[] | Record<string, number>;
  message_expires_at: number | null;
  send_after: number | null;
  sent_at: number | null;
  edited_at: number | null;
  group?: OutboxGroup;
  read_at?: number | null;
}

export interface Outbox {
  outbox: OutboxEntry[];
  count: number;
  held_for_others: number;
}

export interface HeldEnvelope {
  id: string;
  to: string;
  to_name: string;
  from: string;
  from_name: string;
  stored_at: number;
  expires_at: number;
  blob_bytes: number;
  note?: string;
}

export interface OutboxRetryResult {
  ok: boolean;
  delivered: number;
  deposited: number;
  received: number;
  still_queued: number;
  lock_busy: boolean;
  target?: string;
  target_delivered?: boolean;
  target_still_queued?: boolean;
}

// ------------------------------------------------------------- settings --

export interface NodeSettings {
  accept_files: boolean;
  send_read_receipts: boolean;
}

export interface AllowList {
  enabled: boolean;
  entries: { destination: string; name: string }[];
}

export interface DoctorCheck {
  name: string;
  ok?: boolean;
  status?: string;
  message?: string;
  detail?: string;
  [key: string]: unknown;
}

export interface DoctorReport {
  ok: boolean;
  checks: DoctorCheck[];
}

// ------------------------------------------------------------------ Tan --

export type TanKind =
  | "post"
  | "reply"
  | "like"
  | "unlike"
  | "repost"
  | "quote"
  | "poll"
  | "vote"
  | "delete"
  | "edit"
  | string;

export interface TanPollTally {
  options: string[];
  counts: number[];
  total: number;
  closed: boolean;
  /** Index of the viewer's own standing vote, or null. */
  voted: number | null;
}

export interface TanImage {
  sha256: string;
  size?: number;
  mime?: string;
  [key: string]: unknown;
}

export interface TanParentSummary {
  hash: string;
  author: string;
  name: string;
  text: string;
  deleted?: boolean;
  expired?: boolean;
  muted?: boolean;
  hidden?: boolean;
  [key: string]: unknown;
}

export interface TanPost {
  v: number;
  /** 64-char hex sha256 — the post's id. */
  hash: string;
  author: string;
  /** Author's destination hash (same as `author`). */
  key: string;
  name: string;
  text: string;
  created_at: number;
  prev: string;
  sig: string;
  kind: TanKind;
  /** Hash of the post this entry targets (reply parent / quote original /
   *  poll / liked post), "" when none. */
  target: string;
  expires_at: number | null;
  options?: string[];
  image?: TanImage | null;
  // --- annotations added by the bridge (tau/tangents/tan.py) ---
  likes: number;
  liked: boolean;
  own: boolean;
  /** Standing quote count. */
  quotes: number;
  /** Reply count (when annotated). */
  replies?: number;
  /** This post's own text is a reply: parent summary, or null. */
  reply_to?: TanParentSummary | null;
  /** This post quotes another: the quoted original, or null. */
  quoted?: TanParentSummary | null;
  poll?: TanPollTally;
  saved?: boolean;
  pinned?: boolean;
  /** Repost wrapper: the reposted original, when the entry is a repost. */
  reposted?: TanPost | null;
  [key: string]: unknown;
}

export interface TanTag {
  tag: string;
  count: number;
}

export interface TanAuthor {
  author: string;
  name: string;
  posts: number;
  followers?: number;
  following?: boolean;
  last_post_at?: number;
  [key: string]: unknown;
}

export interface TanLiker {
  author: string;
  name: string;
  liked_at: number;
}

export interface TanReposter {
  author: string;
  name: string;
  reposted_at: number;
}

export interface TanQuoter {
  author: string;
  name: string;
  hash: string;
  text: string;
  quoted_at: number;
}

export interface TanReactors {
  target: string;
  likers?: TanLiker[];
  reposters?: TanReposter[];
  quoters?: TanQuoter[];
  count: number;
  total: number;
  deleted?: boolean;
}

export interface ScheduledTanPost {
  id: string;
  text: string;
  reply_to: string | null;
  to: string[];
  ttl: number;
  publish_at: number;
  created_at: number;
  post_hash: string | null;
  attempts: number;
  next_attempt_at: number | null;
  paused_at: number | null;
  expires_in?: number;
  [key: string]: unknown;
}

export interface TanRules {
  /** Shape is the ruleset's to_dict(): kinds allow/deny lists and
   *  limits. Kept open — the UI reads known keys defensively. */
  default_allow?: boolean;
  [key: string]: unknown;
}

export interface TanVerifyReport {
  ok?: boolean;
  [key: string]: unknown;
}

// ------------------------------------------------------------- channels --

export interface Channel {
  channel: string;
  joined_at: number;
  /** True when this channel is pinned to the top of the list. */
  pinned?: boolean;
  /** Messages newer than the last-read cursor; always present from /api/channels. */
  unread: number;
}

export interface ChannelPollTally {
  options: string[];
  counts: number[];
  total: number;
  closed: boolean;
  voters: { author: string; name: string; option: number; voted_at: number }[];
  my_vote: number | null;
  /** True when this node authored the poll (so it may close it). */
  own: boolean;
}

export interface ChannelReaction {
  reaction: string;
  count: number;
  reactors: string[];
}

export interface ChannelMessage {
  v: number;
  hash: string;
  channel: string;
  author: string;
  key: string;
  name: string;
  text: string;
  created_at: number;
  received_at: number;
  sig: string;
  /** Unix timestamp after which honest nodes purge this message, when set. */
  expires_at?: number;
  /** Channel poll question carried on the message itself. */
  poll_options?: string[];
  poll_vote?: { target: { author: string; sent_at: number }; option: number };
  poll_close?: { author: string; sent_at: number };
  /** Channel poll tally, attached by the bridge from the channel store. */
  poll?: ChannelPollTally;
  /** Present on pins: when this node pinned the message. */
  pinned_at?: number;
  /** Reaction summary, grouped by token; absent on old servers. */
  reactions?: ChannelReaction[];
  /** Digest of the parent message when this message is a reply. */
  reply_to?: string;
  /** Present when the author edited this message; the text shown is the latest edit. */
  edited?: boolean;
  edited_at?: number;
}

export interface ChannelTopic {
  v: number;
  hash: string;
  channel: string;
  author: string;
  key: string;
  topic: string;
  updated_at: number;
  sig: string;
}

export interface ScheduledChannelMessage {
  id: string;
  channel: string;
  text: string;
  publish_at: number;
  created_at: number;
  to: string[];
  ttl: number;
  message_hash: string | null;
  attempts: number;
  next_attempt_at: number | null;
  paused_at: number | null;
  [key: string]: unknown;
}

// ---------------------------------------------------------------- pages --

export interface PageFile {
  name: string;
  sha256: string;
  size?: number;
  mime?: string;
  [key: string]: unknown;
}

export interface MeshPage {
  author: string;
  title: string;
  markdown: string;
  files: PageFile[];
  version: number;
  updated_at: number;
}

export interface PageList {
  pages: MeshPage[];
  mine: MeshPage | null;
  count: number;
}

// ------------------------------------------------------- live events -----

/** One frame from /ws, or one SSE event from /api/events. */
export type LiveEvent =
  | { type: "snapshot"; snapshot: Snapshot }
  | { type: "message"; message: ChatMessage; unread: UnreadCounts }
  | { type: "message_updated"; message: ChatMessage }
  | { type: "message_deleted"; message_id: number }
  | { type: "peer"; peer: Peer }
  | { type: "post"; post: TanPost }
  | { type: "channel-message"; message: ChannelMessage }
  | { type: "unread"; unread: UnreadCounts; restored?: unknown[] }
  | { type: "typing"; peer?: string; group?: string; state?: string; name?: string }
  | { type: "status"; action: string }
  | { type: string; [key: string]: unknown };

export interface TypingState {
  peer?: string;
  group?: string;
  name?: string;
  state: "start" | "stop";
  at: number;
}
