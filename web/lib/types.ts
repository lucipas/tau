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

/** Result of adding a peer from a shared contact card. */
export interface AddPeerResult {
  /** 32-char lowercase hex destination hash of the added peer. */
  peer: string;
  name: string;
  /** True when the peer was already in the registry. */
  known: boolean;
  verified: boolean;
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
  /** True when only the group's creator can post. Absent on older payloads: reads as open. */
  announcements_only?: boolean;
  /** True when the group is secret: an opaque id, and only members know it exists or who is in it. */
  secret?: boolean;
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
  requested_at?: number;
  expires_at?: number;
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
  /** A voice note: the console renders an audio player, not a download link. */
  voice?: boolean;
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
  /** Decimal degrees, -90..90. */
  lat: number;
  /** Decimal degrees, -180..180. */
  lon: number;
  /** Optional place name, at most 200 characters. */
  name?: string;
  [key: string]: unknown;
}

export interface LiveBeacon {
  /** Beacon id (8 random bytes, hex). */
  id: string;
  /** Unix time the beacon stops sharing. */
  expires_at: number;
  /** Update sequence number, 1-based. */
  seq: number;
  [key: string]: unknown;
}

/** A live beacon this node is sharing, as served by GET /api/beacons. */
export interface Beacon {
  id: string;
  /** Destination hash of the DM peer, or the group id. */
  target: string;
  target_kind: "dm" | "group";
  target_name: string;
  started_at: number;
  expires_at: number;
  /** Seconds between updates. */
  interval: number;
  coarse: boolean;
  location: { lat: number; lon: number; name?: string };
  location_file?: string;
  next_update_at: number;
  updates_sent: number;
  stopped: boolean;
  /** Still sharing: not stopped and unexpired. */
  active: boolean;
  expires_in_seconds: number;
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
  /** Shared contact card on a top-level message, if any. */
  contact?: { name: string; destination_hash: string };
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
  channels: string[];
}

/** One unsent message parked for a peer or a group (``tau draft save``). */
export interface MessageDraft {
  kind: "peer" | "group" | "channel";
  /** Peer destination hash, group id, or channel name. */
  target: string;
  text: string;
  updated_at: number;
  /** Best-effort conversation name from the bridge; null when unknown. */
  name: string | null;
}

/** A pending message reminder (``tau remind add``): snooze one stored
 * DM or group message and be re-pinged when it is due. */
export interface Reminder {
  /** Reminder id (16 hex chars); a unique prefix cancels it. */
  id: string;
  /** Inbox record id of the snoozed message. */
  message_id: string;
  kind: "peer" | "group";
  /** Peer destination hash or group id. */
  target: string;
  /** Best-effort conversation name from the bridge. */
  target_name: string;
  /** Text snapshot taken when the reminder was set. */
  text: string;
  /** Who sent the message, as labelled when the reminder was set. */
  sender: string;
  /** Unix time the reminder fires. */
  due_at: number;
  created_at: number;
}

export interface OwnProfile {
  author: string | null;
  name: string;
  bio: string;
  avatar: string | null;
  links: string[];
  version: number;
  created_at: number | null;
  published: boolean;
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
  /** Poll this leg carries, if any — the API only includes it when set. */
  poll_options?: string[];
  poll_vote?: Record<string, unknown>;
  poll_close?: Record<string, unknown>;
  /** Live-beacon marker this leg carries, if any. */
  beacon?: Record<string, unknown>;
  /** Static location pin this leg carries, if any. */
  location?: Record<string, unknown>;
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
  tan_sync_depth: number;
  announce_interval: number;
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

/** Result of verifying the newest automatic backup (`POST /api/backup/verify`). */
export interface BackupVerifyResult {
  backup: string;
  node: string | null;
  destination: string;
  files: string[];
  encrypted: boolean;
  valid: boolean;
}

/** One outstanding pairing offer this node issued (`tau devices list`). */
export interface DeviceOffer {
  token: string;
  created_at: number;
  expires_at: number;
  owner: string | null;
}

/** Linked-devices state: this device, the pairing it redeemed, offers issued. */
export interface DevicesInfo {
  identity: string | null;
  destination: string | null;
  paired_from: {
    token: string;
    primary: string;
    paired_at: number;
  } | null;
  issued_offers: DeviceOffer[];
}

/** Result of minting a pairing bundle: token, expiry, and its download URL. */
export interface PairDeviceResult {
  ok: boolean;
  token: string;
  expires_at: number;
  history_messages: number;
  download: string;
  filename: string;
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

/** A place attached to a Tan post: decimal degrees, optional place name. */
export interface TanLocation {
  lat: number;
  lon: number;
  name?: string;
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
  location?: TanLocation | null;
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

export interface TanList {
  name: string;
  created_at: number;
  members: number;
}

export interface TanListMember {
  author: string;
  name: string;
  added_at: number;
  posts?: number;
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
  /** "post" for a queued post, "repost" for a queued repost, "quote" for a queued quote, "poll" for a queued poll question. */
  kind?: "post" | "repost" | "quote" | "poll";
  /** Hash of the post being reposted or quoted; only present for those kinds. */
  target_post?: string | null;
  /** Fixed poll choices; only present when kind is "poll". */
  options?: string[];
  /** Image attachment descriptor, when the queued post carries one. */
  image?: { sha256: string; size: number; mime: string; name: string } | null;
  [key: string]: unknown;
}

/** A saved Tan post draft: plain text plus the reply target and the
 *  expiry it will carry once published (mirrors the CLI's tan draft). */
export interface TanDraft {
  id: number;
  text: string;
  reply_to: string;
  expires_in: number | null;
  post_hash: string;
  pending_peers: string[];
  claimed_at: number | null;
  created_at: number;
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
  /** True when the channel is muted: its badge stays dark. */
  muted?: boolean;
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
  /** Static location pin on a top-level message, if any. */
  location?: LocationPin;
  /** Shared contact card on a top-level message, if any. */
  contact?: { name: string; destination_hash: string };
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
  /** "message" for a queued message, "poll" for a queued poll question. */
  kind?: "message" | "poll";
  /** Fixed poll choices; only present when kind is "poll". */
  options?: string[];
  /** Static location pin on a queued message, if any. */
  location?: LocationPin;
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
  | { type: "groups"; groups: Group[] }
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
