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
  // Unix time the recipient read this outbound message, when known.
  read_at?: number;
  // Outbox entry id, present while the message is queued for delivery.
  mail_id?: string;
  // Group DM context, present on group messages (inbound, and
  // outbound legs recorded per member on a console group send).
  group?: string;
  group_name?: string;
  // Sender-set expiry (epoch seconds). Absent when the message never
  // expires; the console purges it once the deadline passes.
  expires_at?: number;
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
  // File metadata, present on file entries: the name, size and
  // SHA-256 of the file this message carried. Received files can be
  // downloaded from GET /api/files/<sender>/<name>.
  file?: { name: string; size: number; sha256: string };
}

export interface TauGroupMember {
  hash: string;
  name: string;
}

export interface TauGroup {
  id: string;
  name: string;
  members: TauGroupMember[];
  created_at: number;
}

export interface GroupsResponse {
  groups: TauGroup[];
  count: number;
}

export interface CreateGroupResponse {
  ok: boolean;
  group: TauGroup;
}

export interface DeleteGroupResponse {
  ok: boolean;
  deleted: { id: string; name: string };
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

export interface BlockPeerResult {
  peer: { destination: string; name: string };
  blocked: boolean;
  changed: boolean;
}

export interface BlockPeerResponse {
  ok: boolean;
  block: BlockPeerResult;
}

export interface InterfaceInfo {
  name: string;
  online: boolean;
}

// Mailbox counters from TauNode.info(): what the store-and-forward
// layer is holding for this node and for other peers.
export interface MailboxCounts {
  outbox: number;
  held: number;
  seen: number;
  sent: number;
  receipts: number;
}

// LXMF store-and-forward state from TauNode.info().
export interface LxmfInfo {
  enabled: boolean;
  delivery: string | null;
}

// Tan store state from the snapshot: whether Tan is enabled and how
// many posts this node has cached locally.
export interface SocialInfo {
  enabled: boolean;
  posts: number;
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
  // Absent on snapshots from an older bridge; the panel hides the
  // store rows instead of guessing.
  mailbox?: MailboxCounts;
  lxmf?: LxmfInfo;
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
  groups: TauGroup[];
  messages: TauMessage[];
  counts: Counts;
  unread: UnreadCounts;
  bridge: BridgeInfo;
  // Absent on snapshots from an older bridge.
  social?: SocialInfo;
}

export interface PingResult {
  ok: boolean;
  peer: Peer;
  server?: string;
  rtt_ms: number | null;
}

export interface SafetyNumberResult {
  peer: string;
  name: string;
  // Pairwise safety number: compare it with the peer over another
  // channel; showing it records nothing.
  safety_number: string;
}

export interface SafetyNumberResponse {
  ok: boolean;
  safety_number: SafetyNumberResult;
}

export interface PeerVerifyResult {
  peer: string;
  name: string;
  // Pairwise safety number: compare it with the peer over another
  // channel; if it matches, the recorded key is theirs.
  safety_number: string;
  verified_at: number | null;
  reverified: boolean;
  fingerprint_changed: boolean;
}

export interface VerifyPeerResponse {
  ok: boolean;
  verify: PeerVerifyResult;
}

export interface VerifyState {
  pending?: boolean;
  // Number shown for comparison; present before the owner confirms
  // it matches. Nothing is recorded while only this is set.
  safetyNumber?: string;
  result?: PeerVerifyResult;
  error?: string;
}

export interface SendResponse {
  ok: boolean;
  message: TauMessage;
  response: Record<string, unknown>;
}

// POST /api/react: the reaction token echoed back, or cleared when the
// caller retracted their reaction. `id` is the reaction's wire id.
export interface ReactResponse {
  ok: boolean;
  target: string;
  reaction: string;
  cleared: boolean;
  id: string | null;
}

export interface SendGroupResponse {
  ok: boolean;
  messages: TauMessage[];
  response: Record<string, unknown>;
}

export interface SendFileResponse {
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

export interface DeleteMessageResponse {
  ok: boolean;
  deleted: { id: number; wire_id?: string | null };
}
export interface RetryOutboxResponse {
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

export interface HeldMailEntry {
  id: string;
  to: string;
  to_name: string;
  from: string;
  from_name: string;
  stored_at: number;
  expires_at: number;
  blob_bytes: number;
}

export interface HeldMailResponse {
  held: HeldMailEntry[];
  count: number;
}

export interface DropHeldResponse {
  ok: boolean;
  dropped: HeldMailEntry & { note: string };
}

export interface AllowlistEntry {
  destination: string;
  name: string;
}

export interface AllowlistResponse {
  enabled: boolean;
  entries: AllowlistEntry[];
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
  repost_of?: TanReplyParent | null;
  quoted?: TanReplyParent | null;
  likes?: number;
  liked?: boolean;
  reposts?: number;
  reposted?: boolean;
  quotes?: number;
  own?: boolean;
  saved?: boolean;
  saved_at?: number;
  deleted?: boolean;
  // Thread entries carry their depth (the root is 0); timeline entries
  // have no depth.
  depth?: number;
}

export interface TanFollow {
  author: string;
  name: string;
  added_at: number;
  posts?: number;
  seq?: number;
  last_post?: number;
}

export interface MessagesSearchResponse {
  messages: TauMessage[];
  count: number;
}

export interface TanTimelineResponse {
  posts: TanPost[];
  count: number;
}

export interface TanSavedResponse {
  posts: TanPost[];
  count: number;
  next_offset: number | null;
}

export interface TanFollowingResponse {
  following: TanFollow[];
  count: number;
}

export interface TanLiker {
  author: string;
  name: string;
  liked_at: number;
}

export interface TanLikesResponse {
  target: string;
  likers: TanLiker[];
  count: number;
  total: number;
}

export interface TanMutedAuthor {
  author: string;
  name: string;
  muted_at: number;
  posts?: number;
  seq?: number;
  last_post?: number;
}

export interface TanMutedKeyword {
  keyword: string;
  display: string;
  added_at: number;
}

export interface TanMutedResponse {
  muted: TanMutedAuthor[];
  count: number;
}

export interface TanMutedKeywordsResponse {
  muted_keywords: TanMutedKeyword[];
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

export interface MessagesResponse {
  messages: TauMessage[];
}

export interface MessagesExportDocument {
  format: string;
  version: number;
  exported_at: number;
  messages: Array<Record<string, unknown>>;
}

export interface MessagesImportError {
  id: string | null;
  error: string;
}

export interface MessagesImportResult {
  imported: number;
  present: number;
  skipped: number;
  errors: MessagesImportError[];
  // Fresh unread counts, present only when the import stored something.
  unread?: UnreadCounts;
}

export interface MessagesImportResponse {
  ok: boolean;
  import: MessagesImportResult;
}

export interface TanExportDocument {
  format: string;
  version: number;
  exported_at: number;
  authors: string[];
  entries: Array<Record<string, unknown>>;
}

export interface TanImportError {
  hash: string;
  error: string;
}

export interface TanImportResult {
  imported: number;
  present: number;
  skipped: number;
  errors: TanImportError[];
}

export interface TanImportResponse {
  ok: boolean;
  import: TanImportResult;
}
