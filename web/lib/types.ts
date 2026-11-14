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
  // Unix time the message text was last edited, when it has been.
  edited_at?: number;
  // Outbox entry id, present while the message is queued for delivery.
  mail_id?: string;
  // Group DM context, present on group messages (inbound, and
  // outbound legs recorded per member on a console group send).
  group?: string;
  group_name?: string;
  // Sender-set expiry (epoch seconds). Absent when the message never
  // expires; the console purges it once the deadline passes.
  expires_at?: number;
  // Scheduled send time (epoch seconds), set by `tau send --delay` or
  // the console's send-later picker. Present only while the message is
  // queued for a future send; the console shows "sends in …" until then.
  send_after?: number;
  // Reticulum wire id: how replies name this message, and how
  // reactions are matched to it.
  wire_id?: string;
  // Latest reaction per reactor, oldest first.
  reactions?: MessageReaction[];
  // Pinned in the node inbox (`tau pin`): a local-only flag, never sent.
  // Absent reads as unpinned, the same way an absent `reactions` reads
  // as no reactions.
  pinned?: boolean;
  // Short hash of this message's text, for checking a reply's quoted
  // excerpt against it; see resolveQuote in message-feed.tsx.
  excerpt_hash?: string;
  // Previous excerpt hashes, kept when the message is edited so
  // replies that quoted it before the edit still resolve.
  excerpt_hashes?: string[];
  // Quoted message reference: wire id + short excerpt hash of the text
  // the sender quoted. The quote is rendered from local history only;
  // see resolveQuote in message-feed.tsx. A group reply may also carry
  // the quoted message's logical reference — the group, its author's
  // destination hash, and the send time every leg shares — so a member
  // who holds a different leg id can still resolve it.
  in_reply_to?:
    | {
        id: string;
        excerpt: string | null;
        group?: string;
        author?: string;
        sent_at?: number;
      }
    | null;
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
  // Absent on snapshots from an older bridge; only groups this node
  // created offer member changes in the console.
  created_by_me?: boolean;
  // The group's topic, as set with `tau group topic` or from the
  // console; empty when none is set. Absent on snapshots from an older
  // bridge.
  topic?: string;
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

export interface ChangeGroupMembersResponse {
  ok: boolean;
  group: TauGroup;
  previous_id: string;
}

export interface RenameGroupResponse {
  ok: boolean;
  group: TauGroup;
  previous_id: string;
}

export interface SetGroupTopicResponse {
  ok: boolean;
  group: TauGroup;
}

export interface Peer {
  destination: string;
  name: string;
  last_seen: number;
  age: number;
  hops: number;
  protocol: number;
  // Local-only label for this peer, set with `tau peers alias` or from
  // the console. Never sent to peers.
  alias?: string | null;
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

export interface ArchivedConversation {
  destination: string;
  name: string;
  messages: number;
}

export interface ArchivedConversationsResponse {
  archived: ArchivedConversation[];
  count: number;
}

export interface ArchiveConversationResult {
  destination: string;
  name: string;
  archived: boolean;
  changed: boolean;
}

export interface ArchiveConversationResponse {
  ok: boolean;
  archive: ArchiveConversationResult;
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

export interface PeersResponse {
  peers: Peer[];
  count: number;
}

export interface ForgetPeerResponse {
  ok: boolean;
  peer: Peer;
}

export interface PingResult {
  ok: boolean;
  peer: Peer;
  server?: string;
  rtt_ms: number | null;
}

export interface RenameResponse {
  ok: boolean;
  name: string;
  previous_name: string;
  changed: boolean;
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

export interface PeerInfoResult {
  ok: boolean;
  peer: Peer;
  info: PeerInfo;
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
  // Null when the send went out but was retracted before the console
  // record landed: the send succeeded, there is just no record to show.
  message: TauMessage | null;
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
  // Null when the file send went out but was retracted before the
  // console record landed: the transfer succeeded, there is just no
  // record to show.
  message: TauMessage | null;
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
  // Scheduled send time (epoch seconds), or null for a message queued
  // because the peer is offline rather than scheduled for later.
  send_after: number | null;
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

export interface NodeSettings {
  accept_files: boolean;
  send_read_receipts: boolean;
}

export interface UpdateSettingsResponse {
  ok: boolean;
  settings: NodeSettings;
}

export interface DoctorCheck {
  name: string;
  status: "ok" | "warn" | "fail";
  detail: string;
}

export interface DoctorResponse {
  ok: boolean;
  checks: DoctorCheck[];
}

export interface DeleteMessageResponse {
  ok: boolean;
  deleted: { id: number; wire_id?: string | null };
}

export interface EditMessageResponse {
  ok: boolean;
  // Whether the peer applied the edit; false (peer had no copy, or a
  // newer edit already landed) is not an error, and the local copy is
  // left alone in that case.
  edited: boolean;
  // The updated console entry, when it is in this console's history.
  message?: TauMessage | null;
  // Every updated leg of a group edit: one entry per leg, so the
  // whole group send swaps in at once. A DM edit carries only
  // `message`, so this is absent there.
  messages?: TauMessage[];
  // The edit's own wire id, when the peer answered one.
  id?: string | null;
}

export interface RetractMessageResponse {
  ok: boolean;
  retracted: { id: number; wire_id?: string | null };
  // Every console id removed by a group retract: one per leg. A DM
  // retract removes only `retracted.id`, so this is absent there.
  retracted_ids?: number[];
  // Whether the peer had a copy to delete; false is not an error.
  // For a group retract, whether every member the retract reached
  // deleted their copy.
  deleted: boolean;
  // The retract's own wire id, when the peer answered one.
  id?: string | null;
  // Per-member results of a group retract, as the node reports them.
  results?: GroupMemberResult[];
  // Queued legs the group retract cancelled, by outbox id.
  cancelled_queued?: string[];
  // The group a group retract fanned out to.
  group?: string;
}

// One leg of a group fan-out, as the node reports it.
export interface GroupMemberResult {
  to: string;
  to_name?: string;
  delivered?: boolean;
  deleted?: boolean;
  queued?: boolean;
  ok?: boolean;
  error?: string;
  id?: string | null;
  rtt?: number | null;
}

// POST /api/messages/forward: the forwarded copy, as recorded by the
// bridge's send path, plus the node's send response. The copy is null
// when it was retracted before the console record landed.
export interface ForwardMessageResponse {
  ok: boolean;
  message: TauMessage | null;
  response: Record<string, unknown>;
}

// POST /api/messages/pin: the pinned console id and wire id, plus
// whether the pin already existed (a re-pin is a no-op, not an error).
export interface PinMessageResponse {
  ok: boolean;
  pinned: { id: number; wire_id: string; already_pinned: boolean };
}

// POST /api/messages/unpin: the unpinned console id and wire id, plus
// whether a pin was actually removed.
export interface UnpinMessageResponse {
  ok: boolean;
  unpinned: { id: number; wire_id: string; was_pinned: boolean };
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

// A DM held for review from a peer outside the allow-list, as
// `GET /api/messages/requests` returns it.
export interface DmRequestEntry {
  id: string;
  sender: string;
  sender_name: string;
  text: string;
  kind: string;
  sent_at: number;
  received_at: number;
  request_expires_at: number;
  expires_at?: number;
  in_reply_to?: { id: string; excerpt?: string } | null;
}

export interface DmRequestsResponse {
  requests: DmRequestEntry[];
  count: number;
}

export interface AcceptDmRequestResponse {
  accepted: DmRequestEntry;
  delivered: boolean;
  allowlisted: boolean;
  skipped: string | null;
  unread?: UnreadCounts;
}

export interface DeclineDmRequestResponse {
  declined: DmRequestEntry;
  blocked: boolean;
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
  // A refusal the console can act on carries a machine-readable code
  // (e.g. "queued_mail" from /api/peers/forget, with the queued count).
  error?: { message?: string; code?: string; queued?: number };
}

// Sneakernet bundle: a signed JSON file carrying sealed mail (and
// optionally Tan posts) between nodes with no live path between them.
export interface BundleSkipped {
  id: string;
  reason: string;
}

export interface BundleSummary {
  mail: number;
  tan: number;
  skipped: BundleSkipped[];
  bytes: number;
}

export interface BundleExportResponse {
  ok: boolean;
  // The server's serialised bundle text, verbatim: the bundle is
  // signed, so the console downloads this string as-is instead of
  // parsing and re-stringifying it (a client round trip can rewrite
  // a number the signature covers, e.g. 1700000000.0 to 1700000000).
  bundle: string;
  summary: BundleSummary;
}

export interface BundleImportError {
  id: string;
  error: string;
}

// Tan import errors name the post by its hash, not a mail id.
export interface BundleTanError {
  hash: string;
  error: string;
}

export interface BundleTanReport {
  imported: number;
  present: number;
  skipped: number;
  errors: BundleTanError[];
}

export interface BundleImportReport {
  imported: number;
  present: number;
  skipped: number;
  tan: BundleTanReport | null;
  errors: BundleImportError[];
}

export interface BundleImportResponse {
  ok: boolean;
  import: BundleImportReport;
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
  pinned?: boolean;
  pinned_at?: number;
  deleted?: boolean;
  // Epoch seconds after which the post disappears from display
  // surfaces; absent when the post never expires.
  expires_at?: number;
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

export interface TanTag {
  tag: string;
  count: number;
}

export interface TanTagsResponse {
  tags: TanTag[];
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

export interface TanHiddenResponse {
  posts: TanPost[];
  count: number;
  next_offset: number | null;
}

export interface TanPinnedResponse {
  posts: TanPost[];
  count: number;
  next_offset: number | null;
}

export interface TanFollowingResponse {
  following: TanFollow[];
  count: number;
}

export interface TanAuthor {
  author: string;
  name: string;
  posts: number;
  last_post: number;
  followed: boolean;
}

export interface TanAuthorsResponse {
  authors: TanAuthor[];
  count: number;
  // How many authors the directory holds behind the returned page.
  total: number;
}

export interface TanSuggestion {
  author: string;
  name: string;
  posts: number;
  likes: number;
  reason: string;
  last_post: number;
}

export interface TanSuggestResponse {
  suggestions: TanSuggestion[];
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

// One queued Tan post, waiting for its publish time — the shape of
// `tau.scheduled_posts.ScheduledPost.to_dict()`.
export interface TanScheduledPost {
  id: string;
  text: string;
  reply_to: string | null;
  to: string[];
  ttl: number;
  publish_at: number;
  created_at: number;
  post_hash: string | null;
  attempts: number;
  next_attempt_at: number;
  paused_at: number;
  pending_peers: string[];
  // Seconds after publishing before the post disappears; 0 (or
  // absent on older entries) means it never expires.
  expires_in?: number;
}

export interface TanScheduleResponse {
  ok: boolean;
  scheduled: TanScheduledPost;
}

export interface TanScheduledResponse {
  scheduled: TanScheduledPost[];
  count: number;
}

export interface TanCancelScheduledResponse {
  ok: boolean;
  cancelled: TanScheduledPost;
  deleted_local: boolean;
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

export interface TanRules {
  default: "allow" | "deny";
  allow_authors: string[];
  block_authors: string[];
  allow_followed: boolean;
  // null means every kind is displayed; a list names the kinds shown.
  // Display-only: every kind from an admitted author is still stored.
  kinds: string[] | null;
}

export interface TanRulesResponse {
  rules: TanRules;
  available_kinds: string[];
}

export interface TanRulesUpdateResponse {
  ok: boolean;
  rules: TanRules;
}

export interface TanVerifyProblem {
  hash: string;
  author: string;
  problem: string;
}

export interface TanVerifyReport {
  entries: number;
  authors: number;
  unlinked: number;
  problems: TanVerifyProblem[];
  ok: boolean;
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
