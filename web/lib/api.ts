// api.ts — typed fetch client for the tau web bridge (tau/webapi.py).
//
// Every route the bridge serves is wrapped here. Base URL: same origin by
// default (the bridge serves the static export from the same origin), or
// NEXT_PUBLIC_API_BASE when set. In `next dev` (port 3000/3001) the bridge
// listens on :8787, so local dev defaults there for localhost hosts.
//
// Error shape from the bridge: `{ "error": { "message": "..." } }` with an
// HTTP status (400 bad request, 403 loopback-only / cross-origin, 404
// unknown peer, 429 too many subscribers, 500 internal, 502 node error,
// 503 mailbox full / check already running). All failures surface as
// ApiError with the bridge's message and status.
//
// No UI code lives here — hooks in use-tau.ts / use-tan.ts build on this.

import type {
  AllowList,
  ApiErrorBody,
  AwayMessageSettings,
  Beacon,
  BlockResult,
  BroadcastList,
  BroadcastSendResponse,
  Channel,
  ChannelMessage,
  ChannelTopic,
  ChatMessage,
  DoctorReport,
  DmRequest,
  DisappearingDefaults,
  Group,
  GroupChangeResult,
  HeldEnvelope,
  JoinRequest,
  LeftGroups,
  MeshPage,
  MeshRow,
  MutedConversations,
  MessageDraft,
  NodeSettings,
  Outbox,
  OutboxEntry,
  OutboxRetryResult,
  OwnProfile,
  PageList,
  Peer,
  PeerInfo,
  PingResult,
  QuietHoursSettings,
  SafetyNumberResult,
  ScheduledChannelMessage,
  ScheduledTanPost,
  SendResult,
  Snapshot,
  TanAuthor,
  TanDraft,
  TanList,
  TanListMember,
  TanPost,
  TanQuoter,
  TanReactors,
  TanRules,
  TanTag,
  TanVerifyReport,
  UnreadCounts,
  VerifyResult,
} from "./types";

function isLocalHost(hostname: string): boolean {
  return (
    hostname === "localhost" ||
    hostname === "127.0.0.1" ||
    hostname === "::1" ||
    hostname.endsWith(".localhost")
  );
}

function resolveApiBase(): string {
  const configured = process.env.NEXT_PUBLIC_API_BASE;
  if (configured !== undefined) return configured.replace(/\/$/, "");
  if (typeof window === "undefined") return "";
  const { protocol, hostname, port } = window.location;
  if ((port === "3000" || port === "3001") && isLocalHost(hostname)) {
    return `${protocol}//${hostname}:8787`;
  }
  return "";
}

export const API_BASE = resolveApiBase();

/** WebSocket live stream URL (first frame is the snapshot). */
export function wsUrl(): string {
  const base = API_BASE || (typeof window !== "undefined" ? window.location.origin : "");
  return `${base.replace(/^http/, "ws")}/ws`;
}

/** SSE live stream URL. `since` replays events after that event id. */
export function eventsUrl(since?: number): string {
  return `${API_BASE}/api/events${since ? `?since=${since}` : ""}`;
}

export class ApiError extends Error {
  status: number;
  code?: string;
  queued?: number;

  constructor(message: string, status: number, code?: string, queued?: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.queued = queued;
  }
}

function errorMessage(payload: unknown, fallback: string): { message: string; code?: string; queued?: number } {
  if (payload && typeof payload === "object") {
    const body = payload as ApiErrorBody;
    if (body.error?.message) {
      return { message: body.error.message, code: body.error.code, queued: body.error.queued };
    }
    if (body.message) return { message: body.message };
  }
  return { message: fallback };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  const raw = await response.text();
  let payload: unknown = null;
  if (raw) {
    try {
      payload = JSON.parse(raw);
    } catch {
      payload = null;
    }
  }
  if (!response.ok) {
    const { message, code, queued } = errorMessage(
      payload,
      `request failed: ${response.status}`,
    );
    throw new ApiError(message, response.status, code, queued);
  }
  return payload as T;
}

function get<T>(path: string): Promise<T> {
  return request<T>(path);
}

function post<T>(path: string, body: unknown = {}): Promise<T> {
  return request<T>(path, { method: "POST", body: JSON.stringify(body) });
}

function query(params: Record<string, string | number | boolean | undefined | null>): string {
  const parts: string[] = [];
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
  }
  return parts.length ? `?${parts.join("&")}` : "";
}

function enc(value: string): string {
  return encodeURIComponent(value);
}

// ------------------------------------------------------------------ misc --

export async function getHealth(): Promise<{ ok: boolean; version: string }> {
  return get("/healthz");
}

/** Full snapshot: node, peers, groups, messages, counts, unread, bridge. */
export async function getStatus(): Promise<Snapshot> {
  return get("/api/status");
}

// ----------------------------------------------------------------- peers --

export async function getPeers(): Promise<Peer[]> {
  const data = await get<{ peers: Peer[]; count: number }>("/api/peers");
  return data.peers;
}

export async function getMesh(): Promise<MeshRow[]> {
  const data = await get<{ peers: MeshRow[]; count: number }>("/api/mesh");
  return data.peers;
}

export async function getHeard(): Promise<unknown[]> {
  const data = await get<{ heard: unknown[]; count: number }>("/api/heard");
  return data.heard;
}

export async function forgetPeer(target: string, force = false): Promise<unknown> {
  return post("/api/peers/forget", { target, force });
}

export async function getSafetyNumber(target: string): Promise<SafetyNumberResult> {
  return post("/api/peers/safety-number", { target });
}

/** Mark a peer verified — call only after comparing the safety number out of band. */
export async function verifyPeer(target: string): Promise<VerifyResult> {
  return post("/api/peers/verify", { target });
}

export async function blockPeer(target: string): Promise<BlockResult> {
  return post("/api/peers/block", { target });
}

export async function unblockPeer(target: string): Promise<BlockResult> {
  return post("/api/peers/unblock", { target });
}

/** Set (or clear, with a blank string) a peer's local alias. Returns the peer. */
export async function setPeerAlias(destination: string, alias: string): Promise<Peer> {
  return post("/api/peers/alias", { destination, alias });
}

/** Set (or clear, with a blank string) a peer's private note. Loopback only. Returns the peer. */
export async function setPeerNote(destination: string, note: string): Promise<Peer> {
  return post("/api/peers/note", { destination, note });
}

export async function pingPeer(to: string, timeout = 15): Promise<PingResult> {
  return post("/api/ping", { to, timeout });
}

export async function getPeerInfo(to: string, timeout = 15): Promise<PeerInfo> {
  return post("/api/info", { to, timeout });
}

export async function announce(): Promise<unknown> {
  return post("/api/announce", {});
}

/** Rename this node (live name + re-announce). */
export async function renameNode(name: string): Promise<unknown> {
  return post("/api/id/rename", { name });
}

// -------------------------------------------------------------- settings --

export async function getSettings(): Promise<NodeSettings> {
  return get("/api/settings");
}

/** `setting` is "accept_files" | "send_read_receipts" | "tan_sync_depth" | "announce_interval"; boolean settings take a boolean, tan_sync_depth takes 1 or 2, announce_interval takes seconds (0 disables). */
export async function updateSetting(
  setting: "accept_files" | "send_read_receipts" | "tan_sync_depth" | "announce_interval",
  value: boolean | number,
): Promise<NodeSettings> {
  const data = await post<{ ok: boolean; settings: NodeSettings }>("/api/settings", {
    setting,
    value,
  });
  return data.settings;
}

export async function getDoctor(): Promise<DoctorReport> {
  return get("/api/doctor");
}

/** Download URL for a full backup archive of the data directory (loopback only). */
export function backupUrl(): string {
  return `${API_BASE}/api/backup`;
}

export async function getAllowlist(): Promise<AllowList> {
  return get("/api/allowlist");
}

export async function allowlistAdd(target: string): Promise<AllowList> {
  return post("/api/allowlist/add", { target });
}

export async function allowlistRemove(target: string): Promise<AllowList> {
  return post("/api/allowlist/remove", { target });
}

export async function allowlistSetEnabled(enabled: boolean): Promise<AllowList> {
  return post("/api/allowlist/set-enabled", { enabled });
}

// ---------------------------------------------------------------- groups --

export async function getGroups(): Promise<Group[]> {
  const data = await get<{ groups: Group[]; count: number }>("/api/groups");
  return data.groups;
}

export async function getLeftGroups(): Promise<LeftGroups> {
  return get("/api/groups/left");
}

export async function getJoinRequests(): Promise<JoinRequest[]> {
  const data = await get<{ requests: JoinRequest[]; count: number }>(
    "/api/groups/join-requests",
  );
  return data.requests;
}

export async function createGroup(name: string, members: string[]): Promise<Group> {
  return post("/api/groups", { name, members });
}

export async function deleteGroup(group: string): Promise<{ id: string; name: string }> {
  return post("/api/groups/delete", { group });
}

// ---------------------------------------------------------- broadcasts --
// One message, many private DMs: a send fans out as an ordinary DM per
// member, so recipients never see each other or the list.

export async function getBroadcasts(): Promise<BroadcastList[]> {
  const data = await get<{ broadcasts: BroadcastList[]; count: number }>("/api/broadcasts");
  return data.broadcasts ?? [];
}

export async function createBroadcast(
  name: string,
  members: string[],
): Promise<{ ok: boolean; broadcast: BroadcastList }> {
  return post("/api/broadcasts", { name, members });
}

export async function deleteBroadcast(broadcast: string): Promise<{ ok: boolean; name: string }> {
  return post("/api/broadcasts/delete", { broadcast });
}

export async function sendBroadcast(
  broadcast: string,
  text: string,
): Promise<BroadcastSendResponse> {
  return post("/api/broadcasts/send", { broadcast, text });
}

export async function leaveGroup(group: string): Promise<{ id: string; name: string; already_left?: boolean; also_left?: string[] }> {
  return post("/api/groups/leave", { group });
}

export async function rejoinGroup(group: string): Promise<{ rejoined: string }> {
  return post("/api/groups/rejoin", { group });
}

export async function ignoreGroup(group: string): Promise<unknown> {
  return post("/api/groups/ignore", { group });
}

export async function unignoreGroup(group: string): Promise<unknown> {
  return post("/api/groups/unignore", { group });
}

export async function addGroupMembers(group: string, members: string[]): Promise<GroupChangeResult> {
  return post("/api/groups/add-member", { group, members });
}

export async function removeGroupMembers(group: string, members: string[]): Promise<GroupChangeResult> {
  return post("/api/groups/remove-member", { group, members });
}

export interface GroupBannedEntry {
  hash: string;
  name: string;
}

export interface GroupBannedResult {
  group: Group;
  banned: GroupBannedEntry[];
  previous_id?: string;
  count?: number;
}

export async function banGroupMember(group: string, peer: string): Promise<GroupBannedResult> {
  return post("/api/groups/ban", { group, peer });
}

export async function unbanGroupMember(group: string, peer: string): Promise<GroupBannedResult & { unbanned: GroupBannedEntry; was_banned: boolean }> {
  return post("/api/groups/unban", { group, peer });
}

export async function getGroupBanned(group: string): Promise<GroupBannedResult> {
  return get(`/api/groups/banned${query({ group })}`);
}

export async function approveJoinRequest(group: string, peer: string): Promise<unknown> {
  return post("/api/groups/approve-join", { group, peer });
}

export async function denyJoinRequest(group: string, peer: string): Promise<unknown> {
  return post("/api/groups/deny-join", { group, peer });
}

export async function renameGroup(group: string, name: string): Promise<GroupChangeResult> {
  return post("/api/groups/rename", { group, name });
}

export async function setGroupTopic(group: string, topic?: string, clear = false): Promise<{ group: Group }> {
  return post("/api/groups/topic", clear ? { group, clear: true } : { group, topic });
}

/** An invite link for a group this node created, with its stored state. */
export interface GroupInviteStatus {
  group: Group;
  mode: "open" | "closed";
  generation: number;
  link: string | null;
}

export async function getGroupInviteStatus(group: string): Promise<GroupInviteStatus> {
  return get(`/api/groups/invite${query({ group })}`);
}

export async function mintGroupInvite(
  group: string,
  mode?: "open" | "closed",
): Promise<GroupInviteStatus> {
  return post("/api/groups/invite", mode === undefined ? { group } : { group, mode });
}

export async function revokeGroupInvites(group: string): Promise<{ group: Group; generation: number }> {
  return post("/api/groups/invite/revoke", { group });
}

export interface GroupJoinResult {
  status: "added" | "already_member" | "pending" | "queued";
  /** The joined group when added, the group name when queued. */
  group: Group | string;
}

export async function joinGroupViaLink(link: string): Promise<GroupJoinResult> {
  return post("/api/groups/join", { link });
}

/** Voting/closing a group poll returns the sent vote/close legs + node response. */
export interface GroupPollActionResult {
  ok: boolean;
  messages: ChatMessage[];
  response: Record<string, unknown>;
}

export async function voteGroupPoll(
  group: string,
  poll: string,
  option: number | string,
): Promise<GroupPollActionResult> {
  return post("/api/groups/vote", { group, poll, option });
}

export async function closeGroupPoll(group: string, poll: string): Promise<GroupPollActionResult> {
  return post("/api/groups/close-poll", { group, poll });
}

/** Voting/closing a DM poll returns the sent vote/close + node response. */
export type DmPollActionResult = GroupPollActionResult;

export async function voteDmPoll(
  peer: string,
  poll: string,
  option: number | string,
): Promise<DmPollActionResult> {
  return post("/api/dm/vote", { to: peer, poll, option });
}

export async function closeDmPoll(peer: string, poll: string): Promise<DmPollActionResult> {
  return post("/api/dm/close-poll", { to: peer, poll });
}

// --------------------------------------------------------------- beacons --

/** Live beacons this node is sharing, active first. */
export async function getBeacons(): Promise<{ beacons: Beacon[]; count: number }> {
  return get("/api/beacons");
}

export interface StartBeaconOptions {
  lat: number;
  lon: number;
  name?: string;
  /** Seconds the beacon runs (60..28800, default 3600). */
  durationSeconds?: number;
  /** Round updates to ~1 km and drop the place name. */
  coarse?: boolean;
}

/** Start a live beacon; the first update goes out at once. */
export async function startBeacon(
  target: { peer?: string; group?: string },
  opts: StartBeaconOptions,
): Promise<{ beacon: Beacon; update: unknown }> {
  return post("/api/beacons/start", { ...target, ...opts });
}

/** Stop a live beacon by id. */
export async function stopBeacon(id: string): Promise<{ beacon: Beacon }> {
  return post("/api/beacons/stop", { id });
}

/** Feed a running beacon a fresh position from the browser's geolocation. */
export async function updateBeaconPosition(
  id: string,
  lat: number,
  lon: number,
  name?: string,
): Promise<{ beacon: Beacon }> {
  return post("/api/beacons/position", {
    id,
    lat,
    lon,
    ...(name !== undefined ? { name } : {}),
  });
}

// -------------------------------------------------------------- messages --

export interface SendMessageOptions {
  /** Wire id of the message being replied to. */
  replyTo?: string;
  /** Disappearing-message lifetime in seconds (0 < n <= 30 days). */
  expiresIn?: number;
  /** Delay before sending, in seconds (0 < n <= 29d23h). */
  sendIn?: number;
  /** Post `text` as a poll question with these options. */
  pollOptions?: string[];
  /** Pin a place on the message: latitude -90..90, longitude -180..180. */
  location?: { lat: number; lon: number; name?: string };
  timeout?: number;
}

export async function getMessages(limit = 200): Promise<ChatMessage[]> {
  const data = await get<{ messages: ChatMessage[] }>(
    `/api/messages${query({ limit })}`,
  );
  return data.messages;
}

export async function getPinnedMessages(): Promise<ChatMessage[]> {
  const data = await get<{ messages: ChatMessage[] }>("/api/messages?pinned=1");
  return data.messages;
}

export async function searchMessages(
  q: string,
  opts: { peer?: string; limit?: number } = {},
): Promise<ChatMessage[]> {
  const data = await get<{ messages: ChatMessage[]; count: number }>(
    `/api/messages/search${query({ q, peer: opts.peer, limit: opts.limit ?? 100 })}`,
  );
  return data.messages;
}

/** Send a DM (`to`) or a group message (`group`). Exactly one target. */
export async function sendMessage(
  target: { to: string } | { group: string },
  text: string,
  opts: SendMessageOptions = {},
): Promise<SendResult> {
  const body: Record<string, unknown> = { ...target, text };
  if (opts.replyTo) body.reply_to = opts.replyTo;
  if (opts.expiresIn !== undefined) body.expires_in = opts.expiresIn;
  if (opts.sendIn !== undefined) body.send_in = opts.sendIn;
  if (opts.pollOptions) body.poll_options = opts.pollOptions;
  if (opts.location) body.location = opts.location;
  if (opts.timeout !== undefined) body.timeout = opts.timeout;
  return post("/api/messages", body);
}

export interface SendFileOptions {
  timeout?: number;
}

/** Send a file (<=10 MiB) as base64 data to a peer. */
export async function sendFile(
  to: string,
  name: string,
  data: string,
  opts: SendFileOptions = {},
): Promise<SendResult> {
  return post("/api/send-file", { to, name, data, timeout: opts.timeout ?? 300 });
}

export async function sendGroupFile(
  group: string,
  name: string,
  data: string,
  opts: SendFileOptions = {},
): Promise<unknown> {
  return post("/api/groups/send-file", { group, name, data, timeout: opts.timeout ?? 300 });
}

/** Download URL for a file received from `sender` (destination hash). */
export function fileUrl(sender: string, name: string): string {
  return `${API_BASE}/api/files/${enc(sender)}/${enc(name)}`;
}

export async function reactToMessage(target: string, reaction: string): Promise<unknown> {
  return post("/api/react", { target, reaction });
}

/** Clear your reaction on a message (by wire id). */
export async function clearReaction(target: string): Promise<unknown> {
  return post("/api/react", { target, clear: true });
}

export async function sendTyping(
  target: { peer: string } | { group: string },
  state: "start" | "stop",
): Promise<unknown> {
  return post("/api/typing", { ...target, state });
}

export async function deleteMessage(id: number): Promise<unknown> {
  return post("/api/messages/delete", { id });
}

/** Edit your sent message (console id). `timeout` bounds the edit fan-out. */
export async function editMessage(id: number, text: string, timeout = 20): Promise<unknown> {
  return post("/api/messages/edit", { id, text, timeout });
}

/** Ask the peer(s) to delete your sent message, then drop local copies. */
export async function retractMessage(id: number): Promise<unknown> {
  return post("/api/messages/retract", { id });
}

export async function forwardMessage(
  id: number,
  target: { to: string } | { group: string },
): Promise<unknown> {
  return post("/api/messages/forward", { id, ...target });
}

export async function pinMessage(id: number): Promise<unknown> {
  return post("/api/messages/pin", { id });
}

export async function unpinMessage(id: number): Promise<unknown> {
  return post("/api/messages/unpin", { id });
}

export async function getDmRequests(): Promise<DmRequest[]> {
  const data = await get<{ requests: DmRequest[]; count: number }>("/api/messages/requests");
  return data.requests;
}

export async function acceptDmRequest(id: string): Promise<unknown> {
  return post("/api/messages/requests/accept", { id });
}

export async function declineDmRequest(id: string, block = false): Promise<unknown> {
  return post("/api/messages/requests/decline", { id, block });
}

/** Advance the read cursor for one conversation, or all when neither is given. */
export async function markRead(
  target:
    | { peer: string }
    | { group: string }
    | { channel: string }
    | Record<string, never> = {},
): Promise<UnreadCounts> {
  const data = await post<{ ok: boolean; unread: UnreadCounts }>("/api/messages/read", target);
  return data.unread;
}

/** Move a conversation's read cursor back (newest message unread, or all with `all`). */
export async function markUnread(
  target: { peer: string } | { group: string },
  all = false,
): Promise<UnreadCounts> {
  const data = await post<{ ok: boolean; unread: UnreadCounts }>("/api/messages/unread", {
    ...target,
    all,
  });
  return data.unread;
}

export async function getArchivedConversations(): Promise<unknown> {
  return get("/api/conversations/archived");
}

export async function archiveConversation(target: string): Promise<unknown> {
  return post("/api/conversations/archive", { target });
}

export async function unarchiveConversation(target: string): Promise<unknown> {
  return post("/api/conversations/unarchive", { target });
}

export async function getDisappearingDefaults(): Promise<DisappearingDefaults> {
  return get("/api/messages/disappearing");
}

/** Set a disappearing default (seconds; null clears it) for one peer, group or channel. */
export async function setDisappearingDefault(
  target: { peer: string } | { group: string } | { channel: string },
  seconds: number | null,
): Promise<unknown> {
  return post("/api/messages/disappearing", { ...target, seconds });
}

/** This node's published public profile (or the node-name default). */
export async function getOwnProfile(): Promise<OwnProfile> {
  return get("/api/profile");
}

/** Publish a new profile version; omitted fields keep their stored value. */
export async function setOwnProfile(patch: {
  name?: string;
  bio?: string;
  avatar?: string | null;
  links?: string[];
}): Promise<{ ok: boolean; profile: OwnProfile & { changed: boolean } }> {
  return post("/api/profile", patch);
}

export async function getAwayMessage(): Promise<AwayMessageSettings> {
  return get("/api/settings/away-message");
}

/** Set the away auto-reply; omitted fields keep their stored value. */
export async function setAwayMessage(patch: {
  enabled?: boolean;
  message?: string;
}): Promise<{ ok: boolean; away_message: AwayMessageSettings }> {
  return post("/api/settings/away-message", patch);
}

/** The quiet-hours window, plus whether it is quiet right now. */
export async function getQuietHours(): Promise<QuietHoursSettings> {
  return get("/api/settings/quiet-hours");
}

/** Set the quiet-hours window; omitted fields keep their stored value. */
export async function setQuietHours(patch: {
  enabled?: boolean;
  start?: string;
  end?: string;
}): Promise<{ ok: boolean; quiet_hours: QuietHoursSettings }> {
  return post("/api/settings/quiet-hours", patch);
}


export async function getMutedConversations(): Promise<MutedConversations> {
  return get("/api/messages/muted");
}

/** Saved message drafts, oldest first (``tau draft save``). */
export async function getDrafts(): Promise<MessageDraft[]> {
  const data = await get<{ drafts: MessageDraft[]; count: number }>("/api/drafts");
  return data.drafts ?? [];
}

/** Save (or overwrite) the draft for one peer, group, or channel conversation. */
export async function saveDraft(
  kind: "peer" | "group" | "channel",
  target: string,
  text: string,
): Promise<MessageDraft> {
  const data = await post<{ ok: boolean; draft: MessageDraft }>("/api/drafts", {
    kind,
    target,
    text,
  });
  return data.draft;
}

/** Discard the draft for one peer, group, or channel conversation. */
export async function deleteDraft(kind: "peer" | "group" | "channel", target: string): Promise<boolean> {
  const data = await post<{ ok: boolean; deleted: boolean }>("/api/drafts/delete", {
    kind,
    target,
  });
  return data.deleted;
}

export async function setMuted(
  target: { peer: string } | { group: string } | { channel: string },
  muted = true,
): Promise<unknown> {
  return post("/api/messages/mute", { ...target, muted });
}

/** Pinned conversations, newest pin first. */
export async function getPinnedConversations(): Promise<unknown> {
  return get("/api/conversations/pinned");
}

/** Pin or unpin one conversation (peer, group, or channel). */
export async function setPinnedConversation(
  target: { peer: string } | { group: string } | { channel: string },
  pinned = true,
): Promise<unknown> {
  return post("/api/conversations/pin", { ...target, pinned });
}

/** Export stored DMs as the CLI's JSON document (loopback only). */
export async function exportMessages(): Promise<unknown> {
  return get("/api/messages/export");
}

/** Import a DM export document (loopback only). */
export async function importMessages(document: unknown): Promise<unknown> {
  return post("/api/messages/import", document);
}

// ---------------------------------------------------------------- outbox --

export async function getOutbox(): Promise<Outbox> {
  return get("/api/outbox");
}

export async function cancelOutbox(target: string): Promise<unknown> {
  return post("/api/outbox/cancel", { target });
}

export async function editOutbox(
  target: string,
  patch: { text?: string; send_in?: number },
): Promise<{ ok: boolean; edited: OutboxEntry[]; legs: number }> {
  return post("/api/outbox/edit", { target, ...patch });
}

export async function retryOutbox(target?: string): Promise<OutboxRetryResult> {
  return post("/api/outbox/retry", target ? { target } : {});
}

export async function getHeldMail(): Promise<{ held: HeldEnvelope[]; count: number }> {
  return get("/api/outbox/held");
}

export async function dropHeldMail(target: string): Promise<HeldEnvelope> {
  return post("/api/outbox/drop-held", { target });
}

/** Build a signed sneakernet bundle (optionally including Tan posts). */
export async function exportBundle(includeTan = false): Promise<unknown> {
  return post("/api/bundle/export", { include_tan: includeTan });
}

/** Merge a signed bundle document into this node's mailbox and Tan store. */
export async function importBundle(document: unknown): Promise<unknown> {
  return post("/api/bundle/import", document);
}

// ------------------------------------------------------------------- Tan --

export async function getTanTimeline(opts: {
  limit?: number;
  all?: boolean;
  author?: string;
  tag?: string;
  list?: string;
} = {}): Promise<TanPost[]> {
  const data = await get<{ posts: TanPost[]; count: number }>(
    `/api/tan/timeline${query({
      limit: opts.limit ?? 30,
      all: opts.all ? "1" : undefined,
      author: opts.author,
      tag: opts.tag,
      list: opts.list,
    })}`,
  );
  return data.posts;
}

export async function getTanTags(limit = 30): Promise<TanTag[]> {
  const data = await get<{ tags: TanTag[]; count: number }>(
    `/api/tan/tags${query({ limit })}`,
  );
  return data.tags;
}

export async function getTanTop(limit = 10, author?: string): Promise<TanPost[]> {
  const data = await get<{ posts: TanPost[]; count: number }>(
    `/api/tan/top${query({ limit, author })}`,
  );
  return data.posts;
}

export async function searchTan(
  q: string,
  opts: { author?: string; limit?: number } = {},
): Promise<TanPost[]> {
  const data = await get<{ posts: TanPost[]; count: number }>(
    `/api/tan/search${query({ q, author: opts.author, limit: opts.limit ?? 30 })}`,
  );
  return data.posts;
}

export async function getTanMentions(limit = 30): Promise<TanPost[]> {
  const data = await get<{ posts: TanPost[]; count: number }>(
    `/api/tan/mentions${query({ limit })}`,
  );
  return data.posts;
}

/** Thread rooted at a post (hash or prefix), oldest first. */
export async function getTanThread(ref: string): Promise<TanPost[]> {
  const data = await get<{ posts: TanPost[]; count: number }>(
    `/api/tan/thread${query({ ref })}`,
  );
  return data.posts;
}

export async function getTanFollowing(): Promise<TanAuthor[]> {
  const data = await get<{ following: TanAuthor[]; count: number }>("/api/tan/following");
  return data.following;
}

export async function getTanFollowers(): Promise<TanAuthor[]> {
  const data = await get<{ followers: TanAuthor[]; count: number }>("/api/tan/followers");
  return data.followers;
}

export async function getTanAuthors(opts: {
  sort?: "recent" | "posts" | "name";
  limit?: number;
  offset?: number;
} = {}): Promise<{ authors: TanAuthor[]; count: number; total: number }> {
  return get(
    `/api/tan/authors${query({
      sort: opts.sort ?? "recent",
      limit: opts.limit ?? 50,
      offset: opts.offset ?? 0,
    })}`,
  );
}

export async function getTanSuggestions(limit = 5): Promise<TanAuthor[]> {
  const data = await get<{ suggestions: TanAuthor[]; count: number }>(
    `/api/tan/suggest${query({ limit })}`,
  );
  return data.suggestions;
}

export async function getTanLikers(target: string): Promise<TanReactors> {
  return get(`/api/tan/likes${query({ target })}`);
}

export async function getTanReposters(target: string): Promise<TanReactors> {
  return get(`/api/tan/reposts${query({ target })}`);
}

export async function getTanQuoters(target: string): Promise<TanReactors> {
  return get(`/api/tan/quotes${query({ target })}`);
}

export async function getTanMuted(): Promise<TanAuthor[]> {
  const data = await get<{ muted: TanAuthor[]; count: number }>("/api/tan/muted");
  return data.muted;
}

export async function getTanMutedKeywords(): Promise<string[]> {
  const data = await get<{ muted_keywords: string[]; count: number }>(
    "/api/tan/muted-keywords",
  );
  return data.muted_keywords;
}

export async function getTanFollowedTags(): Promise<string[]> {
  const data = await get<{ followed_tags: string[]; count: number }>(
    "/api/tan/followed-tags",
  );
  return data.followed_tags;
}

export async function getTanMutedTags(): Promise<string[]> {
  const data = await get<{ muted_tags: string[]; count: number }>("/api/tan/muted-tags");
  return data.muted_tags;
}

export async function getTanLists(): Promise<TanList[]> {
  const data = await get<{ lists: TanList[]; count: number }>("/api/tan/lists");
  return data.lists;
}

export async function getTanListMembers(name: string): Promise<{
  name: string;
  members: TanListMember[];
  count: number;
}> {
  return get(`/api/tan/lists/members${query({ name })}`);
}

export async function createTanList(name: string): Promise<unknown> {
  return post("/api/tan/lists", { name });
}

export async function deleteTanList(name: string): Promise<unknown> {
  return post("/api/tan/lists/delete", { name });
}

export async function addTanListMember(name: string, target: string): Promise<unknown> {
  return post("/api/tan/lists/add", { name, target });
}

export async function removeTanListMember(name: string, target: string): Promise<unknown> {
  return post("/api/tan/lists/remove", { name, target });
}

export async function getTanSaved(limit = 30, offset = 0): Promise<{ posts: TanPost[]; next_offset: number | null }> {
  const data = await get<{ posts: TanPost[]; count: number; next_offset: number | null }>(
    `/api/tan/saved${query({ limit, offset })}`,
  );
  return { posts: data.posts, next_offset: data.next_offset };
}

export async function getTanHidden(limit = 30, offset = 0): Promise<{ posts: TanPost[]; next_offset: number | null }> {
  const data = await get<{ posts: TanPost[]; count: number; next_offset: number | null }>(
    `/api/tan/hidden${query({ limit, offset })}`,
  );
  return { posts: data.posts, next_offset: data.next_offset };
}

export async function getTanPinned(limit = 30, offset = 0): Promise<{ posts: TanPost[]; next_offset: number | null }> {
  const data = await get<{ posts: TanPost[]; count: number; next_offset: number | null }>(
    `/api/tan/pinned${query({ limit, offset })}`,
  );
  return { posts: data.posts, next_offset: data.next_offset };
}

/** This node's own recent posts (live in-memory list), newest last. */
export async function getTanPosts(limit = 100): Promise<TanPost[]> {
  const data = await get<{ posts: TanPost[]; count: number }>(
    `/api/tan/posts${query({ limit })}`,
  );
  return data.posts;
}

export async function getTanScheduled(): Promise<ScheduledTanPost[]> {
  const data = await get<{ scheduled: ScheduledTanPost[]; count: number }>(
    "/api/tan/scheduled",
  );
  return data.scheduled;
}

export async function getTanRules(): Promise<{ rules: TanRules; available_kinds: string[] }> {
  return get("/api/tan/rules");
}

export async function updateTanRules(rules: Record<string, unknown>): Promise<TanRules> {
  const data = await post<{ ok: boolean; rules: TanRules }>("/api/tan/rules", rules);
  return data.rules;
}

export async function getTanVerify(): Promise<TanVerifyReport> {
  return get("/api/tan/verify");
}

/** Export stored posts (scope "mine" | "all"). */
export async function exportTan(scope: "mine" | "all" = "mine"): Promise<unknown> {
  return get(`/api/tan/export${query({ scope })}`);
}

/** Import a Tan export document (every entry verified). Loopback only. */
export async function importTan(document: unknown): Promise<unknown> {
  return post("/api/tan/import", document);
}

/** URL for a cached Tan image's bytes. */
export function tanMediaUrl(sha256: string): string {
  return `${API_BASE}/api/tan/media/${enc(sha256)}`;
}

/** Fetch a Tan image from its author (202 Accepted; bytes arrive later). */
export async function fetchTanMedia(sha256: string, author: string): Promise<unknown> {
  return post(`/api/tan/media/${enc(sha256)}/fetch`, { author });
}

export interface PublishTanOptions {
  replyTo?: string;
  /** Post lifetime in seconds (0 < n <= 7 days). */
  expiresIn?: number;
  /** Image to attach, as a base64 data URL (top-level posts only). */
  image?: { dataUrl: string };
}

export async function publishTanPost(
  text: string,
  opts: PublishTanOptions = {},
): Promise<TanPost> {
  return post("/api/tan/posts", {
    text,
    reply_to: opts.replyTo,
    expires_in: opts.expiresIn,
    image: opts.image?.dataUrl,
  });
}

/** Largest image a Tan post may carry, in bytes (mirrors MAX_IMAGE_BYTES). */
export const TAN_MAX_IMAGE_BYTES = 2 * 1024 * 1024;

/** Read a picked file as a base64 data URL for upload. */
export function readFileAsDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(new Error("Could not read the image file."));
    reader.readAsDataURL(file);
  });
}

export async function scheduleTanPost(
  text: string,
  sendIn: number,
  opts: PublishTanOptions = {},
): Promise<ScheduledTanPost> {
  return post("/api/tan/schedule", {
    text,
    reply_to: opts.replyTo,
    send_in: sendIn,
    expires_in: opts.expiresIn ?? 0,
    image: opts.image?.dataUrl,
  });
}

/** Queue a Tan poll for later: the question plus its fixed options. */
export async function scheduleTanPoll(
  question: string,
  options: string[],
  sendIn: number,
  opts: { expiresIn?: number } = {},
): Promise<ScheduledTanPost> {
  return post("/api/tan/schedule", {
    text: question,
    poll_options: options,
    send_in: sendIn,
    expires_in: opts.expiresIn ?? 0,
  });
}

export async function scheduleTanRepost(
  target: string,
  sendIn: number,
): Promise<ScheduledTanPost> {
  return post("/api/tan/schedule", {
    kind: "repost",
    target_post: target,
    send_in: sendIn,
  });
}

/** Queue a quote of a post for later: the comment plus the quoted post. */
export async function scheduleTanQuote(
  target: string,
  text: string,
  sendIn: number,
): Promise<ScheduledTanPost> {
  return post("/api/tan/schedule", {
    kind: "quote",
    target_post: target,
    text,
    send_in: sendIn,
  });
}

export async function cancelScheduledTanPost(id: string): Promise<unknown> {
  return post("/api/tan/cancel-scheduled", { id });
}

export async function editScheduledTanPost(
  id: string,
  opts: { text?: string; sendIn?: number },
): Promise<ScheduledTanPost> {
  const data = await post<{ ok: boolean; scheduled: ScheduledTanPost }>(
    "/api/tan/edit-scheduled",
    {
      id,
      text: opts.text,
      send_in: opts.sendIn,
    },
  );
  return data.scheduled;
}

/** Saved Tan post drafts, newest first. */
export async function getTanDrafts(): Promise<TanDraft[]> {
  const data = await get<{ drafts: TanDraft[]; count: number }>("/api/tan/drafts");
  return data.drafts;
}

/** Save a Tan post draft (plain text plus optional reply and expiry). */
export async function saveTanDraft(
  text: string,
  opts: PublishTanOptions = {},
): Promise<TanDraft> {
  const data = await post<{ ok: boolean; draft: TanDraft }>("/api/tan/drafts", {
    text,
    reply_to: opts.replyTo,
    expires_in: opts.expiresIn,
  });
  return data.draft;
}

/** Discard a saved Tan post draft without publishing it. */
export async function deleteTanDraft(id: number): Promise<unknown> {
  return post("/api/tan/drafts/delete", { id });
}

export async function quoteTanPost(target: string, text: string): Promise<TanPost> {
  return post("/api/tan/quote", { target, text });
}

export async function likeTanPost(target: string): Promise<unknown> {
  return post("/api/tan/like", { target });
}

export async function unlikeTanPost(target: string): Promise<unknown> {
  return post("/api/tan/unlike", { target });
}

/** Vote on a Tan poll: `option` is the 0-based option index. */
export async function voteTanPoll(target: string, option: number): Promise<unknown> {
  return post("/api/tan/vote", { target, option });
}

export async function createTanPoll(
  question: string,
  options: string[],
  expiresIn?: number,
): Promise<TanPost> {
  return post("/api/tan/poll", { question, options, expires_in: expiresIn });
}

export async function repostTanPost(target: string): Promise<unknown> {
  return post("/api/tan/repost", { target });
}

/** Share a post with a peer (`to`) or into a group (`group`). */
export async function shareTanPost(
  target: string,
  dest: { to: string } | { group: string },
  timeout = 20,
): Promise<unknown> {
  return post("/api/tan/share", { target, ...dest, timeout });
}

export async function deleteTanPost(target: string): Promise<unknown> {
  return post("/api/tan/delete", { target });
}

export async function editTanPost(target: string, text: string): Promise<unknown> {
  return post("/api/tan/edit", { target, text });
}

export async function followTanAuthor(target: string): Promise<unknown> {
  return post("/api/tan/follow", { target });
}

export async function unfollowTanAuthor(target: string): Promise<unknown> {
  return post("/api/tan/unfollow", { target });
}

export async function muteTanAuthor(target: string): Promise<unknown> {
  return post("/api/tan/mute", { target });
}

export async function unmuteTanAuthor(target: string): Promise<unknown> {
  return post("/api/tan/unmute", { target });
}

export async function muteTanKeyword(keyword: string): Promise<unknown> {
  return post("/api/tan/mute-keyword", { keyword });
}

export async function unmuteTanKeyword(keyword: string): Promise<unknown> {
  return post("/api/tan/unmute-keyword", { keyword });
}

export async function followTanTag(tag: string): Promise<unknown> {
  return post("/api/tan/follow-tag", { tag });
}

export async function unfollowTanTag(tag: string): Promise<unknown> {
  return post("/api/tan/unfollow-tag", { tag });
}

export async function muteTanTag(tag: string): Promise<unknown> {
  return post("/api/tan/mute-tag", { tag });
}

export async function unmuteTanTag(tag: string): Promise<unknown> {
  return post("/api/tan/unmute-tag", { tag });
}

export async function saveTanPost(target: string): Promise<unknown> {
  return post("/api/tan/save", { target });
}

export async function unsaveTanPost(target: string): Promise<unknown> {
  return post("/api/tan/unsave", { target });
}

export async function hideTanPost(target: string): Promise<unknown> {
  return post("/api/tan/hide", { target });
}

export async function unhideTanPost(target: string): Promise<unknown> {
  return post("/api/tan/unhide", { target });
}

export async function pinTanPost(target: string): Promise<unknown> {
  return post("/api/tan/pin", { target });
}

export async function unpinTanPost(target: string): Promise<unknown> {
  return post("/api/tan/unpin", { target });
}

/** Pull missing posts from peers (`explore` defaults true). */
export async function syncTan(explore = true): Promise<unknown> {
  return post("/api/tan/sync", { explore });
}

// -------------------------------------------------------------- channels --

export async function getChannels(): Promise<Channel[]> {
  const data = await get<{ channels: Channel[]; count: number }>("/api/channels");
  return data.channels;
}

export async function getChannelMessages(channel: string, limit = 20): Promise<ChannelMessage[]> {
  const data = await get<{ channel: string; messages: ChannelMessage[]; count: number }>(
    `/api/channels/messages${query({ channel, limit })}`,
  );
  return data.messages;
}

/** Stored messages in a channel matching a keyword, newest first. */
export async function searchChannelMessages(
  channel: string,
  q: string,
  limit = 20,
): Promise<ChannelMessage[]> {
  const data = await get<{ channel: string; query: string; messages: ChannelMessage[]; count: number }>(
    `/api/channels/search${query({ channel, q, limit })}`,
  );
  return data.messages;
}

export async function joinChannel(channel: string): Promise<Channel> {
  return post("/api/channels/join", { channel });
}

export async function leaveChannel(channel: string): Promise<{ channel: string; left: boolean }> {
  return post("/api/channels/leave", { channel });
}

export async function sendChannelMessage(
  channel: string,
  text: string,
  replyTo?: string,
  expiresIn?: number,
): Promise<ChannelMessage> {
  return post("/api/channels/send", {
    channel,
    text,
    ...(replyTo ? { reply_to: replyTo } : {}),
    ...(expiresIn !== undefined ? { expires_in: expiresIn } : {}),
  });
}

export async function editChannelMessage(
  channel: string,
  target: string,
  text: string,
): Promise<{ hash: string }> {
  const data = await post<{ edit: { hash: string } }>("/api/channels/edit", {
    channel,
    target,
    text,
  });
  return data.edit;
}

/** React to a stored channel message; an empty reaction clears yours. */
export async function reactToChannelMessage(
  channel: string,
  target: string,
  reaction: string,
): Promise<{ reaction: string }> {
  return post("/api/channels/react", { channel, target, reaction });
}

export async function getChannelPins(channel: string): Promise<ChannelMessage[]> {
  const data = await get<{ channel: string; pins: ChannelMessage[]; count: number }>(
    `/api/channels/pins${query({ channel })}`,
  );
  return data.pins;
}

export async function pinChannelMessage(channel: string, hash: string): Promise<{ channel: string; hash: string; pinned_at: number }> {
  return post("/api/channels/pin", { channel, hash });
}

export async function unpinChannelMessage(channel: string, hash: string): Promise<{ channel: string; hash: string; unpinned: boolean }> {
  return post("/api/channels/unpin", { channel, hash });
}

export async function postChannelPoll(
  channel: string,
  question: string,
  options: string[],
): Promise<ChannelMessage> {
  const data = await post<{ ok: boolean; message: ChannelMessage }>("/api/channels/poll", {
    channel,
    question,
    options,
  });
  return data.message;
}

export async function voteChannelPoll(
  channel: string,
  poll: string,
  option: number | string,
): Promise<ChannelMessage> {
  const data = await post<{ ok: boolean; message: ChannelMessage }>("/api/channels/vote", {
    channel,
    poll,
    option,
  });
  return data.message;
}

export async function closeChannelPoll(channel: string, poll: string): Promise<ChannelMessage> {
  const data = await post<{ ok: boolean; message: ChannelMessage }>("/api/channels/close-poll", {
    channel,
    poll,
  });
  return data.message;
}

export async function getChannelScheduled(channel?: string): Promise<ScheduledChannelMessage[]> {
  const data = await get<{ scheduled: ScheduledChannelMessage[]; count: number }>(
    `/api/channels/scheduled${query({ channel })}`,
  );
  return data.scheduled;
}

export async function scheduleChannelMessage(
  channel: string,
  text: string,
  sendIn: number,
): Promise<ScheduledChannelMessage> {
  return post("/api/channels/schedule", { channel, text, send_in: sendIn });
}

/** Queue a channel poll for later: the question plus its fixed options. */
export async function scheduleChannelPoll(
  channel: string,
  question: string,
  options: string[],
  sendIn: number,
): Promise<ScheduledChannelMessage> {
  const data = await post<{ ok: boolean; scheduled: ScheduledChannelMessage }>(
    "/api/channels/schedule",
    {
      channel,
      text: question,
      poll_options: options,
      send_in: sendIn,
    },
  );
  return data.scheduled;
}

export async function cancelScheduledChannelMessage(id: string): Promise<unknown> {
  return post("/api/channels/cancel-scheduled", { id });
}

export async function editScheduledChannelMessage(
  id: string,
  patch: { text?: string; sendIn?: number },
): Promise<ScheduledChannelMessage> {
  const body: { id: string; text?: string; send_in?: number } = { id };
  if (patch.text !== undefined) body.text = patch.text;
  if (patch.sendIn !== undefined) body.send_in = patch.sendIn;
  const data = await post<{ ok: boolean; edited: ScheduledChannelMessage }>(
    "/api/channels/edit-scheduled",
    body,
  );
  return data.edited;
}

/** Pull missing history for one channel, or every joined channel when omitted. */
export async function syncChannels(channel?: string, limit = 20): Promise<unknown> {
  return post("/api/channels/sync", channel ? { channel, limit } : { limit });
}

export async function getChannelTopic(channel: string): Promise<ChannelTopic | null> {
  const data = await get<{ channel: string; topic: ChannelTopic | null }>(
    `/api/channels/topic${query({ channel })}`,
  );
  return data.topic;
}

/** Set a channel topic; an empty topic clears it. */
export async function setChannelTopic(
  channel: string,
  topic: string,
): Promise<{ ok: boolean; channel: string; topic: ChannelTopic; cleared: boolean }> {
  return post("/api/channels/topic", { channel, topic });
}

// ----------------------------------------------------------------- pages --

export async function getPages(): Promise<PageList> {
  return get("/api/pages");
}

/** Fetch a peer's page over RNS; returns the stored copy when nothing is newer. */
export async function fetchPage(peer: string): Promise<{ page: MeshPage | null }> {
  return post("/api/pages/fetch", { peer });
}

/** URL for a file linked from a stored page. */
export function pageFileUrl(author: string, sha256: string): string {
  return `${API_BASE}/api/pages/${enc(author)}/files/${enc(sha256)}`;
}
