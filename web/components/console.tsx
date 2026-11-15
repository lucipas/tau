"use client";

import { useEffect, useRef, useState } from "react";

import { useTau } from "@/lib/use-tau";
import type { Peer, TauMessage } from "@/lib/types";

import { BundleSection } from "./bundle-section";
import { Composer } from "./composer";
import { AllowlistSection } from "./allowlist-section";
import { DmRequestsSection } from "./dm-requests-section";
import { DoctorSection } from "./doctor-section";
import { CONSOLE_TABS, Header, type ConsoleTab } from "./header";
import { MessageFeed } from "./message-feed";
import { MessagesBackupSection } from "./messages-backup-section";
import { OutboxSection } from "./outbox-section";
import { PeerSidebar } from "./peer-sidebar";
import { SettingsSection } from "./settings-section";
import { SocialConsole } from "./social-console";
import { SectionLabel } from "./ui";

// The tab named by the URL hash, so a refresh or a shared link lands
// on the same section; anything else falls back to chat.
function tabFromHash(): ConsoleTab {
  if (typeof window === "undefined") return "chat";
  const hash = window.location.hash.replace(/^#/, "");
  return CONSOLE_TABS.some((entry) => entry.id === hash)
    ? (hash as ConsoleTab)
    : "chat";
}

// Defaults are stored under canonical destination hashes, but a custom
// target can be a name, a hash with stray whitespace, or mixed case.
// Map it to the stored key so the picker shows the saved default.
function canonicalPeerTarget(target: string, peers: Peer[]): string {
  const trimmed = target.trim();
  const lowered = trimmed.toLowerCase();
  const match = peers.find(
    (peer) =>
      peer.destination.toLowerCase() === lowered ||
      peer.name === trimmed ||
      peer.alias === trimmed,
  );
  return match ? match.destination.toLowerCase() : lowered;
}

// Peers whose conversation is not archived: the only peers the console
// offers as send targets. An archived thread stays hidden until restored.
function activePeersOf(
  peers: Peer[],
  archived: { destination: string }[],
): Peer[] {
  return peers.filter(
    (peer) =>
      !archived.some((entry) => entry.destination === peer.destination),
  );
}

export function Console() {
  const tau = useTau();
  const [target, setTarget] = useState("");
  const [mode, setMode] = useState<"peer" | "custom" | "group">("peer");
  const [replyTo, setReplyTo] = useState<TauMessage | null>(null);
  // Starts on chat so the prerendered HTML matches the first browser
  // render; the hash is read after mount below.
  const [tab, setTabState] = useState<ConsoleTab>("chat");
  // The outbox drawer starts open when mail is already queued, so the
  // pending work is visible without a click; it starts closed when the
  // queue is empty.
  const [outboxOpen, setOutboxOpen] = useState(
    () => tau.outbox.length > 0 || tau.heldMail.length > 0,
  );

  // useTau loads the queue asynchronously, so mail can arrive after
  // mount. Open the drawer once when it first appears; a later manual
  // collapse must not be overridden.
  const outboxCount = tau.outbox.length + tau.heldMail.length;
  const outboxAutoOpened = useRef(false);
  useEffect(() => {
    if (outboxAutoOpened.current || outboxCount === 0) return;
    outboxAutoOpened.current = true;
    setOutboxOpen(true);
  }, [outboxCount]);

  function setTab(next: ConsoleTab) {
    setTabState(next);
    // replaceState keeps the tab in the URL without adding history
    // entries or firing hashchange.
    window.history.replaceState(null, "", `#${next}`);
  }

  // A hash edited by hand, or a back/forward step, switches tabs too.
  useEffect(() => {
    setTabState(tabFromHash());
    const onHashChange = () => setTabState(tabFromHash());
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  // Alt+1 through Alt+4 jump between tabs, like the header pills.
  // Ignored while typing in a field, so Alt+digit never hijacks
  // text entry.
  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if (!event.altKey) return;
      const index = ["Digit1", "Digit2", "Digit3", "Digit4"].indexOf(
        event.code,
      );
      if (index === -1) return;
      const target = event.target as HTMLElement | null;
      if (
        target !== null &&
        (target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.tagName === "SELECT" ||
          target.isContentEditable)
      ) {
        return;
      }
      event.preventDefault();
      setTab(CONSOLE_TABS[index].id);
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  const customPicked = useRef(false);
  // The selection as it is now, for async completions that must not act
  // on a conversation the user has since switched to.
  const targetRef = useRef(target);

  useEffect(() => {
    targetRef.current = target;
  }, [target]);

  useEffect(() => {
    if (tau.peers.length === 0 && tau.groups.length === 0) {
      // A group that just left must not stay behind as the destination.
      if (mode === "group") {
        setTarget("");
        setReplyTo(null);
      }
      setMode("custom");
      return;
    }
    // An archive from another tab or the CLI hides the selected
    // conversation: never keep submitting to a hidden thread. Fall back
    // to a remaining peer, then to a group.
    if (
      mode === "peer" &&
      target &&
      tau.archivedConversations.some(
        (entry) => entry.destination === target,
      )
    ) {
      const next = tau.peers.find(
        (peer) =>
          !tau.archivedConversations.some(
            (entry) => entry.destination === peer.destination,
          ),
      );
      if (next) {
        setTarget(next.destination);
      } else if (tau.groups.length > 0) {
        setMode("group");
        setTarget(tau.groups[0].id);
      } else {
        setMode("custom");
        setTarget("");
      }
      setReplyTo(null);
      return;
    }
    // A refresh can remove the selected group: never keep submitting its
    // stale id. Fall back to a remaining group, then to a peer.
    if (mode === "group" && !tau.groups.some((group) => group.id === target)) {
      // A roster edit replaces the id: follow the successor, not a
      // fallback group.
      const successor =
        tau.groupSwap?.from === target ? tau.groupSwap.to : null;
      if (successor && tau.groups.some((group) => group.id === successor)) {
        setTarget(successor);
        return;
      }
      if (tau.groups.length > 0) {
        setTarget(tau.groups[0].id);
      } else if (activePeersOf(tau.peers, tau.archivedConversations).length > 0) {
        setMode("peer");
        setTarget(activePeersOf(tau.peers, tau.archivedConversations)[0].destination);
      } else {
        setMode("custom");
        setTarget("");
      }
      return;
    }
    if (!target && !customPicked.current) {
      // Auto-select skips archived conversations: their threads are
      // hidden until restored.
      const firstActive = activePeersOf(tau.peers, tau.archivedConversations)[0];
      if (firstActive) {
        setMode("peer");
        setTarget(firstActive.destination);
      } else if (tau.groups.length > 0) {
        setMode("group");
        setTarget(tau.groups[0].id);
      } else {
        // Every peer is archived and there are no groups: nothing to
        // send to, so offer the custom-address mode instead of an
        // empty peer selection.
        setMode("custom");
        setTarget("");
      }
    }
  }, [tau.peers, tau.groups, tau.groupSwap, tau.archivedConversations, target, mode]);

  function changeMode(next: "peer" | "custom" | "group") {
    customPicked.current = next === "custom";
    setMode(next);
  }

  function handleTargetChange(value: string) {
    // Retargeting the composer invalidates the pending reply: the
    // quoted message belongs to the old conversation, and sending it
    // to a different peer or group would be rejected by the bridge.
    // A group message's conversation is its group; a DM's is its peer.
    if (replyTo && value !== (replyTo.group ?? replyTo.peer ?? "")) {
      setReplyTo(null);
    }
    // Leaving a conversation drops its hold: a restored or
    // mark-unread hold only protects mail while the conversation
    // stays open, so returning to it follows the normal read path
    // instead of skipping markRead. A retarget to the same
    // conversation keeps the hold.
    if (value !== target) manualUnread.current.delete(target);
    setTarget(value);
  }

  // A deleted message can no longer be quoted: the bridge rejects its
  // wire id. This also covers deletes that arrive from another tab.
  useEffect(() => {
    if (!replyTo) return;
    if (!tau.messages.some((message) => message.id === replyTo.id)) {
      setReplyTo(null);
    }
  }, [replyTo, tau.messages]);

  const { markRead } = tau;
  const openUnread = target
    ? mode === "peer"
      ? (tau.unread.per_peer[target] ?? 0)
      : mode === "group"
        ? (tau.unread.per_group[target] ?? 0)
        : 0
    : 0;

  // A deliberate mark-unread holds the badge for the open conversation:
  // the auto-read below must not undo it the moment the count rises.
  // The hold records the count the mark produced; new mail above that
  // level releases it, so a conversation that gets new mail while open
  // still reads as read. Reopening the conversation clears it too.
  const manualUnread = useRef(new Map<string, number>());
  // How many times each conversation has been (re)opened: a mark-unread
  // response that arrives after a reopen must not reinstall its hold.
  const unreadGen = useRef(new Map<string, number>());

  // Restored mail is not new mail: when another console unmutes or
  // unignores the open conversation, the server names the restored
  // count in the unread event, and this console holds it — the same
  // hold the initiating console sets from the unmute response — so
  // the auto-read below does not clear it the moment the badge
  // rises. New mail above the held level releases the hold, like a
  // mark-unread hold. Only the conversation open when the restore
  // lands is held: opening one later marks it read, as before.
  useEffect(() => {
    if (!tau.restoredUnread) return;
    const open = targetRef.current;
    if (!open) return;
    const entry = tau.restoredUnread.find((item) => item.target === open);
    if (entry !== undefined) manualUnread.current.set(open, entry.unread);
  }, [tau.restoredUnread]);

  // The open conversation reads as read: the auto-selected one on load,
  // and one that gets new mail while it stays open. A failed call leaves
  // the badge in place until the count next changes.
  // Mail arriving while another tab is shown stays unread until the chat
  // feed is visible again.
  useEffect(() => {
    if (!target || tab !== "chat") return;
    if (openUnread === 0) {
      // Read elsewhere (another tab, the CLI): the reminder is gone,
      // so the hold goes with it — new mail must auto-read again.
      manualUnread.current.delete(target);
      return;
    }
    const held = manualUnread.current.get(target);
    if (held !== undefined && openUnread <= held) return;
    if (held !== undefined) manualUnread.current.delete(target);
    // A group target marks the group's posts read; a peer target
    // marks only that DM, never the sender's group posts.
    void (
      mode === "group" ? markRead(undefined, target) : markRead(target)
    ).catch(() => undefined);
  }, [target, mode, openUnread, markRead, tab]);

  function selectPeer(destination: string) {
    customPicked.current = false;
    setMode("peer");
    setTarget(destination);
    setReplyTo(null);
    // A sidebar peer opens the chat tab: the composer it fills lives
    // there, and the click means "message this peer".
    setTab("chat");
    // Reopening clears any mark-unread hold: opening the conversation
    // marks it read, as before.
    manualUnread.current.delete(destination);
    unreadGen.current.set(
      destination,
      (unreadGen.current.get(destination) ?? 0) + 1,
    );
    // Opening the conversation advances its read cursor on the node,
    // so the badge clears. A failed call leaves the badge in place.
    void tau.markRead(destination).catch(() => undefined);
    const textarea = document.getElementById(
      "message-text",
    ) as HTMLTextAreaElement | null;
    textarea?.focus();
    if (window.innerWidth < 1024) {
      textarea?.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }

  return (
    <div className="relative flex h-dvh flex-col overflow-hidden">
      <div className="mesh-grid pointer-events-none fixed inset-0" aria-hidden="true" />
      <div
        className="pointer-events-none fixed inset-x-0 top-0 h-96 bg-[radial-gradient(60%_100%_at_50%_0%,rgba(45,212,191,0.13),transparent_72%)]"
        aria-hidden="true"
      />

      <div className="relative flex min-h-0 flex-1 flex-col">
        <Header
          node={tau.node}
          connection={tau.connection}
          announcing={tau.announcing}
          onAnnounce={() => {
            void tau.announce().catch(() => undefined);
          }}
          onRename={async (name) => {
            await tau.renameNode(name);
          }}
          tab={tab}
          onTabChange={setTab}
        />

        {tau.connection === "reconnecting" ? (
          <div className="border-b border-amber/25 bg-amber/[0.07] px-4 py-1.5 text-center font-mono text-[11px] text-amber lg:px-8">
            local bridge unreachable — retrying. Start it with{" "}
            <code className="text-amber/90">tau serve --http</code>
          </div>
        ) : null}

        <main className="mx-auto grid w-full max-w-7xl min-h-0 flex-1 gap-4 overflow-y-auto px-4 py-4 lg:grid-cols-[340px_minmax(0,1fr)] lg:gap-5 lg:overflow-visible lg:px-8 lg:py-6">
          <div className="thin-scroll flex min-w-0 flex-col gap-4 lg:min-h-0 lg:overflow-y-auto lg:pr-1">
          <PeerSidebar
            node={tau.node}
            peers={tau.peers}
            groups={tau.groups}
            bridge={tau.bridge}
            social={tau.social}
            pings={tau.pings}
            infos={tau.infos}
            verifications={tau.verifications}
            blocks={tau.blocks}
            unread={tau.unread.per_peer}
            unreadGroups={tau.unread.per_group}
            archived={tau.archivedConversations}
            archivedError={tau.archivedError}
            muted={tau.mutedConversations}
            mutedError={tau.mutedError}
            selected={target}
            onPing={(destination) => {
              void tau.ping(destination);
            }}
            onInfo={(destination) => {
              void tau.fetchInfo(destination);
            }}
            onForgetPeer={async (destination, force) => {
              const removed = await tau.forgetPeer(destination, force);
              // The forgotten peer's conversation is gone: clear the
              // selection and let the auto-select pick the next peer. Only
              // if it is still selected; the user may have picked another
              // peer while the forget was pending.
              if (targetRef.current === removed.destination) {
                targetRef.current = "";
                setTarget("");
                setReplyTo(null);
              }
            }}
            onSetAlias={(destination, alias) =>
              tau.setPeerAlias(destination, alias)
            }
            onVerify={(destination) => {
              void tau.verifyPeer(destination).catch(() => undefined);
            }}
            onShowSafetyNumber={(destination) => {
              void tau.showSafetyNumber(destination).catch(() => undefined);
            }}
            onDismissVerification={(destination) => {
              tau.clearVerification(destination);
            }}
            onBlock={(destination) => {
              void tau.blockPeer(destination).catch(() => undefined);
            }}
            onUnblock={(destination) => {
              void tau.unblockPeer(destination).catch(() => undefined);
            }}
            onArchivePeer={async (destination) => {
              await tau.archiveConversation(destination);
              // The archived conversation is gone from the feed: clear
              // the selection and let the auto-select pick the next peer.
              // Only if it is still selected; the user may have picked
              // another peer while the archive was pending.
              if (targetRef.current === destination) {
                targetRef.current = "";
                setTarget("");
                setReplyTo(null);
              }
            }}
            onUnarchivePeer={async (destination) => {
              await tau.unarchiveConversation(destination);
            }}
            onSetMuted={async (muteTarget, muted) => {
              // Unmuting the open conversation restores the unread mail
              // that arrived while it was muted. That mail is not new, so
              // hold the count until the unmute answers: the auto-read
              // below must not mark it read. The hold starts unbounded
              // (an event may land before the response) and settles on
              // the restored count. Other open consoles get the same
              // hold from the restored signal in the unread event.
              const heldPeer =
                !muted &&
                muteTarget.peer !== undefined &&
                mode === "peer" &&
                targetRef.current === muteTarget.peer
                  ? muteTarget.peer
                  : undefined;
              const heldGroup =
                !muted &&
                muteTarget.group !== undefined &&
                mode === "group" &&
                targetRef.current === muteTarget.group
                  ? muteTarget.group
                  : undefined;
              const held = heldPeer ?? heldGroup;
              if (held !== undefined) {
                manualUnread.current.set(held, Number.POSITIVE_INFINITY);
              }
              try {
                const change = await tau.setMuted(muteTarget, muted);
                if (held !== undefined) {
                  if (targetRef.current === held) {
                    const restored =
                      heldPeer !== undefined
                        ? (change.unread?.per_peer[held] ?? 0)
                        : (change.unread?.per_group[held] ?? 0);
                    manualUnread.current.set(held, restored);
                  } else {
                    manualUnread.current.delete(held);
                  }
                }
              } catch (error) {
                if (held !== undefined) {
                  manualUnread.current.delete(held);
                }
                throw error;
              }
            }}
            onMarkUnread={async (destination, all = false) => {
              const gen = unreadGen.current.get(destination) ?? 0;
              const counts = await tau.markUnread(destination, all);
              // A reopen since the click invalidates this hold, and so
              // does leaving the conversation: install it only if this
              // selection is still current and still open.
              if ((unreadGen.current.get(destination) ?? 0) !== gen) return;
              if (targetRef.current !== destination) return;
              manualUnread.current.set(
                destination,
                counts.per_peer[destination] ?? 0,
              );
            }}
            onSelectPeer={selectPeer}
            onCreateGroup={(name, members) => tau.createGroup(name, members)}
            onDeleteGroup={(group) => tau.deleteGroup(group)}
            onLeaveGroup={(group) => tau.leaveGroup(group)}
            onRejoinGroup={(group) => tau.rejoinGroup(group)}
            onIgnoreGroup={(group) => tau.ignoreGroup(group)}
            onUnignoreGroup={(group) => tau.unignoreGroup(group)}
            leftGroups={tau.leftGroups}
            ignoredGroups={tau.ignoredGroups}
            onAddGroupMembers={(group, members) =>
              tau.addGroupMembers(group, members)
            }
            onRemoveGroupMembers={(group, members) =>
              tau.removeGroupMembers(group, members)
            }
            onRenameGroup={(group, name) => tau.renameGroup(group, name)}
            onSetGroupTopic={(group, topic) =>
              tau.setGroupTopic(group, topic)
            }
          />
          </div>

          <div className="flex min-h-0 min-w-0 flex-col">
            {/* Stays mounted while another tab is shown, so the composer's
                draft, attachments, and send error survive a tab switch. */}
            <div className={tab === "chat" ? "flex min-h-0 flex-1 flex-col" : "hidden"}>
            <MessageFeed
              messages={
                tau.searchQuery !== null
                  ? tau.searchResults
                  : tau.pinnedMessages ?? tau.messages
              }
              node={tau.node}
              peers={tau.peers}
              groups={tau.groups}
              live={tau.connection === "live"}
              searching={tau.searchQuery !== null}
              searchQuery={tau.searchQuery}
              searchError={tau.searchError}
              quoteContext={tau.messages}
              onSearch={tau.search}
              onClearSearch={tau.clearSearch}
              showingPinned={tau.pinnedMessages !== null}
              pinnedError={tau.pinnedError}
              onShowPinned={tau.showPinned}
              onClearPinned={tau.clearPinned}
              onReply={(message) => {
                setReplyTo(message);
                // A group message belongs to its group conversation, even
                // though it also names a member peer. Jumping to the
                // conversation opens it, so clear any hold on it first:
                // the auto-read below marks it read, like a reopen.
                if (message.group) {
                  manualUnread.current.delete(message.group);
                  setTarget(message.group);
                  setMode("group");
                } else if (message.peer) {
                  manualUnread.current.delete(message.peer);
                  setTarget(message.peer);
                  setMode("peer");
                }
              }}
              onDelete={async (message) => {
                await tau.deleteMessage(message.id);
              }}
              onEdit={async (message, text) => {
                const result = await tau.editMessage(message.id, text);
                return result.edited;
              }}
              onRetract={async (message) => {
                await tau.retractMessage(message.id);
              }}
              onForward={async (message, to) => {
                await tau.forwardMessage(message.id, to);
              }}
              onForwardGroup={async (message, group) => {
                await tau.forwardMessageToGroup(message.id, group);
              }}
              onReact={async (message, reaction) => {
                if (!message.wire_id) return;
                if (reaction === null) {
                  await tau.clearReaction(message.wire_id);
                } else {
                  await tau.reactToMessage(message.wire_id, reaction);
                }
              }}
              onPin={async (message, pinned) => {
                if (pinned) {
                  await tau.pinMessage(message.id);
                } else {
                  await tau.unpinMessage(message.id);
                }
              }}
            />
            <div className="mt-4 shrink-0">
            <Composer
              peers={activePeersOf(tau.peers, tau.archivedConversations)}
              groups={tau.groups}
              target={target}
              mode={mode}
              onTargetChange={handleTargetChange}
              onModeChange={changeMode}
              onSend={tau.send}
              onSendGroup={tau.sendGroup}
              onSendFile={tau.sendFile}
              onSendGroupFile={tau.sendGroupFile}
              replyTo={replyTo}
              onCancelReply={() => setReplyTo(null)}
              disappearingDefault={
                target.trim() === ""
                  ? null
                  : mode === "group"
                    ? (tau.disappearingDefaults.groups[target.trim()] ?? null)
                    : (tau.disappearingDefaults.peers[
                        canonicalPeerTarget(target, tau.peers)
                      ] ?? null)
              }
              disappearingError={tau.disappearingError}
              onSetDisappearingDefault={
                target.trim() === ""
                  ? undefined
                  : async (seconds) => {
                      if (mode === "group") {
                        await tau.setDisappearingDefault(
                          { group: target },
                          seconds,
                        );
                      } else {
                        await tau.setDisappearingDefault(
                          { peer: target },
                          seconds,
                        );
                      }
                    }
              }
            />
            </div>
            {/* Below the composer so an expanded queue never pushes it
                away from the feed. Capped in height so a long list
                scrolls instead of clipping the composer. */}
            <div className="mt-4 shrink-0 max-h-[30dvh] overflow-y-auto thin-scroll">
              <DmRequestsSection
                requests={tau.dmRequests}
                onAccept={tau.acceptDmRequest}
                onDecline={tau.declineDmRequest}
              />
            </div>
            <div className="mt-4 shrink-0 rounded-2xl border border-line/80 bg-panel/40">
              <button
                type="button"
                onClick={() => setOutboxOpen((open) => !open)}
                aria-expanded={outboxOpen}
                className="flex w-full items-center justify-between px-4 py-3 text-left"
              >
                <span className="font-mono text-[11px] uppercase tracking-[0.14em] text-mist">
                  outbox
                </span>
                <span className="font-mono text-[10px] text-mist">
                  {tau.outbox.length} queued
                  {tau.heldMail.length > 0
                    ? ` · ${tau.heldMail.length} held`
                    : ""}
                  {outboxOpen ? " ▲" : " ▼"}
                </span>
              </button>
              {/* Hidden rather than unmounted when collapsed, so a retry's
                  result and any pending confirmation survive a collapse.
                  OutboxSection renders nothing itself when there is
                  nothing to show. */}
              <div
                className={
                  outboxOpen
                    ? "max-h-[30dvh] overflow-y-auto thin-scroll border-t border-line/60 p-4"
                    : "hidden"
                }
              >
                <OutboxSection
                  entries={tau.outbox}
                  heldForOthers={tau.heldForOthers}
                  held={tau.heldMail}
                  onCancel={tau.cancelOutbox}
                  onRetry={tau.retryOutbox}
                  onDropHeld={tau.dropHeldMail}
                />
              </div>
            </div>
            </div>
            {/* Stays mounted so the social composer's draft and reply or
                quote target survive a tab switch. */}
            <div
              className={
                tab === "social"
                  ? "thin-scroll min-h-0 flex-1 overflow-y-auto"
                  : "hidden"
              }
            >
              <SocialConsole embedded />
            </div>
            {tab === "tools" ? (
              <div className="thin-scroll flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto lg:pr-1">
                <BundleSection
                  onExport={tau.exportBundle}
                  onImport={tau.importBundle}
                />
                <DoctorSection onRun={tau.runDoctor} />
                <MessagesBackupSection
                  exporting={tau.exporting}
                  importing={tau.importing}
                  onExport={tau.exportMessages}
                  onImport={tau.importMessages}
                />
                <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
                  <SectionLabel>node backup</SectionLabel>
                  <p className="mt-2 text-[11px] leading-relaxed text-mist">
                    Download the node data directory: identity, stores, and
                    settings.
                  </p>
                  <div className="mt-3 flex items-center gap-2">
                    <button
                      type="button"
                      onClick={() => {
                        void tau.backup();
                      }}
                      disabled={!tau.node || tau.backingUp}
                      className="flex-1 rounded-lg border border-line bg-void/70 px-2.5 py-2 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
                    >
                      {tau.backingUp ? "backing up…" : "download backup"}
                    </button>
                    {tau.backupError ? (
                      <span role="alert" className="font-mono text-[11px] text-rose-300">
                        {tau.backupError}
                      </span>
                    ) : null}
                  </div>
                </section>
              </div>
            ) : null}
            {tab === "settings" ? (
              <div className="thin-scroll flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto lg:pr-1">
                <AllowlistSection
                  allowlist={tau.allowlist}
                  onAdd={(value) => tau.addAllowlistPeer(value)}
                  onRemove={(value) => tau.removeAllowlistPeer(value)}
                  onToggle={(enabled) => tau.setAllowlistEnabled(enabled)}
                />
                <SettingsSection
                  settings={tau.settings}
                  loadError={tau.settingsError}
                  onToggle={tau.updateSetting}
                />
              </div>
            ) : null}
          </div>
        </main>

        <footer className="mx-auto flex w-full max-w-7xl shrink-0 flex-wrap items-center justify-between gap-2 px-4 pb-6 font-mono text-[10px] text-mist/60 lg:px-8">
          <span>
            tau {tau.node?.version ?? "—"} · Reticulum{" "}
            {tau.node?.rns_version ?? "—"}
          </span>
          <span>no servers · no accounts · encrypted links</span>
        </footer>
      </div>
    </div>
  );
}
    </span>
          <span>no servers · no accounts · encrypted links</span>
        </footer>
      </div>
    </div>
  );
}
ccounts · encrypted links</span>
        </footer>
      </div>
    </div>
  );
}
counts · encrypted links</span>
        </footer>
      </div>
    </div>
  );
}
(value)}
                  onToggle={(enabled) => tau.setAllowlistEnabled(enabled)}
                />
                <SettingsSection
                  settings={tau.settings}
                  loadError={tau.settingsError}
                  onToggle={tau.updateSetting}
                />
              </div>
            ) : null}
          </div>
        </main>

        <footer className="mx-auto flex w-full max-w-7xl shrink-0 flex-wrap items-center justify-between gap-2 px-4 pb-6 font-mono text-[10px] text-mist/60 lg:px-8">
          <span>
            tau {tau.node?.version ?? "—"} · Reticulum{" "}
            {tau.node?.rns_version ?? "—"}
          </span>
          <span>no servers · no accounts · encrypted links</span>
        </footer>
      </div>
    </div>
  );
}
    </span>
          <span>no servers · no accounts · encrypted links</span>
        </footer>
      </div>
    </div>
  );
}
ccounts · encrypted links</span>
        </footer>
      </div>
    </div>
  );
}
counts · encrypted links</span>
        </footer>
      </div>
    </div>
  );
}
