"""Command line interface for tau."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import shutil
import signal
import socket
import sqlite3
import sys
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import AbstractSet, Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from . import __version__
from .allowlist import (
    AllowListError,
    allowlist_lock,
    clean_destination,
    load_allowlist,
    load_allowlist_or_deny_all,
    save_allowlist,
)
from .archive import (
    ArchiveError,
    archive_lock,
    load_archive,
    load_archived_set,
    save_archive,
)
from .away import (
    AwayError,
    AwayState,
    away_lock,
    load_away,
    save_away,
)
from .backup import create_backup, restore_backup
from .blocklist import (
    BlockListError,
    load_blocked_set,
    load_blocklist,
    save_blocklist,
)
from .bundle import (
    DEFAULT_MAX_BUNDLE_BYTES,
    apply_bundle,
    build_bundle,
    inspect_bundle,
    parse_bundle,
    serialize_bundle,
)
from .config import (
    ANNOUNCE_INTERVAL_DEFAULT,
    ANNOUNCE_INTERVAL_MAX,
    TauConfig,
    accept_files_enabled,
    announce_interval,
    load_node_name,
    parse_host_port,
    send_read_receipts_enabled,
    set_node_setting,
    tan_sync_depth,
    write_node_metadata,
    write_rns_config,
)
from .beacons import (
    BEACON_DEFAULT_DURATION,
    BEACON_MAX_DURATION,
    BEACON_MAX_INTERVAL,
    BEACON_MIN_INTERVAL,
    BEACON_UPDATE_INTERVAL,
    BeaconsError,
    beacons_lock,
    load_beacons,
    read_location_file,
    save_beacons,
)
from .disappearing import (
    MAX_DISAPPEARING_SECONDS,
    DisappearingError,
    disappearing_lock,
    load_disappearing_defaults,
    save_disappearing_defaults,
)
from .drafts import DraftStore
from .reminders import (
    MAX_REMINDER_DELAY,
    ReminderError,
    ReminderStore,
    parse_remind_at,
)
from .muted import (
    MutedError,
    expand_muted_groups,
    group_lineage,
    load_muted,
    muted_groups_for,
    muted_lock,
    muted_peers_for,
    save_muted,
)
from .pinned import (
    PinnedError,
    clean_pinned_target,
    load_pinned,
    load_pinned_set,
    pinned_lock,
    save_pinned,
)
from .saved_messages import (
    SavedMessages,
    SavedMessagesError,
    load_saved_messages,
    save_saved_messages,
    saved_messages_lock,
)
from .quiet_hours import (
    QuietHours,
    QuietHoursError,
    format_time,
    load_quiet_hours,
    quiet_hours_active,
    quiet_hours_lock,
    save_quiet_hours,
)
from .dm_requests import (
    DmRequestsError,
    accept_dm_request,
    blocklist_edit_lock,
    decline_dm_request,
    list_dm_requests,
)
from .devices import (
    DEFAULT_PAIRING_TTL_SECONDS,
    MAX_PAIRING_BUNDLE_BYTES,
    MAX_PAIRING_TTL_SECONDS,
    DeviceRegistry,
    DevicesError,
    build_pairing_bundle,
    verify_pairing_bundle,
)
from .doctor import all_ok, run_checks
from .identity import (
    IDENTITY_FILE_MODE,
    IdentityError,
    _identity_lock,
    destination_hash,
    load_identity,
    load_or_create_identity,
    save_identity,
)
from .group_invites import (
    JOIN_REQUEST_KIND,
    InviteError,
    invite_status,
    invite_token_group_label,
    mint_invite,
    parse_invite_link,
    revoke_invites,
)
from .broadcasts import BroadcastStore
from .channel_polls import (
    attach_channel_poll_tallies,
    channel_poll_tally,
    is_channel_poll,
    is_channel_poll_close,
    is_channel_poll_vote,
    poll_identity as channel_poll_identity,
    resolve_channel_poll,
)
from .groups import GroupStore, pinned_group, pinned_rosters, visible_reactions
from .group_polls import (
    PostError,
    attach_dm_poll_tallies,
    attach_group_poll_tallies,
    clean_poll_options,
    dm_poll_tally,
    group_poll_tally,
    is_dm_poll,
    is_dm_poll_close,
    is_dm_poll_vote,
    is_group_poll,
    is_group_poll_close,
    is_group_poll_vote,
    poll_identity,
)
from .ignored_groups import (
    IgnoredGroups,
    IgnoredGroupsError,
    ignored_groups_lock,
    load_ignored_group_ids,
    load_ignored_groups,
    save_ignored_groups,
)
from .inbox import Inbox, InboxError, _conversation_key
from .lxmf import lxmf_delivery_hash
from .mailbox import Mailbox, MailboxError
from .node import (
    InstanceAlreadyRunning,
    InstanceLock,
    NodeError,
    TauNode,
    check_tcp_listen_ports,
    instance_lock_holder,
    resolve_received_file,
)
from .peers import Peer, PeerError, PeerRegistry, safety_number
from .follows import (
    FollowListError,
    FollowListStore,
    build_follow_list,
    load_local_follow_list,
    public_follow_list,
    save_local_follow_list,
)
from .pages import (
    PageError,
    PageStore,
    build_page,
    clean_page_files,
    load_local_page,
    load_page_file,
    make_page_file_entry,
    public_page,
    save_local_page,
    store_page_file,
    title_from_markdown,
)
from .profile import (
    MAX_BIO_CHARS,
    MAX_LINKS,
    ProfileError,
    ProfileStore,
    build_profile,
    load_local_profile,
    public_profile,
    resolve_avatar,
    save_local_profile,
)
from .scheduled_posts import (
    MAX_SCHEDULED_POST_DELAY,
    ScheduledPostsError,
    cancel_scheduled_post,
    load_scheduled_posts,
    schedule_poll,
    schedule_post,
    schedule_quote,
    schedule_repost,
)
from .scheduled_channel import (
    MAX_SCHEDULED_CHANNEL_DELAY,
    ScheduledChannelError,
    cancel_scheduled_channel_message,
    edit_scheduled_channel_message,
    load_scheduled_channel_messages,
    schedule_channel_message,
    schedule_channel_poll,
)
from .scheduled_files import (
    ScheduledFile,
    ScheduledFileError,
    _schedule_file_locked,
    cancel_scheduled_file,
    load_scheduled_files,
    new_scheduled_file_id,
    read_file_payload,
    remove_staged_file,
    scheduled_files_lock,
    stage_file,
)
from .protocol import DEFAULT_TCP_PORT as PROTOCOL_TCP_PORT
from .protocol import (
    DEFAULT_MESSAGE_KIND,
    NOTE_KIND,
    MAX_MAIL_TTL,
    MAX_MESSAGE_CHARS,
    PROTOCOL_VERSION,
    ProtocolError,
    clean_beacon,
    clean_forwarded_from_or_none,
    clean_in_reply_to,
    clean_location,
    clean_reaction,
    excerpt_hash,
    location_default_text,
    location_maps_url,
    normalize_message,
    normalize_message_kind,
    parse_location_arg,
    sanitize_name,
    short_hash,
)
from .util import atomic_write_bytes, atomic_write_text, ensure_dir
from .tangents.rules import RulesError, Ruleset, load_ruleset, save_ruleset
from .tangents.tan import (
    DEFAULT_FEED_LIMIT,
    DEFAULT_SUGGEST_LIMIT,
    DRAFT_CLAIM_TTL,
    IMAGE_MIME_TYPES,
    KIND_POLL,
    KIND_QUOTE,
    MAX_GOSSIP_TTL,
    MAX_IMAGE_BYTES,
    MAX_TAN_POST_EXPIRY,
    MIN_POLL_OPTIONS,
    TanApp,
    TanError,
    _NOT_DELETED,
    _author_from_identity,
    _clean_poll_options,
    _decode_options,
    _muted_keyword_list,
    _muted_tag_list,
    add_to_list,
    ancestry_hidden,
    annotate_edits,
    annotate_pinned,
    annotate_quote_counts,
    annotate_quotes,
    annotate_reactions,
    annotate_replies,
    annotate_repost_counts,
    annotate_reposts,
    annotate_saved,
    annotated_pinned_posts,
    annotated_saved_posts,
    apply_display_rules,
    author_stats,
    author_timeline,
    claim_draft,
    clean_list_name,
    clean_tag,
    create_list,
    delete_draft,
    delete_list,
    draft_claim_held,
    export_entries,
    follow,
    follow_tag,
    followed_tags,
    followers,
    following,
    find_post_by_image,
    get_draft,
    hidden_post_set,
    hidden_posts,
    hide_post,
    import_entries,
    init_db,
    likers,
    list_authors,
    list_drafts,
    list_member_authors,
    list_members,
    list_timeline,
    mentions,
    mute,
    mute_keyword,
    mute_tag,
    muted,
    muted_author_set,
    muted_keywords,
    muted_tags,
    open_db_readonly,
    open_readonly_db,
    pin_post,
    poll_results,
    quoters,
    record_draft_publish,
    refresh_draft_claim,
    release_draft_claim,
    reposters,
    remove_from_list,
    resolve_author,
    resolve_cached_author,
    resolve_delete_target,
    resolve_muted_name,
    resolve_post,
    resolve_share_post,
    save_draft,
    save_post,
    search_posts,
    share_text,
    store_media,
    suggest_authors,
    media_path,
    tags,
    tan_lists,
    thread,
    timeline,
    top_posts,
    unfollow,
    unfollow_tag,
    unhide_post,
    unmute,
    unmute_keyword,
    unmute_tag,
    unpin_post,
    unsave_post,
    verify_post,
    verify_store,
)
from .channels import (
    DEFAULT_CHANNEL_LIMIT,
    MAX_FEED_EDITS,
    AmbiguousDigestError,
    ChannelApp,
    ChannelError,
    channel_history_page,
    clean_channel_name,
    create_edit as create_channel_edit,
    create_message as create_channel_message,
    create_reaction as create_channel_reaction,
    get_message as get_channel_message,
    get_visible_message as get_visible_channel_message,
    HASH_HEX_LENGTH,
    init_db as init_channel_db,
    is_member as is_channel_member,
    reply_parent as channel_reply_parent,
    resolve_message_digest,
)

log = logging.getLogger(__name__)

DEFAULT_ANNOUNCE_INTERVAL = ANNOUNCE_INTERVAL_DEFAULT

_EXIT_OK = 0
_EXIT_ERROR = 1
_EXIT_INTERRUPTED = 130


# -- argument parsing ------------------------------------------------------


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    """Options shared by every subcommand.

    ``SUPPRESS`` defaults let the same options appear before *or* after the
    subcommand without the sub-parser clobbering values from the main parser.
    """
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=argparse.SUPPRESS,
        help="tau data directory (default: $TAU_HOME or ~/.tau)",
    )
    parser.add_argument(
        "--rns-config",
        type=Path,
        default=argparse.SUPPRESS,
        help="Reticulum config directory (default: DATA_DIR/reticulum)",
    )
    parser.add_argument(
        "--name",
        default=argparse.SUPPRESS,
        help="node name announced to peers",
    )
    parser.add_argument(
        "--rns-log-level",
        type=int,
        default=argparse.SUPPRESS,
        metavar="0-8",
        help="Reticulum log level (default: 2, or higher with -v)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=argparse.SUPPRESS,
        help="increase log verbosity (repeatable)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=argparse.SUPPRESS,
        help="only log errors",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        dest="json_output",
        help="emit machine readable JSON",
    )
def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    _add_common_options(common)

    parser = argparse.ArgumentParser(
        prog="tau",
        description=(
            "Peer-to-peer messaging over the Reticulum Network Stack. "
            "Every node gets an encrypted, self-authenticating address and "
            "discovers peers through signed announces."
        ),
    )
    _add_common_options(parser)
    parser.add_argument(
        "--version",
        action="version",
        version=f"tau {__version__} (protocol {PROTOCOL_VERSION})",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    init = subparsers.add_parser(
        "init",
        parents=[common],
        help="create a data directory, identity and Reticulum config",
    )
    init.add_argument(
        "--tcp-listen",
        nargs="?",
        const=str(PROTOCOL_TCP_PORT),
        metavar="[HOST:]PORT",
        help="add a TCP server interface (default port: %d)" % PROTOCOL_TCP_PORT,
    )
    init.add_argument(
        "--tcp-connect",
        metavar="HOST:PORT",
        help="add a TCP client interface",
    )
    init.add_argument(
        "--no-auto-interface",
        action="store_true",
        help="do not enable Reticulum's AutoInterface (LAN discovery)",
    )
    init.add_argument(
        "--init",
        action="store_true",
        help="write default Reticulum config (overwrites)",
    )
    init.add_argument(
        "--yes",
        action="store_true",
        help="skip the interactive setup dialogue, use the defaults",
    )
    init.set_defaults(func=cmd_init)

    serve = subparsers.add_parser(
        "serve",
        parents=[common],
        help="announce this node and answer peer requests",
    )
    serve.add_argument(
        "--announce-interval",
        type=float,
        default=None,
        metavar="SECONDS",
        help="periodic announce interval in seconds (default: the saved "
        "setting — see `tau config announce-interval` — 300 when unset; "
        "0 disables automatic announces)",
    )
    serve.add_argument(
        "--no-announce",
        action="store_true",
        help="do not send an announce on startup",
    )
    serve.add_argument(
        "--duration",
        type=float,
        default=None,
        metavar="SECONDS",
        help="stop after this many seconds (default: run until interrupted)",
    )
    serve.add_argument(
        "--http",
        nargs="?",
        const="127.0.0.1:8787",
        metavar="[HOST:]PORT",
        help=("serve the web console and JSON API (default: 127.0.0.1:8787)"),
    )
    serve.add_argument(
        "--web-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="static console build to serve (default: $TAU_WEB_DIR or web/out)",
    )
    serve.add_argument(
        "--http-allow-remote",
        action="store_true",
        help="allow binding the console to a non-loopback address",
    )
    serve.add_argument(
        "--social",
        action="store_true",
        help="enable the Tan social feed request handlers",
    )
    serve.add_argument(
        "--channels",
        action="store_true",
        help="enable the public channels request handlers",
    )
    serve.add_argument(
        "--social-sync-interval",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="periodically pull the Tan feed from peers (0 disables)",
    )
    serve.add_argument(
        "--lxmf",
        action="store_true",
        help=(
            "also run an LXMF router on this node's identity "
            "(requires the lxmf extra: pip install \"tau-p2p[lxmf]\")"
        ),
    )
    serve.add_argument(
        "--lxmf-propagation",
        action="store_true",
        help=(
            "also serve as an LXMF propagation node, holding mail for "
            "offline recipients (requires --lxmf)"
        ),
    )
    serve.set_defaults(func=cmd_serve)

    discover = subparsers.add_parser(
        "discover",
        parents=[common],
        help="listen for tau announces for a while",
    )
    discover.add_argument(
        "--duration",
        type=float,
        default=15.0,
        metavar="SECONDS",
        help="how long to listen (default: %(default)s)",
    )
    discover.add_argument(
        "--max-age",
        type=float,
        default=None,
        metavar="DAYS",
        help="only show peers heard within this many days",
    )
    discover.set_defaults(func=cmd_discover)

    heard = subparsers.add_parser(
        "heard",
        parents=[common],
        help="listen for non-tau announces for a while",
    )
    heard.add_argument(
        "--duration",
        type=float,
        default=15.0,
        metavar="SECONDS",
        help="how long to listen (default: %(default)s)",
    )
    heard.set_defaults(func=cmd_heard)

    send = subparsers.add_parser(
        "send",
        parents=[common],
        help="send a message to a peer",
    )
    send.add_argument(
        "--to",
        action="append",
        required=True,
        metavar="NAME|HASH",
        help="peer name or destination hash (repeatable: send to several peers)",
    )
    send.add_argument(
        "message", nargs="?", help="message text (reads stdin when omitted)"
    )
    send.add_argument("--file", type=Path, help="read the message from a file")
    send.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    send.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    send.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the message when the peer is offline",
    )
    send.add_argument(
        "--kind",
        default=None,
        metavar="SLUG",
        help="tag the message with a kind (default chat); receivers can"
        " filter their inbox by kind",
    )
    send.add_argument(
        "--location",
        default=None,
        metavar='"LAT,LON [PLACE]"',
        help="pin a place on the message (e.g. --location "
        '"34.05,-83.88 Rock Springs Park"); without message text the '
        "place name (or the coordinates) becomes the text, and the "
        "message is tagged location unless --kind says otherwise",
    )
    send.add_argument(
        "--poll-options",
        nargs="+",
        default=None,
        metavar="OPTION",
        help="post the message as a poll question with 2-8 options",
    )
    send.add_argument(
        "--wait",
        type=float,
        default=None,
        metavar="SECONDS",
        help="after queueing, wait up to SECONDS for the message to be delivered",
    )
    send.add_argument(
        "--reply-to",
        metavar="ID|PREFIX",
        help="quote an earlier message in this conversation (message id or unique prefix)",
    )
    send.add_argument(
        "--expire",
        metavar="DURATION",
        help="disappearing message: purge it from the receiver's inbox "
        "after this long (e.g. 30s, 5m, 2h, 7d; at most 30d)",
    )
    send.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the message: queue it now and deliver it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 29d23h — the 30d "
        "mail TTL less an hour of delivery slack)",
    )
    send.set_defaults(func=cmd_send)

    # `tau forward` shares `tau send`'s delivery flags: a forward is a
    # new message carrying the original text plus an attribution line.
    forward = subparsers.add_parser(
        "forward",
        parents=[common],
        help="forward a stored message to a peer, a group, a broadcast list, or a channel",
    )
    forward_target = forward.add_mutually_exclusive_group(required=True)
    forward_target.add_argument(
        "--to",
        action="append",
        metavar="NAME|HASH",
        help="peer name or destination hash (repeatable: forward to several peers)",
    )
    forward_target.add_argument(
        "--group",
        metavar="NAME|ID",
        help="group name or id: forward into the group instead of to a peer",
    )
    forward_target.add_argument(
        "--broadcast",
        metavar="NAME",
        help="broadcast list name: forward to every member instead of to a peer",
    )
    forward_target.add_argument(
        "--channel",
        metavar="NAME",
        help="channel name: forward into the channel instead of to a peer",
    )
    forward.add_argument(
        "message",
        metavar="ID|PREFIX",
        help="message id or unique prefix (see tau messages)",
    )
    forward.add_argument(
        "--timeout",
        type=float,
        default=None,
        metavar="SECONDS",
        help="how long to wait for a delivery; file legs default to "
        "300s (the send-file default) when this is not given",
    )
    forward.add_argument(
        "--discover-timeout",
        type=float,
        # SUPPRESS, not 10.0: every send path defaults a missing value
        # to 10s, but `cmd_forward` must be able to tell an explicitly
        # passed value from the default — a channel forward refuses
        # the flag instead of silently ignoring it.
        default=argparse.SUPPRESS,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    forward.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the message when the peer is offline",
    )
    forward.add_argument(
        "--wait",
        type=float,
        default=None,
        metavar="SECONDS",
        help="after queueing, wait up to SECONDS for the message to be delivered",
    )
    forward.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the forward: queue it now and deliver it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 29d23h — the 30d "
        "mail TTL less an hour of delivery slack)",
    )
    forward.set_defaults(func=cmd_forward)

    # `send --file` already means "read the message text from a file",
    # so file transfer gets its own subcommand instead of overloading it.
    send_file = subparsers.add_parser(
        "send-file",
        parents=[common],
        help="send a small file to a peer over an RNS Resource",
    )
    send_file.add_argument(
        "--to", required=True, metavar="NAME|HASH", help="peer name or destination hash"
    )
    send_file.add_argument(
        "path", type=Path, help="local file to send (at most 10 MiB)"
    )
    send_file.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        metavar="SECONDS",
        help="how long the whole transfer may take (default 300)",
    )
    send_file.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the file: stage it now and send it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 29d23h — the 30d "
        "mail TTL less an hour of delivery slack; needs a node "
        "serving at send time)",
    )
    send_file.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    send_file.set_defaults(func=cmd_send_file)

    # Files queued with `--delay` wait here until a serving node
    # sends them: list them, or drop one before it goes out.
    scheduled_files = subparsers.add_parser(
        "scheduled-files",
        parents=[common],
        help="list files scheduled to send later",
    )
    scheduled_files_sub = scheduled_files.add_subparsers(
        dest="scheduled_files_command", metavar="SCHEDULED_FILES_COMMAND"
    )
    scheduled_files_cancel = scheduled_files_sub.add_parser(
        "cancel",
        parents=[common],
        help="drop a scheduled file before it sends",
    )
    scheduled_files_cancel.add_argument(
        "target",
        metavar="ID",
        help="scheduled file id or unique prefix (see `tau scheduled-files`)",
    )
    scheduled_files_cancel.set_defaults(func=cmd_cancel_scheduled_file)
    scheduled_files.set_defaults(func=cmd_scheduled_files)

    # A note is a message to self that never leaves the machine, so it
    # gets its own subcommand instead of overloading `send --to <self>`
    # (which would queue forever: the send path has no self case).
    note = subparsers.add_parser(
        "note",
        parents=[common],
        help="save a private note to self in the local inbox",
    )
    note.add_argument(
        "message", nargs="?", help="note text (reads stdin when omitted)"
    )
    note.add_argument("--file", type=Path, help="read the note from a file")
    note.set_defaults(func=cmd_note)

    # A draft is an unsent message parked for one peer, group, or
    # channel: saved locally with `draft save`, sent later with
    # `draft send`.  Like a Tan draft, it never leaves the machine
    # until it is sent.
    draft = subparsers.add_parser(
        "draft",
        parents=[common],
        help="save an unsent DM, group, or channel message to send later",
    )
    draft_sub = draft.add_subparsers(dest="draft_command", metavar="DRAFT_COMMAND")

    draft_save = draft_sub.add_parser(
        "save",
        parents=[common],
        help="save a message as a draft for a peer, a group, or a channel",
    )
    draft_save_target = draft_save.add_mutually_exclusive_group(required=True)
    draft_save_target.add_argument(
        "--to",
        metavar="NAME|HASH",
        help="peer name or destination hash",
    )
    draft_save_target.add_argument(
        "--group",
        metavar="NAME|ID",
        help="group name or id: draft for the group instead of a peer",
    )
    draft_save_target.add_argument(
        "--channel",
        metavar="NAME",
        help="channel name: draft for the channel instead of a peer or group",
    )
    draft_save.add_argument(
        "message", nargs="?", help="draft text (reads stdin when omitted)"
    )
    draft_save.set_defaults(func=cmd_draft_save)

    draft_list = draft_sub.add_parser(
        "list",
        parents=[common],
        help="list saved message drafts",
    )
    draft_list.set_defaults(func=cmd_draft_list)

    draft_show = draft_sub.add_parser(
        "show",
        parents=[common],
        help="print a saved draft's full text",
    )
    draft_show_target = draft_show.add_mutually_exclusive_group(required=True)
    draft_show_target.add_argument(
        "--to",
        metavar="NAME|HASH",
        help="peer name or destination hash",
    )
    draft_show_target.add_argument(
        "--group",
        metavar="NAME|ID",
        help="group name or id",
    )
    draft_show_target.add_argument(
        "--channel",
        metavar="NAME",
        help="channel name",
    )
    draft_show.set_defaults(func=cmd_draft_show)

    draft_delete = draft_sub.add_parser(
        "delete",
        parents=[common],
        help="discard a saved draft",
    )
    draft_delete_target = draft_delete.add_mutually_exclusive_group(required=True)
    draft_delete_target.add_argument(
        "--to",
        metavar="NAME|HASH",
        help="peer name or destination hash",
    )
    draft_delete_target.add_argument(
        "--group",
        metavar="NAME|ID",
        help="group name or id",
    )
    draft_delete_target.add_argument(
        "--channel",
        metavar="NAME",
        help="channel name",
    )
    draft_delete.set_defaults(func=cmd_draft_delete)

    draft_send = draft_sub.add_parser(
        "send",
        parents=[common],
        help="send a saved draft (and delete it once queued)",
    )
    draft_send_target = draft_send.add_mutually_exclusive_group(required=True)
    draft_send_target.add_argument(
        "--to",
        metavar="NAME|HASH",
        help="peer name or destination hash",
    )
    draft_send_target.add_argument(
        "--group",
        metavar="NAME|ID",
        help="group name or id: send the draft into a group",
    )
    draft_send_target.add_argument(
        "--channel",
        metavar="NAME",
        help="channel name: send the draft into the channel",
    )
    draft_send.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    draft_send.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    draft_send.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the message when the peer is offline",
    )
    draft_send.add_argument(
        "--wait",
        type=float,
        default=None,
        metavar="SECONDS",
        help="after queueing, wait up to SECONDS for the message to be delivered",
    )
    draft_send.set_defaults(func=cmd_draft_send)

    remind = subparsers.add_parser(
        "remind",
        parents=[common],
        help="snooze a stored DM or group message and be re-pinged later",
    )
    remind_sub = remind.add_subparsers(dest="remind_command", metavar="REMIND_COMMAND")

    remind_add = remind_sub.add_parser(
        "add",
        parents=[common],
        help="remind yourself about a stored DM or group message later",
    )
    remind_add.add_argument(
        "message",
        metavar="MSG",
        help="message id or unique prefix (as shown by `tau messages`)",
    )
    remind_add_time = remind_add.add_mutually_exclusive_group(required=True)
    remind_add_time.add_argument(
        "--in",
        dest="remind_in",
        metavar="DURATION",
        help="remind after this long (e.g. 30s, 5m, 2h, 7d; at most 30d)",
    )
    remind_add_time.add_argument(
        "--at",
        dest="remind_at",
        metavar="ISO-TIME",
        help="remind at this time (ISO 8601, e.g. 2026-11-21T09:00; naive times are local)",
    )
    remind_add.set_defaults(func=cmd_remind_add)

    remind_list = remind_sub.add_parser(
        "list",
        parents=[common],
        help="list pending message reminders, soonest first",
    )
    remind_list.set_defaults(func=cmd_remind_list)

    remind_cancel = remind_sub.add_parser(
        "cancel",
        parents=[common],
        help="drop a pending reminder before it fires",
    )
    remind_cancel.add_argument(
        "reminder",
        metavar="ID",
        help="reminder id or unique prefix (as shown by `tau remind list`)",
    )
    remind_cancel.set_defaults(func=cmd_remind_cancel)

    react = subparsers.add_parser(
        "react",
        parents=[common],
        help="react to a received message",
    )
    react.add_argument(
        "message",
        metavar="MESSAGE-ID",
        help="message id or unique prefix (see `tau messages`)",
    )
    react.add_argument(
        "reaction",
        nargs="?",
        help="emoji or short text token, e.g. 👍 or +1 (omit with --clear)",
    )
    react.add_argument(
        "--clear",
        action="store_true",
        help="retract your reaction to the message",
    )
    react.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    react.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    react.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing when the peer is offline",
    )
    react.set_defaults(func=cmd_react)

    # `tau typing` sends a live "composing" hint: a start when the
    # user begins typing, a stop when they send, blur, or go idle.
    # The console drives it from the composer; the CLI is the same
    # event for scripting and tests.
    typing = subparsers.add_parser(
        "typing",
        parents=[common],
        help="send a typing event to a peer or a group",
    )
    typing.add_argument(
        "state",
        choices=["start", "stop"],
        help="'start' when composing begins, 'stop' when it ends",
    )
    typing_target = typing.add_mutually_exclusive_group(required=True)
    typing_target.add_argument(
        "--to",
        metavar="NAME|HASH",
        help="peer name or destination hash",
    )
    typing_target.add_argument(
        "--group",
        metavar="NAME|ID",
        help="group name or id: type in the group instead of to a peer",
    )
    typing.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    typing.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    typing.set_defaults(func=cmd_typing)

    pin = subparsers.add_parser(
        "pin",
        parents=[common],
        help="pin a stored message so it stays easy to find",
    )
    pin.add_argument(
        "message",
        metavar="MESSAGE-ID",
        help="message id or unique prefix (see `tau messages`)",
    )
    pin.set_defaults(func=cmd_pin)

    unpin = subparsers.add_parser(
        "unpin",
        parents=[common],
        help="remove the pin from a stored message",
    )
    unpin.add_argument(
        "message",
        metavar="MESSAGE-ID",
        help="message id or unique prefix (see `tau messages`)",
    )
    unpin.set_defaults(func=cmd_unpin)

    pinned = subparsers.add_parser(
        "pinned",
        parents=[common],
        help="list pinned stored messages",
    )
    pinned.add_argument(
        "--peer",
        default=None,
        metavar="NAME|HASH",
        help="only show pins from this peer (name, hash or hash prefix)",
    )
    pinned.add_argument(
        "--limit",
        type=int,
        default=30,
        metavar="N",
        help="show the newest N pinned messages (default 30)",
    )
    pinned.set_defaults(func=cmd_pinned)
    retract = subparsers.add_parser(
        "retract",
        parents=[common],
        help="retract a sent message from the recipient's inbox "
        "(a group creator may also retract a member's message)",
    )
    retract.add_argument(
        "message",
        metavar="MESSAGE-ID",
        help="message id or unique prefix (see `tau messages --sent`)",
    )
    retract.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    retract.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    retract.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the retract when the peer is offline",
    )
    retract.set_defaults(func=cmd_retract)

    vote = subparsers.add_parser(
        "vote",
        parents=[common],
        help="vote on a DM poll (option number or exact option text)",
    )
    vote.add_argument(
        "--to",
        required=True,
        metavar="NAME|HASH",
        help="peer name or destination hash",
    )
    vote.add_argument(
        "poll",
        metavar="POLL",
        help="poll message id or unique prefix (see `tau messages`)",
    )
    vote.add_argument(
        "option",
        metavar="OPTION",
        help="option number (1-based) or the exact option text",
    )
    vote.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    vote.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the vote when the peer is offline",
    )
    vote.set_defaults(func=cmd_vote)

    close_poll = subparsers.add_parser(
        "close-poll",
        parents=[common],
        help="close a DM poll you created (freezes the tally)",
    )
    close_poll.add_argument(
        "--to",
        required=True,
        metavar="NAME|HASH",
        help="peer name or destination hash",
    )
    close_poll.add_argument(
        "poll",
        metavar="POLL",
        help="poll message id or unique prefix (see `tau messages`)",
    )
    close_poll.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    close_poll.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the close when the peer is offline",
    )
    close_poll.set_defaults(func=cmd_close_poll)

    edit = subparsers.add_parser(
        "edit",
        parents=[common],
        help="edit a message you sent",
    )
    edit.add_argument(
        "target",
        metavar="MESSAGE-ID",
        help="message id or unique prefix (see `tau messages --sent`)",
    )
    edit.add_argument(
        "text", nargs="?", help="replacement text (reads stdin when omitted)"
    )
    edit.add_argument("--file", type=Path, help="read the replacement text from a file")
    edit.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    edit.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    edit.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing when the peer is offline",
    )
    edit.set_defaults(func=cmd_edit)

    ping = subparsers.add_parser(
        "ping", parents=[common], help="measure reachability of a peer"
    )
    ping.add_argument("--to", required=True, metavar="NAME|HASH")
    ping.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    ping.add_argument("--discover-timeout", type=float, default=10.0, metavar="SECONDS")
    ping.set_defaults(func=cmd_ping)

    trace = subparsers.add_parser(
        "trace",
        parents=[common],
        help="trace the mesh path to a peer (hops + live probe)",
    )
    trace.add_argument("peer", metavar="NAME|HASH")
    trace.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    trace.add_argument("--discover-timeout", type=float, default=10.0, metavar="SECONDS")
    trace.set_defaults(func=cmd_trace)

    mesh = subparsers.add_parser(
        "mesh",
        parents=[common],
        help="list known peers with live reachability and hop counts",
    )
    mesh.set_defaults(func=cmd_mesh)

    info = subparsers.add_parser(
        "info", parents=[common], help="show a remote peer's information"
    )
    info.add_argument("--to", required=True, metavar="NAME|HASH")
    info.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    info.add_argument("--discover-timeout", type=float, default=10.0, metavar="SECONDS")
    info.set_defaults(func=cmd_info)

    doctor = subparsers.add_parser(
        "doctor",
        parents=[common],
        help="check the local data directory's health (read-only)",
    )
    doctor.set_defaults(func=cmd_doctor)

    peers = subparsers.add_parser(
        "peers", parents=[common], help="list peers from the local registry"
    )
    peers.add_argument(
        "--max-age",
        type=float,
        default=None,
        metavar="DAYS",
        help="only show peers heard within this many days",
    )
    peers.add_argument(
        "--exchange",
        action="store_true",
        help="ask known peers for the peers they know and merge the answers",
    )
    peers.add_argument(
        "--to",
        default=None,
        metavar="NAME|HASH",
        help="with --exchange, ask only this peer instead of all known peers",
    )
    peers.add_argument(
        "--safety-number",
        action="store_true",
        help="show each peer's pairwise safety number (compare it with the "
        "peer over another channel to confirm the key is theirs)",
    )
    peers.set_defaults(func=cmd_peers)
    peers_sub = peers.add_subparsers(
        dest="peers_command", metavar="PEERS_COMMAND"
    )

    peers_show = peers_sub.add_parser(
        "show",
        parents=[common],
        help="show what this node knows about one peer (offline, read-only)",
    )
    peers_show.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer name, destination hash or hash prefix",
    )
    peers_show.set_defaults(func=cmd_peers_show)

    peers_alias = peers_sub.add_parser(
        "alias",
        parents=[common],
        help="give a known peer a local alias",
    )
    peers_alias.add_argument(
        "peer",
        metavar="PEER",
        help="peer name, destination hash, or hash prefix",
    )
    peers_alias.add_argument(
        "alias", metavar="ALIAS", help="local label for the peer"
    )
    peers_alias.set_defaults(func=cmd_peers_alias)

    peers_unalias = peers_sub.add_parser(
        "unalias",
        parents=[common],
        help="remove a peer's local alias",
    )
    peers_unalias.add_argument(
        "peer",
        metavar="PEER",
        help="peer name, alias, destination hash, or hash prefix",
    )
    peers_unalias.set_defaults(func=cmd_peers_unalias)

    peers_note = peers_sub.add_parser(
        "note",
        parents=[common],
        help="save, show, or clear a private note on a peer",
    )
    peers_note.add_argument(
        "peer",
        metavar="PEER",
        help="peer name, alias, destination hash, or hash prefix",
    )
    peers_note.add_argument(
        "text",
        metavar="TEXT",
        nargs="?",
        default=None,
        help="private note to save (omit to show the current note)",
    )
    peers_note.add_argument(
        "--clear",
        action="store_true",
        help="remove the peer's note",
    )
    peers_note.set_defaults(func=cmd_peers_note)
    peers_verify = peers_sub.add_parser(
        "verify",
        parents=[common],
        help="record that the safety number was compared with a peer "
        "out of band and matched",
    )
    peers_verify.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer to mark verified",
    )
    peers_verify.set_defaults(func=cmd_peers_verify)
    peers_forget = peers_sub.add_parser(
        "forget",
        parents=[common],
        help="remove a peer from the local registry (the next genuine "
        "announce re-adds it; use this to drop a corrupt record)",
    )
    peers_forget.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer to remove",
    )
    peers_forget.add_argument(
        "--force",
        action="store_true",
        help="forget even while messages are still queued for the peer "
        "(they will not be attempted until the peer is rediscovered)",
    )
    peers_forget.set_defaults(func=cmd_peers_forget)

    peers_profile = peers_sub.add_parser(
        "profile",
        parents=[common],
        help="show a peer's signed public profile, fetching it live "
        "when this node does not hold a copy",
    )
    peers_profile.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer name, destination hash or hash prefix",
    )
    peers_profile.add_argument(
        "--cached",
        action="store_true",
        help="show the stored copy without fetching from the peer",
    )
    peers_profile.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        metavar="SECONDS",
        help="how long to wait for the peer to answer (default: %(default)s)",
    )
    peers_profile.set_defaults(func=cmd_peers_profile)

    profile = subparsers.add_parser(
        "profile",
        parents=[common],
        help="publish and show this node's signed public profile",
    )
    profile_sub = profile.add_subparsers(
        dest="profile_command", metavar="PROFILE_COMMAND"
    )
    profile_sub.required = True

    profile_set = profile_sub.add_parser(
        "set",
        parents=[common],
        help="publish a new version of your public profile "
        "(display name, bio, avatar, links)",
    )
    profile_set.add_argument(
        "--display-name",
        default=None,
        metavar="NAME",
        help="display name (default: the current profile name, or the "
        "node name when no profile is published yet)",
    )
    profile_set.add_argument(
        "--bio",
        default=None,
        metavar="TEXT",
        help=f"short bio, at most {MAX_BIO_CHARS} characters",
    )
    profile_set.add_argument(
        "--avatar",
        default=None,
        metavar="FILE|HASH",
        help="avatar image file to hash, or a 32/64-character hex hash",
    )
    profile_set.add_argument(
        "--link",
        dest="links",
        action="append",
        default=None,
        metavar="URL",
        help=f"profile link (repeatable, at most {MAX_LINKS})",
    )
    profile_set.set_defaults(func=cmd_profile_set)

    profile_show = profile_sub.add_parser(
        "show",
        parents=[common],
        help="show your published public profile",
    )
    profile_show.set_defaults(func=cmd_profile_show)

    page = subparsers.add_parser(
        "page",
        parents=[common],
        help="publish and fetch signed personal mesh pages",
    )
    page_sub = page.add_subparsers(dest="page_command", metavar="PAGE_COMMAND")
    page_sub.required = True

    page_publish = page_sub.add_parser(
        "publish",
        parents=[common],
        help="publish a Markdown file as this node's signed personal page",
    )
    page_publish.add_argument(
        "file",
        metavar="FILE.md",
        help="Markdown file to publish (at most 8 KiB)",
    )
    page_publish.add_argument(
        "--title",
        default=None,
        metavar="TEXT",
        help="page title (default: the file's first '# ' heading)",
    )
    page_publish.add_argument(
        "--file",
        dest="attach",
        action="append",
        default=None,
        metavar="PATH",
        help="attach a small file to the page (at most 5, 32 KiB each; "
        "repeatable; the new version's file list is exactly these files)",
    )
    page_publish.set_defaults(func=cmd_page_publish)

    page_show = page_sub.add_parser(
        "show",
        parents=[common],
        help="show your published personal page",
    )
    page_show.set_defaults(func=cmd_page_show)

    page_fetch = page_sub.add_parser(
        "fetch",
        parents=[common],
        help="show a peer's signed personal page, fetching it live "
        "when this node does not hold a copy",
    )
    page_fetch.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer name, destination hash or hash prefix",
    )
    page_fetch.add_argument(
        "--cached",
        action="store_true",
        help="show the stored copy without fetching from the peer",
    )
    page_fetch.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        metavar="SECONDS",
        help="how long to wait for the peer to answer (default: %(default)s)",
    )
    page_fetch.set_defaults(func=cmd_page_fetch)

    page_file = page_sub.add_parser(
        "file",
        parents=[common],
        help="save one of a peer's linked page files to disk",
    )
    page_file.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer name, destination hash or hash prefix",
    )
    page_file.add_argument(
        "selector",
        metavar="NAME|SHA256",
        help="linked file name, or a unique SHA-256 prefix",
    )
    page_file.add_argument(
        "--to",
        default=None,
        metavar="PATH",
        help="where to write the file (default: the file's name here)",
    )
    page_file.add_argument(
        "--force",
        action="store_true",
        help="overwrite the destination file when it already exists",
    )
    page_file.add_argument(
        "--cached",
        action="store_true",
        help="use the stored page and files without fetching from the peer",
    )
    page_file.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        metavar="SECONDS",
        help="how long to wait for the peer to answer (default: %(default)s)",
    )
    page_file.set_defaults(func=cmd_page_file)

    page_allow = page_sub.add_parser(
        "allow",
        parents=[common],
        help="control who may fetch this node's page (public unless enforced)",
    )
    page_allow_sub = page_allow.add_subparsers(
        dest="page_allow_command", metavar="PAGE_ALLOW_COMMAND"
    )
    page_allow_sub.required = True

    page_allow_list = page_allow_sub.add_parser(
        "list", parents=[common], help="show the page allow-list"
    )
    page_allow_list.set_defaults(func=cmd_page_allow)

    page_allow_add = page_allow_sub.add_parser(
        "add",
        parents=[common],
        help="allow fetching this node's page from a peer (name, hash or prefix)",
    )
    page_allow_add.add_argument(
        "peer", help="peer name, destination hash or hash prefix"
    )
    page_allow_add.set_defaults(func=cmd_page_allow)

    page_allow_remove = page_allow_sub.add_parser(
        "remove",
        parents=[common],
        help="stop allowing a peer to fetch this node's page",
    )
    page_allow_remove.add_argument(
        "peer", help="peer name, destination hash or hash prefix"
    )
    page_allow_remove.set_defaults(func=cmd_page_allow)

    page_allow_on = page_allow_sub.add_parser(
        "on", parents=[common], help="enforce the page allow-list"
    )
    page_allow_on.set_defaults(func=cmd_page_allow)

    page_allow_off = page_allow_sub.add_parser(
        "off", parents=[common], help="stop enforcing the page allow-list"
    )
    page_allow_off.set_defaults(func=cmd_page_allow)

    status = subparsers.add_parser(
        "status",
        parents=[common],
        help="show a read-only summary of this node's state",
    )
    status.set_defaults(func=cmd_status)

    identity = subparsers.add_parser(
        "id",
        parents=[common],
        help="show this node's name and destination, or back up / restore its identity",
    )
    identity.set_defaults(func=cmd_id)
    id_sub = identity.add_subparsers(dest="id_command", metavar="ID_COMMAND")
    id_export = id_sub.add_parser(
        "export",
        parents=[common],
        help="write a backup copy of the node identity to a file",
    )
    id_export.add_argument(
        "--to",
        required=True,
        type=Path,
        metavar="PATH",
        help="backup file to write",
    )
    id_export.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing backup file",
    )
    id_export.set_defaults(func=cmd_id_export)
    id_import = id_sub.add_parser(
        "import",
        parents=[common],
        help="restore the node identity from a backup file",
    )
    id_import.add_argument(
        "--from",
        required=True,
        type=Path,
        dest="from_path",
        metavar="PATH",
        help="backup file to read",
    )
    id_import.add_argument(
        "--force",
        action="store_true",
        help="replace an existing identity",
    )
    id_import.set_defaults(func=cmd_id_import)
    id_rename = id_sub.add_parser(
        "rename",
        parents=[common],
        help="change this node's display name",
    )
    id_rename.add_argument(
        "new_name",
        metavar="NEWNAME",
        help="new node name",
    )
    id_rename.set_defaults(func=cmd_id_rename)

    devices = subparsers.add_parser(
        "devices",
        parents=[common],
        help="pair a second device to this identity, or list paired devices",
    )
    devices.set_defaults(func=cmd_devices_list)
    devices_sub = devices.add_subparsers(dest="devices_command", metavar="DEVICES_COMMAND")
    devices_pair = devices_sub.add_parser(
        "pair",
        parents=[common],
        help="mint a signed pairing bundle for a second device",
    )
    devices_pair.add_argument(
        "--to",
        required=True,
        type=Path,
        metavar="PATH",
        help="pairing bundle file to write",
    )
    devices_pair.add_argument(
        "--ttl",
        type=float,
        default=DEFAULT_PAIRING_TTL_SECONDS,
        metavar="SECONDS",
        help=f"how long the bundle stays redeemable (default {DEFAULT_PAIRING_TTL_SECONDS})",
    )
    devices_pair.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing bundle file",
    )
    devices_pair.set_defaults(func=cmd_devices_pair)
    devices_redeem = devices_sub.add_parser(
        "redeem",
        parents=[common],
        help="adopt the identity in a pairing bundle from another device",
    )
    devices_redeem.add_argument(
        "--from",
        required=True,
        type=Path,
        dest="from_path",
        metavar="PATH",
        help="pairing bundle file to read",
    )
    devices_redeem.add_argument(
        "--force",
        action="store_true",
        help="replace an existing identity",
    )
    devices_redeem.set_defaults(func=cmd_devices_redeem)
    devices_list = devices_sub.add_parser(
        "list",
        parents=[common],
        help="show this device and the pairing offers it issued",
    )
    devices_list.set_defaults(func=cmd_devices_list)

    backup = subparsers.add_parser(
        "backup",
        parents=[common],
        help="back up the whole data directory to one archive file",
    )
    backup.add_argument(
        "file",
        type=Path,
        metavar="FILE",
        help="backup archive to write",
    )
    backup.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing backup archive",
    )
    backup.set_defaults(func=cmd_backup)

    restore = subparsers.add_parser(
        "restore",
        parents=[common],
        help="restore a backup archive into the data directory",
    )
    restore.add_argument(
        "file",
        type=Path,
        metavar="FILE",
        help="backup archive to read",
    )
    restore.add_argument(
        "--force",
        action="store_true",
        help="replace existing node state in the data directory",
    )
    restore.set_defaults(func=cmd_restore)

    outbox = subparsers.add_parser(
        "outbox",
        parents=[common],
        help="list messages queued for offline peers (store-and-forward)",
    )
    outbox.set_defaults(func=cmd_outbox)
    outbox_sub = outbox.add_subparsers(dest="outbox_command", metavar="OUTBOX_COMMAND")

    outbox_list = outbox_sub.add_parser(
        "list",
        parents=[common],
        help="list messages queued for offline peers",
    )
    outbox_list.set_defaults(func=cmd_outbox)

    outbox_held = outbox_sub.add_parser(
        "held",
        parents=[common],
        help="list sealed messages held for other peers",
    )
    outbox_held.set_defaults(func=cmd_outbox_held)

    outbox_drop_held = outbox_sub.add_parser(
        "drop-held",
        parents=[common],
        help="drop a held message so it is never delivered",
    )
    outbox_drop_held.add_argument(
        "target", metavar="ID", help="message id or unique id prefix"
    )
    outbox_drop_held.set_defaults(func=cmd_outbox_drop_held)
    outbox_cancel = outbox_sub.add_parser(
        "cancel",
        parents=[common],
        help="cancel a queued message so it is never delivered",
    )
    outbox_cancel.add_argument(
        "target", metavar="ID", help="message id or unique id prefix"
    )
    outbox_cancel.set_defaults(func=cmd_outbox_cancel)

    outbox_edit = outbox_sub.add_parser(
        "edit",
        parents=[common],
        help="edit a scheduled message before it sends",
    )
    outbox_edit.add_argument(
        "target", metavar="ID", help="message id or unique id prefix"
    )
    outbox_edit.add_argument(
        "--text",
        metavar="TEXT",
        default=None,
        help="replacement text for the scheduled message",
    )
    outbox_edit.add_argument(
        "--delay",
        metavar="DURATION",
        default=None,
        help="move the send time to this long from now (e.g. 30s, 5m, 2h, "
        "7d; at most 29d23h — the 30d mail TTL less an hour of delivery slack)",
    )
    outbox_edit.set_defaults(func=cmd_outbox_edit)

    outbox_retry = outbox_sub.add_parser(
        "retry",
        parents=[common],
        help="try delivering queued messages now, ignoring backoff",
    )
    outbox_retry.add_argument(
        "target",
        metavar="ID",
        nargs="?",
        default=None,
        help="message id or unique id prefix (default: retry everything queued)",
    )
    outbox_retry.add_argument(
        "--timeout",
        type=float,
        default=6.0,
        help="seconds to wait per peer contact (default: 6.0)",
    )
    outbox_retry.set_defaults(func=cmd_outbox_retry)

    allowlist = subparsers.add_parser(
        "allowlist",
        parents=[common],
        help="restrict who may send this node direct messages",
    )
    allowlist_sub = allowlist.add_subparsers(
        dest="allowlist_command", metavar="ALLOWLIST_COMMAND"
    )
    allowlist_sub.required = True

    allowlist_list = allowlist_sub.add_parser(
        "list", parents=[common], help="show the DM allow-list"
    )
    allowlist_list.set_defaults(func=cmd_allowlist)

    allowlist_add = allowlist_sub.add_parser(
        "add",
        parents=[common],
        help="allow direct messages from a peer (name, hash or prefix)",
    )
    allowlist_add.add_argument(
        "peer", help="peer name, destination hash or hash prefix"
    )
    allowlist_add.set_defaults(func=cmd_allowlist)

    allowlist_remove = allowlist_sub.add_parser(
        "remove",
        parents=[common],
        help="stop allowing direct messages from a peer",
    )
    allowlist_remove.add_argument(
        "peer", help="peer name, destination hash or hash prefix"
    )
    allowlist_remove.set_defaults(func=cmd_allowlist)

    allowlist_on = allowlist_sub.add_parser(
        "on", parents=[common], help="enforce the DM allow-list"
    )
    allowlist_on.set_defaults(func=cmd_allowlist)

    allowlist_off = allowlist_sub.add_parser(
        "off", parents=[common], help="stop enforcing the DM allow-list"
    )
    allowlist_off.set_defaults(func=cmd_allowlist)

    config_cmd = subparsers.add_parser(
        "config",
        parents=[common],
        help="show or change this node's own settings",
    )
    config_sub = config_cmd.add_subparsers(
        dest="config_command", metavar="CONFIG_COMMAND"
    )
    config_files = config_sub.add_parser(
        "accept-files",
        parents=[common],
        help="turn receiving files on or off (default off)",
    )
    config_files.add_argument(
        "state",
        choices=["on", "off"],
        help="whether this node accepts file transfers",
    )
    config_files.set_defaults(func=cmd_config)
    config_cmd.set_defaults(func=cmd_config)

    config_receipts = config_sub.add_parser(
        "read-receipts",
        parents=[common],
        help="turn sending read receipts on or off (default on)",
    )
    config_receipts.add_argument(
        "state",
        choices=["on", "off"],
        help="whether this node sends read receipts when it reads a DM",
    )
    config_receipts.set_defaults(func=cmd_config)
    config_cmd.set_defaults(func=cmd_config)

    config_depth = config_sub.add_parser(
        "tan-sync-depth",
        parents=[common],
        help="how many hops of the follow graph Tan sync reaches (1 or 2, default 2)",
    )
    config_depth.add_argument(
        "depth",
        nargs="?",
        choices=["1", "2"],
        help="1 = followed authors only, 2 = plus the authors they follow",
    )
    config_depth.set_defaults(func=cmd_config)
    config_cmd.set_defaults(func=cmd_config)

    config_announce = config_sub.add_parser(
        "announce-interval",
        parents=[common],
        help="how often this node re-announces itself (seconds, 0 disables, default 300)",
    )
    config_announce.add_argument(
        "seconds",
        nargs="?",
        type=_announce_interval_value,
        help="re-announce every SECONDS seconds; 0 sends no automatic announces",
    )
    config_announce.set_defaults(func=cmd_config)
    config_cmd.set_defaults(func=cmd_config)

    block = subparsers.add_parser(
        "block",
        parents=[common],
        help="silence a peer: drop their DMs and hide their Tan posts",
    )
    block.add_argument(
        "target",
        nargs="?",
        help="peer to block (name, hash or prefix); 'list' shows the list, "
        "'add'/'remove' take the peer as a second word",
    )
    block.add_argument(
        "peer",
        nargs="?",
        help="peer for 'add'/'remove'",
    )
    block.set_defaults(func=cmd_block)

    unblock = subparsers.add_parser(
        "unblock",
        parents=[common],
        help="unblock a peer (name, hash or prefix)",
    )
    unblock.add_argument("peer", help="peer name, destination hash or hash prefix")
    unblock.set_defaults(func=cmd_block, block_command="remove")

    archive = subparsers.add_parser(
        "archive",
        parents=[common],
        help="hide a peer's conversation from `tau messages` (history kept)",
    )
    archive.add_argument("peer", help="peer name, destination hash or hash prefix")
    archive.set_defaults(func=cmd_archive)

    unarchive = subparsers.add_parser(
        "unarchive",
        parents=[common],
        help="restore an archived conversation to `tau messages`",
    )
    unarchive.add_argument("peer", help="peer name, destination hash or hash prefix")
    unarchive.set_defaults(func=cmd_unarchive)

    pin_conversation = subparsers.add_parser(
        "pin-conversation",
        parents=[common],
        help="pin a conversation so it stays at the top of the conversation list",
    )
    pin_target = pin_conversation.add_mutually_exclusive_group(required=True)
    pin_target.add_argument(
        "--peer",
        metavar="PEER",
        help="peer name, destination hash or hash prefix",
    )
    pin_target.add_argument(
        "--group",
        metavar="GROUP",
        help="group id, id prefix or name",
    )
    pin_target.add_argument(
        "--channel",
        metavar="CHANNEL",
        help="channel name (must be joined)",
    )
    pin_conversation.set_defaults(func=cmd_pin_conversation)

    unpin_conversation = subparsers.add_parser(
        "unpin-conversation",
        parents=[common],
        help="remove the pin from a conversation",
    )
    unpin_target = unpin_conversation.add_mutually_exclusive_group(required=True)
    unpin_target.add_argument(
        "--peer",
        metavar="PEER",
        help="peer name, destination hash or hash prefix",
    )
    unpin_target.add_argument(
        "--group",
        metavar="GROUP",
        help="group id, id prefix or name",
    )
    unpin_target.add_argument(
        "--channel",
        metavar="CHANNEL",
        help="channel name",
    )
    unpin_conversation.set_defaults(func=cmd_unpin_conversation)

    pinned_conversations = subparsers.add_parser(
        "pinned-conversations",
        parents=[common],
        help="list pinned conversations, newest pin first",
    )
    pinned_conversations.set_defaults(func=cmd_pinned_conversations)

    save_message = subparsers.add_parser(
        "save",
        parents=[common],
        help="star a stored message so it stays in `tau saved`",
    )
    save_target = save_message.add_mutually_exclusive_group(required=True)
    save_target.add_argument(
        "--peer",
        metavar="PEER",
        help="peer name, destination hash or hash prefix",
    )
    save_target.add_argument(
        "--group",
        metavar="GROUP",
        help="group id, id prefix or name",
    )
    save_target.add_argument(
        "--channel",
        metavar="CHANNEL",
        help="channel name",
    )
    save_message.add_argument(
        "message",
        metavar="MESSAGE",
        help="message id or unique prefix",
    )
    save_message.set_defaults(func=cmd_save_message)

    unsave_message = subparsers.add_parser(
        "unsave",
        parents=[common],
        help="remove the star from a stored message",
    )
    unsave_target = unsave_message.add_mutually_exclusive_group(required=True)
    unsave_target.add_argument(
        "--peer",
        metavar="PEER",
        help="peer name, destination hash or hash prefix",
    )
    unsave_target.add_argument(
        "--group",
        metavar="GROUP",
        help="group id, id prefix or name",
    )
    unsave_target.add_argument(
        "--channel",
        metavar="CHANNEL",
        help="channel name",
    )
    unsave_message.add_argument(
        "message",
        metavar="MESSAGE",
        help="message id or unique prefix",
    )
    unsave_message.set_defaults(func=cmd_unsave_message)

    saved_messages = subparsers.add_parser(
        "saved",
        parents=[common],
        help="list starred messages, newest save first",
    )
    saved_messages.add_argument(
        "--limit",
        type=int,
        default=30,
        metavar="N",
        help="maximum starred messages to show (default 30)",
    )
    saved_messages.set_defaults(func=cmd_saved_messages)

    disappearing = subparsers.add_parser(
        "disappearing",
        parents=[common],
        help="set, show or clear a per-conversation disappearing-message default",
    )
    disappearing_target = disappearing.add_mutually_exclusive_group(required=True)
    disappearing_target.add_argument(
        "--peer",
        metavar="PEER",
        help="peer name, destination hash or hash prefix",
    )
    disappearing_target.add_argument(
        "--group",
        metavar="GROUP",
        help="group id, id prefix or name",
    )
    disappearing_target.add_argument(
        "--channel",
        metavar="NAME",
        help="channel name",
    )
    disappearing.add_argument(
        "duration",
        nargs="?",
        metavar="DURATION",
        help="default lifetime from 1s to 7d, like 30s, 5m, 2h or 7d (omit to show the current setting)",
    )
    disappearing.add_argument(
        "--off",
        action="store_true",
        help="clear the default for this conversation",
    )
    disappearing.set_defaults(func=cmd_disappearing)

    mute = subparsers.add_parser(
        "mute",
        parents=[common],
        help="mute or unmute a conversation so it never lights the unread badge",
    )
    mute_target = mute.add_mutually_exclusive_group(required=False)
    mute_target.add_argument(
        "--peer",
        metavar="PEER",
        help="peer name, destination hash or hash prefix",
    )
    mute_target.add_argument(
        "--group",
        metavar="GROUP",
        help="group id, id prefix or name",
    )
    mute_target.add_argument(
        "--channel",
        metavar="CHANNEL",
        help="channel name (a joined channel)",
    )
    mute.add_argument(
        "--off",
        action="store_true",
        help="unmute this conversation instead",
    )
    mute.add_argument(
        "--for",
        dest="for_duration",
        default=None,
        metavar="DURATION",
        help="mute only for this long, e.g. 30s, 5m, 2h or 7d (at most 30d); "
        "afterwards the conversation counts as unread again",
    )
    mute.set_defaults(func=cmd_mute)

    quiet_hours = subparsers.add_parser(
        "quiet-hours",
        parents=[common],
        help="silence the unread badge every day during a set window",
    )
    quiet_hours_sub = quiet_hours.add_subparsers(
        dest="quiet_hours_command", metavar="QUIET_HOURS_COMMAND"
    )
    quiet_hours_set = quiet_hours_sub.add_parser(
        "set",
        parents=[common],
        help="set the daily quiet-hours window",
    )
    quiet_hours_set.add_argument(
        "--start",
        required=True,
        metavar="HH:MM",
        help="when quiet hours start each day, e.g. 22:00",
    )
    quiet_hours_set.add_argument(
        "--end",
        required=True,
        metavar="HH:MM",
        help="when quiet hours end each day, e.g. 07:00 "
        "(earlier than the start means overnight)",
    )
    quiet_hours_set.set_defaults(func=cmd_quiet_hours_set)

    quiet_hours_off = quiet_hours_sub.add_parser(
        "off",
        parents=[common],
        help="turn quiet hours off, keeping the window",
    )
    quiet_hours_off.set_defaults(func=cmd_quiet_hours_off)

    quiet_hours_status = quiet_hours_sub.add_parser(
        "status",
        parents=[common],
        help="show the quiet-hours window and whether it is quiet now",
    )
    quiet_hours_status.set_defaults(func=cmd_quiet_hours_status)

    # No subcommand shows the status, like `tau mute` with no target lists.
    quiet_hours.set_defaults(func=cmd_quiet_hours_status)

    away = subparsers.add_parser(
        "away",
        parents=[common],
        help="set an away message that auto-replies to DMs while you are away",
    )
    away_sub = away.add_subparsers(dest="away_command", metavar="AWAY_COMMAND")
    away_sub.required = True

    away_on = away_sub.add_parser(
        "on",
        parents=[common],
        help="turn the away reply on, optionally with a custom message",
    )
    away_on.add_argument(
        "--message",
        metavar="TEXT",
        help="custom away message (default: \"I'm away right now. "
        "I'll reply when I'm back.\")",
    )
    away_on.set_defaults(func=cmd_away_on)

    away_off = away_sub.add_parser(
        "off",
        parents=[common],
        help="turn the away reply off",
    )
    away_off.set_defaults(func=cmd_away_off)

    away_status = away_sub.add_parser(
        "status",
        parents=[common],
        help="show whether the away reply is on and what it says",
    )
    away_status.set_defaults(func=cmd_away_status)

    beacon = subparsers.add_parser(
        "beacon",
        parents=[common],
        help="share live location with a conversation until it expires",
    )
    beacon_sub = beacon.add_subparsers(dest="beacon_command", metavar="BEACON_COMMAND")
    beacon_sub.required = True

    beacon_start = beacon_sub.add_parser(
        "start",
        parents=[common],
        help="start sharing live location with a peer or group",
    )
    beacon_start_target = beacon_start.add_mutually_exclusive_group(required=True)
    beacon_start_target.add_argument(
        "peer",
        nargs="?",
        metavar="PEER",
        help="peer name, destination hash or hash prefix",
    )
    beacon_start_target.add_argument(
        "--group",
        metavar="GROUP",
        help="group id, id prefix or name",
    )
    beacon_start.add_argument(
        "--location",
        required=True,
        metavar="LAT,LON [PLACE]",
        help='starting position (e.g. --location "34.05,-83.88 Rock Springs Park")',
    )
    beacon_start.add_argument(
        "--location-file",
        metavar="PATH",
        help="JSON position file re-read on every update, so an external "
        "GPS writer moves the beacon (e.g. a termux-location loop)",
    )
    beacon_start.add_argument(
        "--coarse",
        action="store_true",
        help="round every update to ~1 km and drop the place name",
    )
    beacon_start.add_argument(
        "--duration-mins",
        type=float,
        default=60.0,
        metavar="MINUTES",
        help="how long the beacon runs (default 60, at most 480)",
    )
    beacon_start.add_argument(
        "--interval-mins",
        type=float,
        default=5.0,
        metavar="MINUTES",
        help="minutes between updates (default 5, 1 to 60)",
    )
    beacon_start.set_defaults(func=cmd_beacon_start)

    beacon_stop = beacon_sub.add_parser(
        "stop",
        parents=[common],
        help="stop sharing live location with a peer or group",
    )
    beacon_stop_target = beacon_stop.add_mutually_exclusive_group(required=True)
    beacon_stop_target.add_argument(
        "peer",
        nargs="?",
        metavar="PEER",
        help="peer name, destination hash or hash prefix",
    )
    beacon_stop_target.add_argument(
        "--group",
        metavar="GROUP",
        help="group id, id prefix or name",
    )
    beacon_stop.set_defaults(func=cmd_beacon_stop)

    beacon_update = beacon_sub.add_parser(
        "update",
        parents=[common],
        help="refresh a running beacon's position or position file",
    )
    beacon_update_target = beacon_update.add_mutually_exclusive_group(required=True)
    beacon_update_target.add_argument(
        "peer",
        nargs="?",
        metavar="PEER",
        help="peer name, destination hash or hash prefix",
    )
    beacon_update_target.add_argument(
        "--group",
        metavar="GROUP",
        help="group id, id prefix or name",
    )
    beacon_update.add_argument(
        "--location",
        metavar="LAT,LON [PLACE]",
        help="new position (e.g. --location \"34.05,-83.88 Rock Springs Park\")",
    )
    beacon_update.add_argument(
        "--location-file",
        metavar="PATH",
        help="JSON position file re-read on every update from now on",
    )
    beacon_update.set_defaults(func=cmd_beacon_update)

    beacon_list = beacon_sub.add_parser(
        "list",
        parents=[common],
        help="list running and recently ended beacons",
    )
    beacon_list.set_defaults(func=cmd_beacon_list)

    archived = subparsers.add_parser(
        "archived",
        parents=[common],
        help="list archived conversations",
    )
    archived.set_defaults(func=cmd_archived)


    messages = subparsers.add_parser(
        "messages",
        parents=[common],
        help="list received direct messages kept in the node inbox",
    )
    messages.add_argument(
        "--peer",
        default=None,
        metavar="NAME|HASH",
        help="only show messages from this peer (name, hash or hash prefix)",
    )
    messages.add_argument(
        "--search",
        default=None,
        metavar="TEXT",
        help="only show messages whose text contains this (case-insensitive)",
    )
    messages.add_argument(
        "--limit",
        type=int,
        default=30,
        metavar="N",
        help="show the newest N messages (default 30)",
    )
    messages.add_argument(
        "--group",
        default=None,
        metavar="ID|NAME",
        help="only show messages from this group (id, id prefix or name)",
    )
    messages.add_argument(
        "--kind",
        default=None,
        metavar="SLUG",
        help="only show messages of this kind (default: all kinds)",
    )
    messages.add_argument(
        "--sent",
        action="store_true",
        help="show messages this node sent, with their read state, instead",
    )
    messages.add_argument(
        "--unread",
        action="store_true",
        help="only show messages not yet read",
    )
    messages.add_argument(
        "--archived",
        action="store_true",
        help="only show conversations archived with `tau archive`",
    )
    messages.add_argument(
        "--count",
        action="store_true",
        help="print per-peer unread counts instead of listing messages",
    )
    messages.add_argument(
        "--ignored",
        action="store_true",
        help="only show messages from ignored groups",
    )
    messages.set_defaults(func=cmd_messages)

    search = subparsers.add_parser(
        "search",
        parents=[common],
        help="search stored chat history by keyword, newest first",
    )
    search.add_argument("query", metavar="TEXT", help="text to search for")
    search_scope = search.add_mutually_exclusive_group()
    search_scope.add_argument(
        "--peer",
        default=None,
        metavar="NAME|HASH",
        help="only search one DM peer (name, hash or hash prefix)",
    )
    search_scope.add_argument(
        "--group",
        default=None,
        metavar="ID|NAME",
        help="only search one group (id, id prefix or name)",
    )
    search_scope.add_argument(
        "--channel",
        default=None,
        metavar="NAME",
        help="only search one channel",
    )
    search.add_argument(
        "--limit",
        type=int,
        default=20,
        metavar="N",
        help="show the newest N matches (default 20)",
    )
    search.set_defaults(func=cmd_search)

    messages_sub = messages.add_subparsers(metavar="COMMAND")
    messages_delete = messages_sub.add_parser(
        "delete",
        parents=[common],
        help="delete one stored direct message from the local inbox",
    )
    messages_delete.add_argument(
        "target", metavar="ID", help="message id or unique id prefix"
    )
    messages_delete.set_defaults(func=cmd_messages_delete)

    messages_show = messages_sub.add_parser(
        "show",
        parents=[common],
        help="show one stored message's full details",
    )
    messages_show.add_argument(
        "target", metavar="ID", help="message id or unique id prefix"
    )
    messages_show.set_defaults(func=cmd_messages_show)

    messages_unread = messages_sub.add_parser(
        "unread",
        parents=[common],
        help="mark a conversation unread again so it shows in the unread view",
    )
    messages_unread.add_argument(
        "peer",
        metavar="PEER",
        nargs="?",
        default=None,
        help="peer name, destination hash or hash prefix",
    )
    messages_unread.add_argument(
        "--group",
        default=None,
        metavar="ID|NAME",
        help="mark a group's posts unread instead of a peer's",
    )
    messages_unread.add_argument(
        "--all",
        action="store_true",
        help="mark the whole conversation unread instead of just the newest message",
    )
    messages_unread.set_defaults(func=cmd_messages_unread)

    messages_export = messages_sub.add_parser(
        "export",
        parents=[common],
        help="back up stored direct messages as a JSON export",
    )
    messages_export.add_argument(
        "--peer",
        default=None,
        metavar="NAME|HASH",
        help="export one sender's messages (name, hash or hash prefix)",
    )
    messages_export.add_argument(
        "--output",
        type=Path,
        metavar="FILE",
        help="write the export to a file instead of stdout",
    )
    messages_export.set_defaults(func=cmd_messages_export)

    messages_import = messages_sub.add_parser(
        "import",
        parents=[common],
        help="restore direct messages from a JSON export",
    )
    messages_import.add_argument(
        "file",
        metavar="FILE",
        type=Path,
        help='export file to read ("-" reads stdin)',
    )
    messages_import.set_defaults(func=cmd_messages_import)

    messages_prune = messages_sub.add_parser(
        "prune",
        parents=[common],
        help="delete stored messages older than a cutoff",
    )
    messages_prune.add_argument(
        "--older-than",
        required=True,
        metavar="DURATION",
        help="delete messages older than this (e.g. 30s, 5m, 2h, 7d, 90d; at most 10y)",
    )
    messages_prune.add_argument(
        "--peer",
        # SUPPRESS, not None: the parent `messages` parser also defines
        # --peer, and a subparser default of None would silently discard
        # a scope flag given before `prune` (e.g. `messages --peer alice
        # prune ...`). With SUPPRESS the parent value survives.
        default=argparse.SUPPRESS,
        metavar="NAME|HASH",
        help="only prune messages exchanged with this peer (name, hash or hash prefix)",
    )
    messages_prune.add_argument(
        "--group",
        # Same as --peer above: preserve a --group given before `prune`.
        default=argparse.SUPPRESS,
        metavar="ID|NAME",
        help="only prune messages from this group (id, id prefix or name)",
    )
    messages_prune.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be pruned without deleting anything",
    )
    messages_prune.set_defaults(func=cmd_messages_prune)

    messages_requests = messages_sub.add_parser(
        "requests",
        parents=[common],
        help="list DMs held for review from peers outside the allow-list",
    )
    messages_requests.set_defaults(func=cmd_messages_requests)

    messages_accept_request = messages_sub.add_parser(
        "accept-request",
        parents=[common],
        help="allow-list the sender of a held DM request and deliver it",
    )
    messages_accept_request.add_argument(
        "target", metavar="ID", help="request id or unique id prefix"
    )
    messages_accept_request.set_defaults(func=cmd_messages_accept_request)

    messages_decline_request = messages_sub.add_parser(
        "decline-request",
        parents=[common],
        help="drop a held DM request without allow-listing its sender",
    )
    messages_decline_request.add_argument(
        "target", metavar="ID", help="request id or unique id prefix"
    )
    messages_decline_request.add_argument(
        "--block",
        action="store_true",
        help="also add the sender to the block-list",
    )
    messages_decline_request.set_defaults(func=cmd_messages_decline_request)

    group = subparsers.add_parser(
        "group",
        parents=[common],
        help="private group DMs with a fixed member list",
    )
    group.set_defaults(func=cmd_group_list)
    group_sub = group.add_subparsers(dest="group_command", metavar="GROUP_COMMAND")

    group_create = group_sub.add_parser(
        "create",
        parents=[common],
        help="create a group with a fixed member list",
    )
    group_create.add_argument(
        "group_name", metavar="NAME", help="group name"
    )
    group_create.add_argument(
        "--peer",
        required=True,
        action="append",
        default=[],
        metavar="NAME|HASH",
        help="a member peer (repeatable; the creator is always a member)",
    )
    group_create.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_create.add_argument(
        "--secret",
        action="store_true",
        help="create a secret group: an opaque random id bound to the "
        "roster by a creator-signed attestation, instead of an id "
        "derived from the creator, name and roster",
    )
    group_create.set_defaults(func=cmd_group_create)

    group_list = group_sub.add_parser(
        "list",
        parents=[common],
        help="list known groups",
    )
    group_list.set_defaults(func=cmd_group_list)

    group_show = group_sub.add_parser(
        "show",
        parents=[common],
        help="show one group's details (creator, members, banned peers)",
    )
    group_show.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_show.set_defaults(func=cmd_group_show)

    group_delete = group_sub.add_parser(
        "delete",
        parents=[common],
        help="delete a group (frees capacity when a group cap is reached)",
    )
    group_delete.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_delete.set_defaults(func=cmd_group_delete)

    group_leave = group_sub.add_parser(
        "leave",
        parents=[common],
        help="leave a group you joined (stops its messages for good)",
    )
    group_leave.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_leave.set_defaults(func=cmd_group_leave)

    group_rejoin = group_sub.add_parser(
        "rejoin",
        parents=[common],
        help="undo a leave so the next message pins the group again",
    )
    group_rejoin.add_argument(
        "group", metavar="ID", help="full id of a group this node left"
    )
    group_rejoin.set_defaults(func=cmd_group_rejoin)

    group_left = group_sub.add_parser(
        "left",
        parents=[common],
        help="list groups this node has left",
    )
    group_left.set_defaults(func=cmd_group_left)

    group_ignore = group_sub.add_parser(
        "ignore",
        parents=[common],
        help="hide a group's messages from the default `tau messages` listing",
    )
    group_ignore.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_ignore.set_defaults(func=cmd_group_ignore)

    group_unignore = group_sub.add_parser(
        "unignore",
        parents=[common],
        help="show an ignored group's messages in `tau messages` again",
    )
    group_unignore.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_unignore.set_defaults(func=cmd_group_unignore)

    group_ignored = group_sub.add_parser(
        "ignored",
        parents=[common],
        help="list ignored groups",
    )
    group_ignored.set_defaults(func=cmd_group_ignored)

    group_add_member = group_sub.add_parser(
        "add-member",
        parents=[common],
        help="add peers to a group you created",
    )
    group_add_member.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_add_member.add_argument(
        "--peer",
        required=True,
        action="append",
        default=[],
        metavar="NAME|HASH",
        help="a peer to add (repeatable)",
    )
    group_add_member.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_add_member.set_defaults(func=cmd_group_add_member)

    group_remove_member = group_sub.add_parser(
        "remove-member",
        parents=[common],
        help="remove peers from a group you created",
    )
    group_remove_member.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_remove_member.add_argument(
        "--peer",
        required=True,
        action="append",
        default=[],
        metavar="NAME|HASH",
        help="a peer to remove (repeatable)",
    )
    group_remove_member.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_remove_member.set_defaults(func=cmd_group_remove_member)

    group_ban = group_sub.add_parser(
        "ban",
        parents=[common],
        help="ban a peer from a group you created",
    )
    group_ban.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_ban.add_argument(
        "peer", metavar="PEER", help="peer name or destination hash"
    )
    group_ban.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_ban.set_defaults(func=cmd_group_ban)

    group_unban = group_sub.add_parser(
        "unban",
        parents=[common],
        help="lift a ban on a group you created",
    )
    group_unban.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_unban.add_argument(
        "peer", metavar="PEER", help="peer name or destination hash"
    )
    group_unban.set_defaults(func=cmd_group_unban)

    group_banned = group_sub.add_parser(
        "banned",
        parents=[common],
        help="list the peers banned from a group you created",
    )
    group_banned.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_banned.set_defaults(func=cmd_group_banned)

    group_invite = group_sub.add_parser(
        "invite",
        parents=[common],
        help="mint an invite link for a group you created",
    )
    group_invite.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_invite.add_argument(
        "--closed",
        action="store_true",
        help="invitees ask to join and you approve them (default: open)",
    )
    group_invite.add_argument(
        "--open",
        action="store_true",
        help="invitees join at once (default unless the group already has invites)",
    )
    group_invite.add_argument(
        "--revoke",
        action="store_true",
        help="invalidate every invite minted for this group so far",
    )
    group_invite.set_defaults(func=cmd_group_invite)

    group_join = group_sub.add_parser(
        "join",
        parents=[common],
        help="join a group with an invite link",
    )
    group_join.add_argument(
        "link", metavar="LINK", help="invite link from `tau group invite`"
    )
    group_join.add_argument(
        "--timeout", type=float, default=20.0, metavar="SECONDS"
    )
    group_join.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for the creator to announce",
    )
    group_join.set_defaults(func=cmd_group_join)

    group_requests = group_sub.add_parser(
        "requests",
        parents=[common],
        help="list pending join requests for groups you created",
    )
    group_requests.add_argument(
        "group",
        metavar="GROUP",
        nargs="?",
        help="group id, id prefix or name (omit for all groups)",
    )
    group_requests.set_defaults(func=cmd_group_requests)

    group_approve = group_sub.add_parser(
        "approve",
        parents=[common],
        help="admit a pending join request to a group you created",
    )
    group_approve.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_approve.add_argument(
        "peer",
        metavar="PEER",
        help="requesting peer (name, destination hash, or hash prefix)",
    )
    group_approve.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_approve.set_defaults(func=cmd_group_approve)

    group_deny = group_sub.add_parser(
        "deny",
        parents=[common],
        help="refuse a pending join request to a group you created",
    )
    group_deny.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_deny.add_argument(
        "peer",
        metavar="PEER",
        help="requesting peer (name, destination hash, or hash prefix)",
    )
    group_deny.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_deny.set_defaults(func=cmd_group_deny)

    group_topic = group_sub.add_parser(
        "topic",
        parents=[common],
        help="set or show the topic of a group you created",
    )
    group_topic.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_topic.add_argument(
        "text",
        nargs="?",
        help="topic text (omit to show the current topic)",
    )
    group_topic.add_argument(
        "--file", type=Path, help="read the topic text from a file"
    )
    group_topic.add_argument(
        "--clear",
        action="store_true",
        help="remove the topic instead of setting one",
    )
    group_topic.set_defaults(func=cmd_group_topic)
    group_announcements = group_sub.add_parser(
        "announcements",
        parents=[common],
        help="switch a group you created to (or from) announcements-only",
    )
    group_announcements.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_announcements.add_argument(
        "state",
        metavar="STATE",
        nargs="?",
        choices=["on", "off"],
        help="on to let only the creator post, off to let every member post (omit to show the current setting)",
    )
    group_announcements.set_defaults(func=cmd_group_announcements)
    group_rename = group_sub.add_parser(
        "rename",
        parents=[common],
        help="rename a group you created",
    )
    group_rename.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_rename.add_argument(
        "new_name", metavar="NAME", help="the new group name"
    )
    group_rename.set_defaults(func=cmd_group_rename)

    group_poll = group_sub.add_parser(
        "poll",
        parents=[common],
        help="post a poll in a group (question + 2-8 options)",
    )
    group_poll.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_poll.add_argument(
        "question", metavar="QUESTION", help="the poll question"
    )
    group_poll.add_argument(
        "options",
        metavar="OPTION",
        nargs="+",
        help="2-8 distinct options",
    )
    group_poll.add_argument(
        "--timeout", type=float, default=20.0, metavar="SECONDS"
    )
    group_poll.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the poll when a member is offline",
    )
    group_poll.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the poll: queue it now and ask it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 29d23h — the 30d "
        "mail TTL less an hour of delivery slack)",
    )
    group_poll.set_defaults(func=cmd_group_poll)

    group_vote = group_sub.add_parser(
        "vote",
        parents=[common],
        help="vote on a group poll (option number or exact option text)",
    )
    group_vote.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_vote.add_argument(
        "poll",
        metavar="POLL",
        help="poll message id or unique prefix (see `tau messages --group`)",
    )
    group_vote.add_argument(
        "option",
        metavar="OPTION",
        help="option number (1-based) or the exact option text",
    )
    group_vote.add_argument(
        "--timeout", type=float, default=20.0, metavar="SECONDS"
    )
    group_vote.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the vote when a member is offline",
    )
    group_vote.set_defaults(func=cmd_group_vote)

    group_close_poll = group_sub.add_parser(
        "close-poll",
        parents=[common],
        help="close a group poll you created (freezes the tally)",
    )
    group_close_poll.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_close_poll.add_argument(
        "poll",
        metavar="POLL",
        help="poll message id or unique prefix (see `tau messages --group`)",
    )
    group_close_poll.add_argument(
        "--timeout", type=float, default=20.0, metavar="SECONDS"
    )
    group_close_poll.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the close when a member is offline",
    )
    group_close_poll.set_defaults(func=cmd_group_close_poll)

    group_send = group_sub.add_parser(
        "send",
        parents=[common],
        help="send a message to every member of a group",
    )
    group_send.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_send.add_argument(
        "message", nargs="?", help="message text (reads stdin when omitted)"
    )
    group_send.add_argument("--file", type=Path, help="read the message from a file")
    group_send.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    group_send.add_argument(
        "--reply-to",
        metavar="ID|PREFIX",
        help="quote an earlier message in this group (message id or unique prefix)",
    )
    group_send.add_argument(
        "--location",
        default=None,
        metavar='"LAT,LON [PLACE]"',
        help="pin a place on the message (e.g. --location "
        '"34.05,-83.88 Rock Springs Park"); without message text the '
        "place name (or the coordinates) becomes the text",
    )
    group_send.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_send.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the message when a member is offline",
    )
    group_send.add_argument(
        "--wait",
        type=float,
        default=None,
        metavar="SECONDS",
        help="after queueing, wait up to SECONDS for the message to be delivered",
    )
    group_send.add_argument(
        "--expire",
        metavar="DURATION",
        help="disappearing message: purge it from the receivers' inboxes "
        "after this long (e.g. 30s, 5m, 2h, 7d; at most 30d)",
    )
    group_send.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the message: queue it now and deliver it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 29d23h — the 30d "
        "mail TTL less an hour of delivery slack)",
    )
    group_send.set_defaults(func=cmd_group_send)

    group_send_file = group_sub.add_parser(
        "send-file",
        parents=[common],
        help="send a file to every member of a group",
    )
    group_send_file.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_send_file.add_argument(
        "path", type=Path, help="local file to send (at most 10 MiB)"
    )
    group_send_file.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        metavar="SECONDS",
        help="how long each member's transfer may take (default 300)",
    )
    group_send_file.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the file: stage it now and send it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 29d23h — the 30d "
        "mail TTL less an hour of delivery slack; needs a node "
        "serving at send time)",
    )
    group_send_file.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_send_file.set_defaults(func=cmd_group_send_file)

    group_pin = group_sub.add_parser(
        "pin",
        parents=[common],
        help="pin a message in this group so it stays easy to find",
    )
    group_pin.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_pin.add_argument(
        "message",
        metavar="MESSAGE",
        help="message id or unique prefix within the group",
    )
    group_pin.set_defaults(func=cmd_group_pin)

    group_unpin = group_sub.add_parser(
        "unpin",
        parents=[common],
        help="remove the pin from a message in this group",
    )
    group_unpin.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_unpin.add_argument(
        "message",
        metavar="MESSAGE",
        help="message id or unique prefix within the group",
    )
    group_unpin.set_defaults(func=cmd_group_unpin)

    group_pinned = group_sub.add_parser(
        "pinned",
        parents=[common],
        help="list pinned messages in this group",
    )
    group_pinned.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_pinned.add_argument(
        "--limit",
        type=int,
        default=30,
        metavar="N",
        help="show the newest N pinned messages (default 30)",
    )
    group_pinned.set_defaults(func=cmd_group_pinned)

    # A broadcast list is a named set of peers for one-to-many DMs: each
    # send fans out as an ordinary DM per member, so recipients never
    # see each other or the list — the middle ground between a group
    # (shared context) and repeating `tau send --to` by hand.
    broadcast = subparsers.add_parser(
        "broadcast",
        parents=[common],
        help="send one message to many peers as private DMs",
    )
    broadcast.set_defaults(func=cmd_broadcast_list)
    broadcast_sub = broadcast.add_subparsers(
        dest="broadcast_command", metavar="BROADCAST_COMMAND"
    )

    broadcast_create = broadcast_sub.add_parser(
        "create",
        parents=[common],
        help="save a named list of peers to message together",
    )
    broadcast_create.add_argument(
        "broadcast_name", metavar="NAME", help="broadcast list name"
    )
    broadcast_create.add_argument(
        "--peer",
        required=True,
        action="append",
        default=[],
        metavar="NAME|HASH",
        help="a member peer (repeatable; yourself is the sender, not a member)",
    )
    broadcast_create.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    broadcast_create.set_defaults(func=cmd_broadcast_create)

    broadcast_list = broadcast_sub.add_parser(
        "list",
        parents=[common],
        help="list saved broadcast lists",
    )
    broadcast_list.set_defaults(func=cmd_broadcast_list)

    broadcast_show = broadcast_sub.add_parser(
        "show",
        parents=[common],
        help="show one broadcast list and its members",
    )
    broadcast_show.add_argument(
        "broadcast", metavar="NAME", help="broadcast list name"
    )
    broadcast_show.set_defaults(func=cmd_broadcast_show)

    broadcast_add_member = broadcast_sub.add_parser(
        "add-member",
        parents=[common],
        help="add peers to a broadcast list",
    )
    broadcast_add_member.add_argument(
        "broadcast", metavar="NAME", help="broadcast list name"
    )
    broadcast_add_member.add_argument(
        "--peer",
        required=True,
        action="append",
        default=[],
        metavar="NAME|HASH",
        help="a peer to add (repeatable)",
    )
    broadcast_add_member.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    broadcast_add_member.set_defaults(func=cmd_broadcast_add_member)

    broadcast_remove_member = broadcast_sub.add_parser(
        "remove-member",
        parents=[common],
        help="remove peers from a broadcast list",
    )
    broadcast_remove_member.add_argument(
        "broadcast", metavar="NAME", help="broadcast list name"
    )
    broadcast_remove_member.add_argument(
        "--peer",
        required=True,
        action="append",
        default=[],
        metavar="NAME|HASH",
        help="a peer to remove (repeatable)",
    )
    broadcast_remove_member.set_defaults(func=cmd_broadcast_remove_member)

    broadcast_delete = broadcast_sub.add_parser(
        "delete",
        parents=[common],
        help="delete a broadcast list (frees capacity when the list cap is reached)",
    )
    broadcast_delete.add_argument(
        "broadcast", metavar="NAME", help="broadcast list name"
    )
    broadcast_delete.set_defaults(func=cmd_broadcast_delete)

    broadcast_rename = broadcast_sub.add_parser(
        "rename",
        parents=[common],
        help="rename a broadcast list, keeping its members and creation time",
    )
    broadcast_rename.add_argument(
        "broadcast", metavar="NAME", help="broadcast list name"
    )
    broadcast_rename.add_argument(
        "new_name", metavar="NEW_NAME", help="the list's new name"
    )
    broadcast_rename.set_defaults(func=cmd_broadcast_rename)

    # `tau broadcast send` shares `tau send`'s delivery flags: a
    # broadcast is a fan-out of ordinary DMs, so every flag means what
    # it means for a one-to-one send.  There is no --reply-to: a
    # broadcast is never a reply.
    broadcast_send = broadcast_sub.add_parser(
        "send",
        parents=[common],
        help="send a message to every member of a broadcast list, as a private DM each",
    )
    broadcast_send.add_argument(
        "broadcast", metavar="NAME", help="broadcast list name"
    )
    broadcast_send.add_argument(
        "message", nargs="?", help="message text (reads stdin when omitted)"
    )
    broadcast_send.add_argument("--file", type=Path, help="read the message from a file")
    broadcast_send.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    broadcast_send.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    broadcast_send.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the message when the peer is offline",
    )
    broadcast_send.add_argument(
        "--kind",
        default=None,
        metavar="SLUG",
        help="tag the message with a kind (default chat); receivers can"
        " filter their inbox by kind",
    )
    broadcast_send.add_argument(
        "--location",
        default=None,
        metavar='"LAT,LON [PLACE]"',
        help="pin a place on the message (e.g. --location "
        '"34.05,-83.88 Rock Springs Park"); without message text the '
        "place name (or the coordinates) becomes the text, and the "
        "message is tagged location unless --kind says otherwise",
    )
    broadcast_send.add_argument(
        "--expire",
        metavar="DURATION",
        help="disappearing message: purge it from the receiver's inbox "
        "after this long (e.g. 30s, 5m, 2h, 7d; at most 30d)",
    )
    broadcast_send.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the message: queue it now and deliver it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 29d23h — the 30d "
        "mail TTL less an hour of delivery slack)",
    )
    broadcast_send.add_argument(
        "--wait",
        type=float,
        default=None,
        metavar="SECONDS",
        help="after queueing, wait up to SECONDS for the message to be delivered",
    )
    broadcast_send.set_defaults(func=cmd_broadcast_send)

    broadcast_send_file = broadcast_sub.add_parser(
        "send-file",
        parents=[common],
        help="send a file to every member of a broadcast list, as a private DM each",
    )
    broadcast_send_file.add_argument(
        "broadcast", metavar="NAME", help="broadcast list name"
    )
    broadcast_send_file.add_argument(
        "path", type=Path, help="local file to send (at most 10 MiB)"
    )
    broadcast_send_file.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        metavar="SECONDS",
        help="how long each member's transfer may take (default 300)",
    )
    broadcast_send_file.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the file: stage it now and send it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 29d23h — the 30d "
        "mail TTL less an hour of delivery slack; needs a node "
        "serving at send time)",
    )
    broadcast_send_file.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    broadcast_send_file.set_defaults(func=cmd_broadcast_send_file)

    broadcast_poll = broadcast_sub.add_parser(
        "poll",
        parents=[common],
        help="post a poll to every member of a broadcast list, as a private DM poll each",
    )
    broadcast_poll.add_argument(
        "broadcast", metavar="NAME", help="broadcast list name"
    )
    broadcast_poll.add_argument(
        "question", metavar="QUESTION", help="the poll question"
    )
    broadcast_poll.add_argument(
        "options",
        metavar="OPTION",
        nargs="+",
        help="2-8 distinct options",
    )
    broadcast_poll.add_argument(
        "--timeout", type=float, default=20.0, metavar="SECONDS"
    )
    broadcast_poll.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the poll when a member is offline",
    )
    broadcast_poll.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the poll: queue it now and ask it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 29d23h — the 30d "
        "mail TTL less an hour of delivery slack)",
    )
    broadcast_poll.set_defaults(func=cmd_broadcast_poll)

    tan = subparsers.add_parser(
        "tan",
        parents=[common],
        help="use the Tan federated social feed",
    )
    tan_sub = tan.add_subparsers(dest="tan_command", metavar="TAN_COMMAND")
    tan_sub.required = True

    tan_post = tan_sub.add_parser(
        "post",
        parents=[common],
        help="sign and store a post, then publish it to known peers",
    )
    tan_post.add_argument(
        "text", nargs="?", help="post text (reads stdin when omitted)"
    )
    tan_post.add_argument("--file", type=Path, help="read the post text from a file")
    tan_post.add_argument(
        "--image",
        type=Path,
        default=None,
        metavar="IMAGE",
        help="attach one image to the post (JPEG, PNG, GIF or WebP, at most "
        "2 MiB) — the signed post carries its hash, size, mime and name, "
        "and the bytes are fetched on demand with `tau tan media`",
    )
    tan_post.add_argument(
        "--location",
        default=None,
        metavar="LAT,LON [NAME]",
        help="attach a place to the post (e.g. --location "
        '"34.05,-83.88 Rock Springs Park") — the signed post carries '
        "the place, and the timeline and console show it with an "
        "open-in-maps link",
    )
    tan_post.add_argument(
        "--reply-to",
        default=None,
        metavar="POST",
        help="reply to this post instead of posting standalone (post hash or unique hash prefix)",
    )
    tan_post.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_post.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_post.add_argument(
        "--delay",
        default=None,
        metavar="DURATION",
        help="schedule the post: queue it now and publish it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 7d) — needs a node "
        "serving with --social to publish it",
    )
    tan_post.add_argument(
        "--expire",
        default=None,
        metavar="DURATION",
        help="disappearing post: hide it from timelines and searches "
        "after this long (e.g. 30s, 5m, 2h, 7d; at most 7d) — the "
        "expiry is signed with the post and starts when it is "
        "published, not when it is queued; followers on tau releases "
        "older than this one reject expiring posts and so stop seeing "
        "the author's later posts",
    )
    tan_post.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_post.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_post.set_defaults(func=cmd_tan_post)

    tan_media = tan_sub.add_parser(
        "media",
        parents=[common],
        help="fetch the image attached to a Tan post, by its SHA-256",
    )
    tan_media.add_argument(
        "sha256", metavar="SHA256", help="image digest, as shown on the post"
    )
    tan_media.add_argument(
        "--author",
        default=None,
        metavar="HASH",
        help="author to fetch from (defaults to the author of the local "
        "post carrying this image)",
    )
    tan_media.add_argument(
        "--out",
        type=Path,
        default=None,
        metavar="PATH",
        help="copy the fetched image to this path (default: print the "
        "media store path)",
    )
    tan_media.add_argument("--timeout", type=float, default=60.0, metavar="SECONDS")
    tan_media.set_defaults(func=cmd_tan_media)

    tan_draft = tan_sub.add_parser(
        "draft",
        parents=[common],
        help="save a post draft to publish later",
    )
    tan_draft.add_argument(
        "text", nargs="?", help="draft text (reads stdin when omitted)"
    )
    tan_draft.add_argument(
        "--file", type=Path, help="read the draft text from a file"
    )
    tan_draft.add_argument(
        "--reply-to",
        default=None,
        metavar="POST",
        help="draft a reply to this post (post hash or unique hash prefix)",
    )
    tan_draft.add_argument(
        "--expire",
        default=None,
        metavar="DURATION",
        help="disappearing post: hide it from timelines and searches "
        "after this long once published (e.g. 30s, 5m, 2h, 7d; at most 7d)",
    )
    tan_draft.set_defaults(func=cmd_tan_draft)

    tan_drafts = tan_sub.add_parser(
        "drafts",
        parents=[common],
        help="list saved post drafts",
    )
    tan_drafts.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="show the newest N drafts (default 20)",
    )
    tan_drafts.set_defaults(func=cmd_tan_drafts)

    tan_publish_draft = tan_sub.add_parser(
        "publish-draft",
        parents=[common],
        help="publish a saved draft as a post",
    )
    tan_publish_draft.add_argument("draft", metavar="ID", help="draft id")
    tan_publish_draft.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_publish_draft.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_publish_draft.add_argument(
        "--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS"
    )
    tan_publish_draft.add_argument(
        "--timeout", type=float, default=15.0, metavar="SECONDS"
    )
    tan_publish_draft.set_defaults(func=cmd_tan_publish_draft)

    tan_discard_draft = tan_sub.add_parser(
        "discard-draft",
        parents=[common],
        help="delete a saved draft without publishing it",
    )
    tan_discard_draft.add_argument("draft", metavar="ID", help="draft id")
    tan_discard_draft.set_defaults(func=cmd_tan_discard_draft)

    tan_scheduled = tan_sub.add_parser(
        "scheduled",
        parents=[common],
        help="list Tan posts queued for later publishing",
    )
    tan_scheduled.set_defaults(func=cmd_tan_scheduled)

    tan_cancel_scheduled = tan_sub.add_parser(
        "cancel-scheduled",
        parents=[common],
        help="drop a queued Tan post before it publishes",
    )
    tan_cancel_scheduled.add_argument(
        "target", metavar="ID", help="scheduled post id or unique id prefix"
    )
    tan_cancel_scheduled.set_defaults(func=cmd_tan_cancel_scheduled)

    tan_edit_scheduled = tan_sub.add_parser(
        "edit-scheduled",
        parents=[common],
        help="change the text and/or publish time of a queued Tan post",
    )
    tan_edit_scheduled.add_argument(
        "target", metavar="ID", help="scheduled post id or unique id prefix"
    )
    tan_edit_scheduled.add_argument(
        "--text", default=None, help="replacement text for the queued post"
    )
    tan_edit_scheduled.add_argument(
        "--delay",
        default=None,
        help="publish this far from now, like 30m or 2h (at most 7d)",
    )
    tan_edit_scheduled.set_defaults(func=cmd_tan_edit_scheduled)

    tan_quote = tan_sub.add_parser(
        "quote",
        parents=[common],
        help="quote a post with your own comment (writes a signed quote entry to your chain)",
    )
    tan_quote.add_argument(
        "target", metavar="POST", help="post hash or unique hash prefix"
    )
    tan_quote.add_argument(
        "text", nargs="?", help="quote comment (reads stdin when omitted)"
    )
    tan_quote.add_argument("--file", type=Path, help="read the comment from a file")
    tan_quote.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_quote.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_quote.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the quote: queue it now and publish it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 7d)",
    )
    tan_quote.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_quote.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_quote.set_defaults(func=cmd_tan_quote)

    for verb, help_text in (
        ("like", "like a post (writes a signed like entry to your chain)"),
        ("unlike", "remove your like from a post (writes an unlike entry)"),
    ):
        tan_react = tan_sub.add_parser(verb, parents=[common], help=help_text)
        tan_react.add_argument(
            "target", metavar="POST", help="post hash or unique hash prefix"
        )
        tan_react.add_argument(
            "--to",
            action="append",
            default=None,
            metavar="NAME|HASH",
            help="publish only to these peers (repeatable)",
        )
        tan_react.add_argument(
            "--no-publish",
            action="store_true",
            help="store locally without contacting peers",
        )
        tan_react.add_argument(
            "--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS"
        )
        tan_react.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
        tan_react.set_defaults(func=cmd_tan_like if verb == "like" else cmd_tan_unlike)

    tan_repost = tan_sub.add_parser(
        "repost",
        parents=[common],
        help="repost a post (writes a signed repost entry to your chain)",
    )
    tan_repost.add_argument(
        "target", metavar="POST", help="post hash or unique hash prefix"
    )
    tan_repost.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_repost.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_repost.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the repost: queue it now and publish it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 7d)",
    )
    tan_repost.add_argument(
        "--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS"
    )
    tan_repost.add_argument(
        "--timeout", type=float, default=15.0, metavar="SECONDS"
    )
    tan_repost.set_defaults(func=cmd_tan_repost)

    # `tau tan share` shares `tau forward`'s delivery flags: a share is a
    # new message carrying the post's text plus an attribution line.
    tan_share = tan_sub.add_parser(
        "share",
        parents=[common],
        help="share a Tan post with a peer, a group, a broadcast list, or a channel (sends it as a message)",
    )
    share_target = tan_share.add_mutually_exclusive_group(required=True)
    share_target.add_argument(
        "--to",
        action="append",
        metavar="NAME|HASH",
        help="peer name or destination hash (repeatable: share with several peers)",
    )
    share_target.add_argument(
        "--group",
        metavar="NAME|ID",
        help="group name or id: share into the group instead of with a peer",
    )
    share_target.add_argument(
        "--broadcast",
        metavar="NAME",
        help="broadcast list name: share with every member instead of with a peer or a group",
    )
    share_target.add_argument(
        "--channel",
        metavar="NAME",
        help="channel name: share into the channel instead of with a peer or a group",
    )
    tan_share.add_argument(
        "target", metavar="POST", help="post hash or unique hash prefix"
    )
    tan_share.add_argument(
        "--timeout", type=float, default=20.0, metavar="SECONDS"
    )
    tan_share.add_argument(
        "--discover-timeout",
        type=float,
        # SUPPRESS, not 10.0: every send path defaults a missing value
        # to 10s, but `cmd_tan_share` must be able to tell an explicitly
        # passed value from the default — a channel share refuses
        # the flag instead of silently ignoring it.
        default=argparse.SUPPRESS,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    tan_share.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the message when the peer is offline",
    )
    tan_share.add_argument(
        "--wait",
        type=float,
        default=None,
        metavar="SECONDS",
        help="after queueing, wait up to SECONDS for the message to be delivered",
    )
    tan_share.set_defaults(func=cmd_tan_share)

    tan_delete = tan_sub.add_parser(
        "delete",
        parents=[common],
        help="delete one of your own posts (writes a signed delete entry to your chain)",
    )
    tan_delete.add_argument(
        "target", metavar="POST", help="post hash or unique hash prefix"
    )
    tan_delete.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_delete.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_delete.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_delete.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_delete.set_defaults(func=cmd_tan_delete)

    tan_edit = tan_sub.add_parser(
        "edit",
        parents=[common],
        help="edit one of your own posts (writes a signed edit entry to your chain)",
    )
    tan_edit.add_argument("target", metavar="POST", help="post hash or unique hash prefix")
    tan_edit.add_argument("text", nargs="?", help="replacement text (reads stdin when omitted)")
    tan_edit.add_argument("--file", type=Path, help="read the replacement text from a file")
    tan_edit.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_edit.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_edit.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_edit.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_edit.set_defaults(func=cmd_tan_edit)

    tan_poll = tan_sub.add_parser(
        "poll",
        parents=[common],
        help="ask a question with options (writes a signed poll entry to your chain)",
    )
    tan_poll.add_argument(
        "text", nargs="?", help="poll question (reads stdin when omitted)"
    )
    tan_poll.add_argument(
        "--file", type=Path, help="read the question from a file"
    )
    tan_poll.add_argument(
        "--option",
        action="append",
        default=None,
        dest="options",
        metavar="TEXT",
        help="one answer option (repeatable: give 2-8 options)",
    )
    tan_poll.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_poll.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_poll.add_argument(
        "--expire",
        default=None,
        metavar="DURATION",
        help="disappearing poll: hide it from timelines and searches "
        "after this long (e.g. 30s, 5m, 2h, 7d; at most 7d) — the "
        "expiry is signed with the poll",
    )
    tan_poll.add_argument(
        "--delay",
        default=None,
        metavar="DURATION",
        help="schedule the poll: queue it now and ask it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 7d) — needs a node "
        "serving with --social to publish it",
    )
    tan_poll.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_poll.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_poll.set_defaults(func=cmd_tan_poll)

    tan_vote = tan_sub.add_parser(
        "vote",
        parents=[common],
        help="vote for one option of a poll (writes a signed vote entry to your chain)",
    )
    tan_vote.add_argument(
        "target", metavar="POLL", help="poll hash or unique hash prefix"
    )
    tan_vote.add_argument(
        "option",
        metavar="OPTION",
        help="option number (1-based) or exact option text",
    )
    tan_vote.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_vote.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_vote.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_vote.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_vote.set_defaults(func=cmd_tan_vote)

    tan_close_poll = tan_sub.add_parser(
        "close-poll",
        parents=[common],
        help="close one of your own polls early (writes a signed close entry to your chain)",
    )
    tan_close_poll.add_argument(
        "target", metavar="POLL", help="poll hash or unique hash prefix"
    )
    tan_close_poll.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_close_poll.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_close_poll.add_argument(
        "--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS"
    )
    tan_close_poll.add_argument(
        "--timeout", type=float, default=15.0, metavar="SECONDS"
    )
    tan_close_poll.set_defaults(func=cmd_tan_close_poll)

    tan_results = tan_sub.add_parser(
        "results",
        parents=[common],
        help="show a poll's question, options and current tally",
    )
    tan_results.add_argument(
        "ref", metavar="HASH|PREFIX", help="poll hash or unique hash prefix"
    )
    tan_results.set_defaults(func=cmd_tan_results)

    tan_timeline = tan_sub.add_parser(
        "timeline",
        parents=[common],
        help="show posts from you and the authors you follow",
    )
    tan_timeline.add_argument(
        "--all", action="store_true", help="show every cached post"
    )
    tan_timeline.add_argument(
        "--author", metavar="NAME|HASH", help="show one author's posts"
    )
    tan_timeline.add_argument(
        "--list",
        metavar="NAME",
        default=None,
        help="show only posts from this list's members",
    )
    tan_timeline.add_argument(
        "--tag", metavar="TAG", help="show only posts carrying this tag"
    )
    tan_timeline.add_argument(
        "--limit", type=int, default=30, metavar="N", help="maximum posts"
    )
    tan_timeline.set_defaults(func=cmd_tan_timeline)

    tan_search = tan_sub.add_parser(
        "search",
        parents=[common],
        help="search stored posts by keyword",
    )
    tan_search.add_argument("query", metavar="TEXT", help="text to search for")
    tan_search.add_argument(
        "--author", metavar="NAME|HASH", help="search one author's posts"
    )
    tan_search.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="maximum posts",
    )
    tan_search.set_defaults(func=cmd_tan_search)

    tan_top = tan_sub.add_parser(
        "top",
        parents=[common],
        help="show the most-liked stored posts",
    )
    tan_top.add_argument(
        "--author", metavar="NAME|HASH", help="rank one author's posts"
    )
    tan_top.add_argument(
        "--limit",
        type=int,
        default=10,
        metavar="N",
        help="maximum posts (default 10)",
    )
    tan_top.set_defaults(func=cmd_tan_top)

    tan_tags = tan_sub.add_parser(
        "tags",
        parents=[common],
        help="list hashtags used in stored posts, most-used first",
    )
    tan_tags.add_argument(
        "--limit",
        type=int,
        default=30,
        metavar="N",
        help="maximum tags (default 30)",
    )
    tan_tags.set_defaults(func=cmd_tan_tags)

    tan_verify = tan_sub.add_parser(
        "verify",
        parents=[common],
        help="re-verify every stored post's signature and chain linkage",
    )
    tan_verify.set_defaults(func=cmd_tan_verify)

    tan_mentions = tan_sub.add_parser(
        "mentions",
        parents=[common],
        help="show replies to your own posts",
    )
    tan_mentions.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="maximum posts",
    )
    tan_mentions.set_defaults(func=cmd_tan_mentions)

    tan_thread = tan_sub.add_parser(
        "thread",
        parents=[common],
        help="show a post with its replies as a nested thread",
    )
    tan_thread.add_argument(
        "ref",
        metavar="HASH|PREFIX",
        help="post or reply hash (full hash or unique prefix)",
    )
    tan_thread.set_defaults(func=cmd_tan_thread)

    tan_show = tan_sub.add_parser(
        "show",
        parents=[common],
        help="show one post's full details",
    )
    tan_show.add_argument(
        "ref",
        metavar="HASH|PREFIX",
        help="post or reply hash (full hash or unique prefix)",
    )
    tan_show.set_defaults(func=cmd_tan_show)

    tan_likes = tan_sub.add_parser(
        "likes",
        parents=[common],
        help="show who currently likes a post",
    )
    tan_likes.add_argument(
        "ref", metavar="HASH|PREFIX", help="post or reply hash (full hash or unique prefix)"
    )
    tan_likes.set_defaults(func=cmd_tan_likes)

    tan_reposts = tan_sub.add_parser(
        "reposts",
        parents=[common],
        help="show who currently reposted a post",
    )
    tan_reposts.add_argument(
        "ref", metavar="HASH|PREFIX", help="post or reply hash (full hash or unique prefix)"
    )
    tan_reposts.set_defaults(func=cmd_tan_reposts)

    tan_quotes = tan_sub.add_parser(
        "quotes",
        parents=[common],
        help="show the standing quotes of a post",
    )
    tan_quotes.add_argument(
        "ref", metavar="HASH|PREFIX", help="post or reply hash (full hash or unique prefix)"
    )
    tan_quotes.set_defaults(func=cmd_tan_quotes)

    tan_follow = tan_sub.add_parser(
        "follow", parents=[common], help="follow a peer or author"
    )
    tan_follow.add_argument("target", metavar="NAME|HASH")
    tan_follow.set_defaults(func=cmd_tan_follow)

    tan_unfollow = tan_sub.add_parser(
        "unfollow", parents=[common], help="stop following an author"
    )
    tan_unfollow.add_argument("target", metavar="NAME|HASH")
    tan_unfollow.set_defaults(func=cmd_tan_unfollow)

    tan_following = tan_sub.add_parser(
        "following", parents=[common], help="list followed authors"
    )
    tan_following.set_defaults(func=cmd_tan_following)

    tan_followers = tan_sub.add_parser(
        "followers",
        parents=[common],
        help="list authors who follow you, from stored follow lists",
    )
    tan_followers.set_defaults(func=cmd_tan_followers)

    tan_publish_follows = tan_sub.add_parser(
        "publish-follows",
        parents=[common],
        help="publish your follow list as a signed document peers can fetch",
    )
    tan_publish_follows.set_defaults(func=cmd_tan_publish_follows)

    tan_follows_of = tan_sub.add_parser(
        "follows-of",
        parents=[common],
        help="show who a peer follows, fetching their signed follow list "
        "when this node does not hold a copy",
    )
    tan_follows_of.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer name, destination hash or hash prefix",
    )
    tan_follows_of.add_argument(
        "--cached",
        action="store_true",
        help="show the stored copy without fetching from the peer",
    )
    tan_follows_of.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        metavar="SECONDS",
        help="how long to wait for the peer to answer (default: %(default)s)",
    )
    tan_follows_of.set_defaults(func=cmd_tan_follows_of)

    tan_authors = tan_sub.add_parser(
        "authors", parents=[common], help="list every author in the local Tan store"
    )
    tan_authors.add_argument(
        "--sort",
        choices=["recent", "posts", "name"],
        default="recent",
        help="sort order (default: recent)",
    )
    tan_authors.add_argument(
        "--limit", type=int, default=50, metavar="N", help="maximum authors"
    )
    tan_authors.set_defaults(func=cmd_tan_authors)

    tan_suggest = tan_sub.add_parser(
        "suggest",
        parents=[common],
        help="suggest authors to follow, from who the authors you follow engage with",
    )
    tan_suggest.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_SUGGEST_LIMIT,
        metavar="N",
        help="maximum suggestions (default: %(default)s)",
    )
    tan_suggest.set_defaults(func=cmd_tan_suggest)
    tan_mute = tan_sub.add_parser(
        "mute", parents=[common], help="hide an author's posts from local views"
    )
    tan_mute.add_argument("target", metavar="NAME|HASH")
    tan_mute.set_defaults(func=cmd_tan_mute)

    tan_unmute = tan_sub.add_parser(
        "unmute", parents=[common], help="show a muted author's posts again"
    )
    tan_unmute.add_argument("target", metavar="NAME|HASH")
    tan_unmute.set_defaults(func=cmd_tan_unmute)

    tan_muted = tan_sub.add_parser("muted", parents=[common], help="list muted authors")
    tan_muted.set_defaults(func=cmd_tan_muted)

    tan_lists_cmd = tan_sub.add_parser(
        "lists",
        parents=[common],
        help="curate followed authors into named lists",
    )
    lists_sub = tan_lists_cmd.add_subparsers(
        dest="lists_cmd", metavar="LISTS_COMMAND"
    )
    lists_create = lists_sub.add_parser(
        "create", parents=[common], help="create an empty list"
    )
    lists_create.add_argument("name", metavar="NAME", help="list name")
    lists_delete = lists_sub.add_parser(
        "delete", parents=[common], help="delete a list"
    )
    lists_delete.add_argument("name", metavar="NAME", help="list name")
    lists_add = lists_sub.add_parser(
        "add", parents=[common], help="add authors to a list"
    )
    lists_add.add_argument("name", metavar="NAME", help="list name")
    lists_add.add_argument(
        "authors", nargs="+", metavar="NAME|HASH", help="authors to add"
    )
    lists_remove = lists_sub.add_parser(
        "remove", parents=[common], help="remove authors from a list"
    )
    lists_remove.add_argument("name", metavar="NAME", help="list name")
    lists_remove.add_argument(
        "authors", nargs="+", metavar="NAME|HASH", help="authors to remove"
    )
    lists_show = lists_sub.add_parser(
        "show", parents=[common], help="show a list's members"
    )
    lists_show.add_argument("name", metavar="NAME", help="list name")
    tan_lists_cmd.set_defaults(func=cmd_tan_lists)

    tan_mute_keyword = tan_sub.add_parser(
        "mute-keyword",
        parents=[common],
        help="hide posts containing a word or phrase from local views",
    )
    tan_mute_keyword.add_argument(
        "keyword", metavar="WORD|PHRASE", help="word or phrase to mute"
    )
    tan_mute_keyword.set_defaults(func=cmd_tan_mute_keyword)

    tan_unmute_keyword = tan_sub.add_parser(
        "unmute-keyword",
        parents=[common],
        help="stop hiding posts containing a word or phrase",
    )
    tan_unmute_keyword.add_argument(
        "keyword", metavar="WORD|PHRASE", help="word or phrase to unmute"
    )
    tan_unmute_keyword.set_defaults(func=cmd_tan_unmute_keyword)

    tan_muted_keywords = tan_sub.add_parser(
        "muted-keywords",
        parents=[common],
        help="list muted keywords",
    )
    tan_muted_keywords.set_defaults(func=cmd_tan_muted_keywords)

    tan_follow_tag = tan_sub.add_parser(
        "follow-tag",
        parents=[common],
        help="follow a hashtag so its posts join your timeline",
    )
    tan_follow_tag.add_argument("tag", metavar="#TAG", help="hashtag to follow")
    tan_follow_tag.set_defaults(func=cmd_tan_follow_tag)

    tan_unfollow_tag = tan_sub.add_parser(
        "unfollow-tag",
        parents=[common],
        help="stop following a hashtag",
    )
    tan_unfollow_tag.add_argument("tag", metavar="#TAG", help="hashtag to unfollow")
    tan_unfollow_tag.set_defaults(func=cmd_tan_unfollow_tag)

    tan_followed_tags = tan_sub.add_parser(
        "followed-tags",
        parents=[common],
        help="list followed hashtags",
    )
    tan_followed_tags.set_defaults(func=cmd_tan_followed_tags)

    tan_mute_tag = tan_sub.add_parser(
        "mute-tag",
        parents=[common],
        help="mute a hashtag so its posts stay out of your timeline",
    )
    tan_mute_tag.add_argument("tag", metavar="#TAG", help="hashtag to mute")
    tan_mute_tag.set_defaults(func=cmd_tan_mute_tag)

    tan_unmute_tag = tan_sub.add_parser(
        "unmute-tag",
        parents=[common],
        help="stop muting a hashtag",
    )
    tan_unmute_tag.add_argument("tag", metavar="#TAG", help="hashtag to unmute")
    tan_unmute_tag.set_defaults(func=cmd_tan_unmute_tag)

    tan_muted_tags = tan_sub.add_parser(
        "muted-tags",
        parents=[common],
        help="list muted hashtags",
    )
    tan_muted_tags.set_defaults(func=cmd_tan_muted_tags)

    tan_save = tan_sub.add_parser(
        "save", parents=[common], help="save a post to read later"
    )
    tan_save.add_argument(
        "ref", metavar="HASH|PREFIX", help="post hash (full hash or unique prefix)"
    )
    tan_save.set_defaults(func=cmd_tan_save)

    tan_unsave = tan_sub.add_parser(
        "unsave", parents=[common], help="remove a post from your saved list"
    )
    tan_unsave.add_argument(
        "ref", metavar="HASH|PREFIX", help="post hash (full hash or unique prefix)"
    )
    tan_unsave.set_defaults(func=cmd_tan_unsave)

    tan_saved = tan_sub.add_parser(
        "saved", parents=[common], help="list your saved posts"
    )
    tan_saved.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="maximum posts",
    )
    tan_saved.set_defaults(func=cmd_tan_saved)

    tan_hide = tan_sub.add_parser(
        "hide", parents=[common], help="hide a post from your local views"
    )
    tan_hide.add_argument(
        "ref", metavar="HASH|PREFIX", help="post hash (full hash or unique prefix)"
    )
    tan_hide.set_defaults(func=cmd_tan_hide)

    tan_unhide = tan_sub.add_parser(
        "unhide", parents=[common], help="show a hidden post in local views again"
    )
    tan_unhide.add_argument(
        "ref", metavar="HASH|PREFIX", help="post hash (full hash or unique prefix)"
    )
    tan_unhide.set_defaults(func=cmd_tan_unhide)

    tan_hidden = tan_sub.add_parser(
        "hidden", parents=[common], help="list hidden posts"
    )
    tan_hidden.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="maximum posts",
    )
    tan_hidden.set_defaults(func=cmd_tan_hidden)

    tan_pin = tan_sub.add_parser(
        "pin", parents=[common], help="pin one of your own posts to the top"
    )
    tan_pin.add_argument(
        "ref", metavar="HASH|PREFIX", help="post hash (full hash or unique prefix)"
    )
    tan_pin.set_defaults(func=cmd_tan_pin)

    tan_unpin = tan_sub.add_parser(
        "unpin", parents=[common], help="remove a post from your pinned list"
    )
    tan_unpin.add_argument(
        "ref", metavar="HASH|PREFIX", help="post hash (full hash or unique prefix)"
    )
    tan_unpin.set_defaults(func=cmd_tan_unpin)

    tan_pinned = tan_sub.add_parser(
        "pinned", parents=[common], help="list your pinned posts"
    )
    tan_pinned.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="maximum posts",
    )
    tan_pinned.set_defaults(func=cmd_tan_pinned)

    tan_sync = tan_sub.add_parser(
        "sync",
        parents=[common],
        help="pull missing posts from known peers",
    )
    tan_sync.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="sync only with these peers (repeatable)",
    )
    tan_sync.add_argument(
        "--author",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="sync only these authors (repeatable)",
    )
    tan_sync.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="posts per request (default: %(default)s)",
    )
    tan_sync.add_argument(
        "--explore",
        action="store_true",
        help="also fetch recent public posts to discover new authors",
    )
    tan_sync.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_sync.set_defaults(func=cmd_tan_sync)

    tan_export = tan_sub.add_parser(
        "export",
        parents=[common],
        help="back up stored Tan posts as a JSON export",
    )
    tan_export.add_argument(
        "--author",
        metavar="NAME|HASH",
        help="export one author's entries (default: your own)",
    )
    tan_export.add_argument(
        "--all", action="store_true", help="export every stored author's entries"
    )
    tan_export.add_argument(
        "--output",
        type=Path,
        metavar="FILE",
        help="write the export to a file instead of stdout",
    )
    tan_export.set_defaults(func=cmd_tan_export)

    tan_import = tan_sub.add_parser(
        "import",
        parents=[common],
        help="restore Tan posts from a JSON export (signatures re-verified)",
    )
    tan_import.add_argument(
        "file",
        metavar="FILE",
        type=Path,
        help='export file to read ("-" reads stdin)',
    )
    tan_import.set_defaults(func=cmd_tan_import)

    tan_rules = tan_sub.add_parser(
        "rules",
        parents=[common],
        help="show or change which remote Tan entries this node commits",
    )
    tan_rules.add_argument(
        "--default",
        choices=["allow", "deny"],
        default=None,
        help="policy for authors no other rule covers",
    )
    tan_rules.add_argument(
        "--allow-author",
        action="append",
        default=None,
        metavar="PEER",
        help="always admit this author (repeatable)",
    )
    tan_rules.add_argument(
        "--block-author",
        action="append",
        default=None,
        metavar="PEER",
        help="never admit this author (repeatable)",
    )
    tan_rules.add_argument(
        "--unallow-author",
        action="append",
        default=None,
        metavar="PEER",
        help="remove an author from the allow list (repeatable)",
    )
    tan_rules.add_argument(
        "--unblock-author",
        action="append",
        default=None,
        metavar="PEER",
        help="remove an author from the block list (repeatable)",
    )
    tan_rules.add_argument(
        "--followed-only",
        action="store_true",
        default=argparse.SUPPRESS,
        help="also admit authors you follow",
    )
    tan_rules.add_argument(
        "--no-followed-only",
        dest="followed_only",
        action="store_false",
        default=argparse.SUPPRESS,
        help="stop admitting authors just because you follow them",
    )
    tan_rules.add_argument(
        "--kinds",
        default=None,
        metavar="post,reply,repost,quote,edit,like,unlike,delete",
        help="comma-separated entry kinds to display (display-only: "
        "every kind is still stored, so chains stay complete)",
    )
    tan_rules.add_argument(
        "--all-kinds",
        action="store_true",
        help="display every entry kind (clears --kinds)",
    )
    tan_rules.set_defaults(func=cmd_tan_rules)

    channel = subparsers.add_parser(
        "channel",
        parents=[common],
        help="use public channels — open topic rooms anyone can join",
    )
    channel_sub = channel.add_subparsers(dest="channel_command", metavar="CHANNEL_COMMAND")
    channel_sub.required = True

    channel_join = channel_sub.add_parser(
        "join",
        parents=[common],
        help="join a channel by name, then fetch its recent history with `tau channel sync`",
    )
    channel_join.add_argument("channel", metavar="NAME", help="channel name to join")
    channel_join.set_defaults(func=cmd_channel_join)

    channel_leave = channel_sub.add_parser(
        "leave",
        parents=[common],
        help="leave a channel (cached messages are kept)",
    )
    channel_leave.add_argument("channel", metavar="NAME", help="channel name to leave")
    channel_leave.set_defaults(func=cmd_channel_leave)

    channel_list = channel_sub.add_parser(
        "list",
        parents=[common],
        help="list joined channels with cached message counts",
    )
    channel_list.set_defaults(func=cmd_channel_list)

    channel_show = channel_sub.add_parser(
        "show",
        parents=[common],
        help="show one channel's topic, counts, and last message",
    )
    channel_show.add_argument("channel", metavar="NAME", help="channel to show")
    channel_show.set_defaults(func=cmd_channel_show)

    channel_send = channel_sub.add_parser(
        "send",
        parents=[common],
        help="send a message to a joined channel",
    )
    channel_send.add_argument("channel", metavar="NAME", help="channel to send to")
    channel_send.add_argument(
        "text", nargs="?", help="message text (reads stdin when omitted)"
    )
    channel_send.add_argument("--file", type=Path, help="read the message text from a file")
    channel_send.add_argument(
        "--expire",
        metavar="DURATION",
        help="disappearing message: purge it from every member's store "
        "after this long (e.g. 30s, 5m, 2h, 7d; at most 30d)",
    )
    channel_send.add_argument(
        "--reply-to",
        default=None,
        metavar="HASH|PREFIX",
        help="reply to this message instead of posting standalone (message hash or unique hash prefix)",
    )
    channel_send.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    channel_send.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    channel_send.add_argument(
        "--location",
        default=None,
        metavar='"LAT,LON [PLACE]"',
        help="pin a place on the message (e.g. --location "
        '"34.05,-83.88 Rock Springs Park"); without message text the '
        "place name (or the coordinates) becomes the text",
    )
    channel_send.add_argument(
        "--delay",
        metavar="DURATION",
        help="schedule the message: queue it now and publish it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 7d). A node serving "
        "with --channels publishes it when the time comes",
    )
    channel_send.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    channel_send.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    channel_send.set_defaults(func=cmd_channel_send)

    channel_react = channel_sub.add_parser(
        "react",
        parents=[common],
        help="react to a stored channel message (one reaction per reactor; empty clears yours)",
    )
    channel_react.add_argument("channel", metavar="NAME", help="channel to react in")
    channel_react.add_argument(
        "target",
        metavar="HASH",
        help="message hash or unique hex prefix (at least 8 characters)",
    )
    channel_react.add_argument(
        "reaction",
        nargs="?",
        default=None,
        metavar="EMOJI",
        help="emoji or short token (omit with --clear to retract yours)",
    )
    channel_react.add_argument(
        "--clear",
        action="store_true",
        help="retract your reaction to the message",
    )
    channel_react.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    channel_react.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    channel_react.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    channel_react.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    channel_react.set_defaults(func=cmd_channel_react)

    channel_read = channel_sub.add_parser(
        "read",
        parents=[common],
        help="show stored messages for a channel, oldest first",
    )
    channel_read.add_argument("channel", metavar="NAME", help="channel to read")
    channel_read.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_CHANNEL_LIMIT,
        metavar="N",
        help="show the newest N messages (default 20)",
    )
    channel_read.set_defaults(func=cmd_channel_read)

    channel_mark_read = channel_sub.add_parser(
        "mark-read",
        parents=[common],
        help="clear a channel's unread badge by marking its newest message read",
    )
    channel_mark_read.add_argument(
        "channel", metavar="NAME", help="channel to mark read"
    )
    channel_mark_read.set_defaults(func=cmd_channel_mark_read)

    channel_scheduled = channel_sub.add_parser(
        "scheduled",
        parents=[common],
        help="list channel messages queued for later publishing",
    )
    channel_scheduled.add_argument(
        "channel",
        nargs="?",
        default=None,
        metavar="NAME",
        help="only messages queued for this channel (default: all channels)",
    )
    channel_scheduled.set_defaults(func=cmd_channel_scheduled)

    channel_cancel_scheduled = channel_sub.add_parser(
        "cancel-scheduled",
        parents=[common],
        help="drop a queued channel message before it publishes",
    )
    channel_cancel_scheduled.add_argument(
        "target", metavar="ID", help="scheduled message id or unique id prefix"
    )
    channel_cancel_scheduled.set_defaults(func=cmd_channel_cancel_scheduled)

    channel_edit_scheduled = channel_sub.add_parser(
        "edit-scheduled",
        parents=[common],
        help="change a queued channel message's text and/or publish time",
    )
    channel_edit_scheduled.add_argument(
        "target", metavar="ID", help="scheduled message id or unique id prefix"
    )
    channel_edit_scheduled.add_argument(
        "--text", default=None, help="replacement message text"
    )
    channel_edit_scheduled.add_argument(
        "--delay",
        default=None,
        metavar="DURATION",
        help="publish in DURATION (e.g. 30s, 5m, 2h, 7d); at most 7d",
    )
    channel_edit_scheduled.set_defaults(func=cmd_channel_edit_scheduled)
    channel_topic = channel_sub.add_parser(
        "topic",
        parents=[common],
        help="show or set a channel's topic (any member may set it; newest wins)",
    )
    channel_topic.add_argument("channel", metavar="NAME", help="channel to show or set the topic of")
    channel_topic.add_argument(
        "text", nargs="?", help="topic text (omit to show the current topic)"
    )
    channel_topic.add_argument("--file", type=Path, help="read the topic text from a file")
    channel_topic.add_argument(
        "--clear", action="store_true", help="clear the channel topic"
    )
    channel_topic.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    channel_topic.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    channel_topic.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    channel_topic.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    channel_topic.set_defaults(func=cmd_channel_topic)

    channel_pin = channel_sub.add_parser(
        "pin",
        parents=[common],
        help="pin a stored message so it stays surfaced (and survives pruning)",
    )
    channel_pin.add_argument("channel", metavar="NAME", help="channel holding the message")
    channel_pin.add_argument("digest", metavar="HASH", help="message hash to pin")
    channel_pin.set_defaults(func=cmd_channel_pin)

    channel_unpin = channel_sub.add_parser(
        "unpin",
        parents=[common],
        help="drop the pin on a message in a channel",
    )
    channel_unpin.add_argument("channel", metavar="NAME", help="channel holding the message")
    channel_unpin.add_argument("digest", metavar="HASH", help="message hash to unpin")
    channel_unpin.set_defaults(func=cmd_channel_unpin)

    channel_pins = channel_sub.add_parser(
        "pins",
        parents=[common],
        help="list pinned messages for a channel, newest pin first",
    )
    channel_pins.add_argument("channel", metavar="NAME", help="channel to list pins for")
    channel_pins.set_defaults(func=cmd_channel_pins)
    channel_search = channel_sub.add_parser(
        "search",
        parents=[common],
        help="search stored messages in a channel by keyword, newest first",
    )
    channel_search.add_argument("channel", metavar="NAME", help="channel to search")
    channel_search.add_argument("query", metavar="TEXT", help="text to search for")
    channel_search.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_CHANNEL_LIMIT,
        metavar="N",
        help="show the newest N matches (default 20)",
    )
    channel_search.set_defaults(func=cmd_channel_search)

    channel_edit = channel_sub.add_parser(
        "edit",
        parents=[common],
        help="edit your own message in a joined channel",
    )
    channel_edit.add_argument("channel", metavar="NAME", help="channel to edit in")
    channel_edit.add_argument(
        "target",
        metavar="DIGEST|PREFIX",
        help="message to edit (full digest, or a hex prefix of at least 4 characters)",
    )
    channel_edit.add_argument(
        "text", nargs="?", help="replacement text (reads stdin when omitted)"
    )
    channel_edit.add_argument("--file", type=Path, help="read the replacement text from a file")
    channel_edit.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    channel_edit.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    channel_edit.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    channel_edit.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    channel_edit.set_defaults(func=cmd_channel_edit)

    channel_edits = channel_sub.add_parser(
        "edits",
        parents=[common],
        help="list edit notices for a channel, newest first",
    )
    channel_edits.add_argument("channel", metavar="NAME", help="channel to list edits for")
    channel_edits.add_argument(
        "--limit",
        type=int,
        default=MAX_FEED_EDITS,
        metavar="N",
        help="show the newest N edits (default 50)",
    )
    channel_edits.set_defaults(func=cmd_channel_edits)

    channel_sync = channel_sub.add_parser(
        "sync",
        parents=[common],
        help="pull missing history for a channel (or every joined channel)",
    )
    channel_sync.add_argument(
        "channel", nargs="?", default=None, metavar="NAME",
        help="channel to sync (default: every joined channel)",
    )
    channel_sync.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="sync only from these peers (repeatable)",
    )
    channel_sync.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_CHANNEL_LIMIT,
        metavar="N",
        help="messages per page (default 20)",
    )
    channel_sync.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    channel_sync.set_defaults(func=cmd_channel_sync)

    channel_poll = channel_sub.add_parser(
        "poll",
        parents=[common],
        help="post a poll in a joined channel (question + 2-8 options)",
    )
    channel_poll.add_argument("channel", metavar="NAME", help="channel to post the poll in")
    channel_poll.add_argument(
        "question", metavar="QUESTION", help="the poll question"
    )
    channel_poll.add_argument(
        "options",
        metavar="OPTION",
        nargs="+",
        help="2-8 distinct options",
    )
    channel_poll.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    channel_poll.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    channel_poll.add_argument(
        "--delay",
        default=None,
        metavar="DURATION",
        help="schedule the poll: queue it now and ask it after "
        "this long (e.g. 30s, 5m, 2h, 7d; at most 7d) — needs a node "
        "serving with --channels to publish it",
    )
    channel_poll.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    channel_poll.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    channel_poll.set_defaults(func=cmd_channel_poll)

    channel_vote = channel_sub.add_parser(
        "vote",
        parents=[common],
        help="vote on a channel poll (option number or exact option text)",
    )
    channel_vote.add_argument("channel", metavar="NAME", help="channel the poll is in")
    channel_vote.add_argument(
        "poll",
        metavar="POLL",
        help="poll message hash or unique prefix (see `tau channel read`)",
    )
    channel_vote.add_argument(
        "option",
        metavar="OPTION",
        help="option number (1-based) or the exact option text",
    )
    channel_vote.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    channel_vote.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    channel_vote.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    channel_vote.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    channel_vote.set_defaults(func=cmd_channel_vote)

    channel_close_poll = channel_sub.add_parser(
        "close-poll",
        parents=[common],
        help="close a channel poll you created (freezes the tally)",
    )
    channel_close_poll.add_argument("channel", metavar="NAME", help="channel the poll is in")
    channel_close_poll.add_argument(
        "poll",
        metavar="POLL",
        help="poll message hash or unique prefix (see `tau channel read`)",
    )
    channel_close_poll.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    channel_close_poll.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    channel_close_poll.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    channel_close_poll.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    channel_close_poll.set_defaults(func=cmd_channel_close_poll)

    channel_retract = channel_sub.add_parser(
        "retract",
        parents=[common],
        help="retract one of your own messages from a channel",
    )
    channel_retract.add_argument(
        "channel", metavar="NAME", help="channel the message is in"
    )
    channel_retract.add_argument(
        "digest",
        metavar="DIGEST|PREFIX",
        help="message hash or unique prefix (shown by `tau channel read`)",
    )
    channel_retract.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="gossip only to these peers (repeatable)",
    )
    channel_retract.add_argument(
        "--no-publish",
        action="store_true",
        help="retract locally without gossiping it to peers",
    )
    channel_retract.add_argument(
        "--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS"
    )
    channel_retract.add_argument(
        "--timeout", type=float, default=15.0, metavar="SECONDS"
    )
    channel_retract.set_defaults(func=cmd_channel_retract)

    channel_retractions = channel_sub.add_parser(
        "retractions",
        parents=[common],
        help="list stored retract notices for a channel",
    )
    channel_retractions.add_argument(
        "channel", metavar="NAME", help="channel to list"
    )
    channel_retractions.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_CHANNEL_LIMIT,
        metavar="N",
        help="show the newest N notices (default 20)",
    )
    channel_retractions.set_defaults(func=cmd_channel_retractions)

    bundle = subparsers.add_parser(
        "bundle",
        parents=[common],
        help="carry mail and Tan posts on a file (sneakernet)",
    )
    bundle_sub = bundle.add_subparsers(
        dest="bundle_command", metavar="BUNDLE_COMMAND"
    )
    bundle_sub.required = True

    bundle_export = bundle_sub.add_parser(
        "export",
        parents=[common],
        help="write sealed mail (and optionally Tan posts) to a signed bundle file",
    )
    bundle_export.add_argument(
        "file", metavar="FILE", type=Path, help="bundle file to write"
    )
    bundle_export.add_argument(
        "--tan",
        action="store_true",
        help="also include Tan posts from every stored author",
    )
    bundle_export.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BUNDLE_BYTES,
        metavar="N",
        help="refuse to write a bundle larger than N bytes (default: %(default)s)",
    )
    bundle_export.set_defaults(func=cmd_bundle_export)

    bundle_import = bundle_sub.add_parser(
        "import",
        parents=[common],
        help="merge a bundle file into this node's mailbox and Tan store",
    )
    bundle_import.add_argument(
        "file",
        metavar="FILE",
        type=Path,
        help='bundle file to read ("-" reads stdin)',
    )
    bundle_import.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BUNDLE_BYTES,
        metavar="N",
        help="refuse to read a bundle larger than N bytes (default: %(default)s)",
    )
    bundle_import.set_defaults(func=cmd_bundle_import)

    bundle_inspect = bundle_sub.add_parser(
        "inspect",
        parents=[common],
        help="describe a bundle file without importing it",
    )
    bundle_inspect.add_argument(
        "file",
        metavar="FILE",
        type=Path,
        help='bundle file to read ("-" reads stdin)',
    )
    bundle_inspect.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BUNDLE_BYTES,
        metavar="N",
        help="refuse to read a bundle larger than N bytes (default: %(default)s)",
    )
    bundle_inspect.set_defaults(func=cmd_bundle_inspect)

    return parser


# -- logging and output helpers -------------------------------------------


def _app_log_level(args: argparse.Namespace) -> int:
    if getattr(args, "quiet", False):
        return logging.ERROR
    verbose = getattr(args, "verbose", 0)
    if verbose >= 2:
        return logging.DEBUG
    if verbose == 1:
        return logging.INFO
    return logging.WARNING


def _rns_log_level(args: argparse.Namespace) -> int:
    explicit = getattr(args, "rns_log_level", None)
    if explicit is not None:
        return max(0, min(8, int(explicit)))
    if getattr(args, "quiet", False):
        return 1
    verbose = getattr(args, "verbose", 0)
    if verbose >= 2:
        return 5
    if verbose == 1:
        return 4
    return 2


def _configure_logging(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=_app_log_level(args),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )


def _json_mode(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "json_output", False))


def _emit(payload: Any, json_output: bool, *, file: Any = None) -> None:
    stream = file or sys.stdout
    if json_output:
        print(json.dumps(payload, sort_keys=True), file=stream, flush=True)
        return
    print(payload, file=stream, flush=True)


def _emit_event(args: argparse.Namespace, event: str, **fields: Any) -> None:
    if _json_mode(args):
        payload = {"event": event}
        payload.update(fields)
        print(json.dumps(payload, sort_keys=True), flush=True)


def _install_stop_handlers(stop: threading.Event) -> Dict[int, Any]:
    """Turn SIGINT/SIGTERM into a graceful stop request.

    Reticulum installs its own handlers that call ``os._exit``; overriding them
    lets us save the peer registry and detach interfaces cleanly.
    """
    previous: Dict[int, Any] = {}

    def request_stop(signum: int, frame: Any) -> None:
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[signum] = signal.signal(signum, request_stop)
        except (ValueError, OSError):  # pragma: no cover - non-main thread
            pass
    return previous


def _restore_stop_handlers(previous: Dict[int, Any]) -> None:
    for signum, handler in previous.items():
        try:
            signal.signal(signum, handler)
        except (ValueError, OSError):  # pragma: no cover
            pass


def _stamp(timestamp: Optional[float] = None) -> str:
    return datetime.fromtimestamp(timestamp or time.time()).strftime("%H:%M:%S")


def _ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def _next_try_at(entry: Any) -> Optional[float]:
    """When an outbox entry may next be attempted: the later of its
    delivery backoff and its scheduled send time, if either is set."""
    next_try = entry.next_attempt_at
    send_after = getattr(entry, "send_after", None)
    if send_after is not None and (next_try is None or send_after > next_try):
        return send_after
    return next_try



_DURATION_UNITS = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 7 * 86400,
    "y": 365 * 86400,
}

# A prune cutoff may reach years back; the 30-day mail TTL cap only
# applies to disappearing messages, never to pruning old history.
_PRUNE_MAX_SECONDS = 10 * 365 * 24 * 60 * 60


def _describe_duration(seconds: float) -> str:
    """Shortest whole-unit rendering of a duration in seconds."""
    for suffix, size in (
        ("y", 365 * 86400),
        ("w", 7 * 86400),
        ("d", 86400),
        ("h", 3600),
        ("m", 60),
        ("s", 1),
    ):
        if seconds >= size and seconds % size == 0:
            return f"{int(seconds // size)}{suffix}"
    return f"{seconds:g}s"


def _parse_duration(
    value: str,
    *,
    max_seconds: float = MAX_MAIL_TTL,
    label: str = "--expire",
    flag: Optional[str] = None,
) -> float:
    """Parse a ``--expire``/``--delay``/``--older-than`` duration like ``30s``, ``5m``, ``2h``, ``7d``.

    A bare number means seconds.  Returns the duration in seconds;
    raises ``ValueError`` for anything unparseable, non-positive, or
    longer than *max_seconds* (the mail TTL by default: a disappearing
    message cannot outlive the store-and-forward envelope that might
    carry it).
    """
    if flag is not None:
        # Main's `--delay` caller names the flag this way; the prune
        # caller names it `label`.  Both mean the flag shown in errors.
        label = flag
    text = (value or "").strip().lower()
    if not text:
        raise ValueError(f"{label} needs a duration like 30s, 5m, 2h or 7d")
    unit = 1.0
    if text[-1] in _DURATION_UNITS:
        unit = float(_DURATION_UNITS[text[-1]])
        text = text[:-1]
    try:
        amount = float(text)
    except ValueError as exc:
        raise ValueError(
            f"invalid {label} duration {value!r} (try 30s, 5m, 2h or 7d)"
        ) from exc
    seconds = amount * unit
    if not seconds > 0:
        raise ValueError(f"{label} duration must be positive")
    if seconds > max_seconds:
        raise ValueError(
            f"{label} duration must be at most {_describe_duration(max_seconds)}"
        )
    return seconds

def _print_peer_table(
    peers: Iterable[Peer],
    *,
    now: Optional[float] = None,
    safety_numbers: Optional[Dict[str, str]] = None,
    lookalikes: Optional[Dict[str, Peer]] = None,
    pinned: Optional[Set[str]] = None,
) -> None:
    """Print the peers table, flagging possible lookalikes.

    *lookalikes* maps a peer's destination hex hash to the verified
    peer or saved contact it may be impersonating (see
    :meth:`PeerRegistry.lookalike_for`); a flagged peer is rendered
    with the collision in its name cell and a warning below the table.
    *pinned* names the destination hashes pinned to the top of the
    list, shown with a 📌 in the name cell.
    """
    rows = list(peers)
    if not rows:
        print("No peers known. Run `tau discover` or `tau serve` first.")
        return
    timestamp = now if now is not None else time.time()
    header = ("DESTINATION", "NAME", "VERIFIED", "HOPS", "LAST SEEN", "LXMF")
    table: List[tuple] = [header]
    for peer in rows:
        name = peer.display_name
        if peer.alias and peer.alias != peer.name:
            name = f"{peer.alias} ({peer.name})"
        collided = lookalikes.get(peer.hexhash) if lookalikes else None
        if collided is not None:
            name = f"{name} ⚠ lookalike of {collided.display_name}"
        if pinned is not None and peer.hexhash in pinned:
            name = f"📌 {name}"
        table.append(
            (
                peer.hexhash,
                name,
                "✓" if peer.is_verified else "",
                str(peer.hops),
                _ago(peer.age(timestamp)),
                peer.lxmf_hash or "—",
            )
        )
    widths = [max(len(row[i]) for row in table) for i in range(len(header))]
    for row in table:
        print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
    changed = [p for p in rows if p.key_change_at is not None]
    for peer in changed:
        print(
            f"WARNING: {peer.display_name} ({short_hash(peer.hexhash)}) "
            "presented a different public key under the same destination "
            "hash; the recorded key was kept. Re-verify this peer out of "
            "band before trusting it.",
            file=sys.stderr,
        )
    if lookalikes:
        for peer in rows:
            collided = lookalikes.get(peer.hexhash)
            if collided is None:
                continue
            print(
                f"WARNING: {peer.display_name} ({short_hash(peer.hexhash)}) "
                "is unverified and announces with the same name as "
                f"{collided.display_name} "
                f"({short_hash(collided.hexhash)}); treat it as a stranger "
                "until its safety number is compared out of band.",
                file=sys.stderr,
            )
    renamed = [
        p for p in rows if p.is_verified and p.name_change_at is not None
    ]
    for peer in renamed:
        previous = (
            f" from {peer.previous_name!r}" if peer.previous_name else ""
        )
        print(
            f"WARNING: verified peer {peer.display_name} "
            f"({short_hash(peer.hexhash)}) changed its announced name"
            f"{previous}; re-verify out of band before trusting the "
            "new name.",
            file=sys.stderr,
        )
    if safety_numbers:
        print("\nSafety numbers — compare each with the peer over another "
              "channel; a match confirms the key is theirs:")
        for peer in rows:
            number = safety_numbers.get(peer.hexhash)
            if number:
                print(f"  {peer.display_name} ({short_hash(peer.hexhash)}): {number}")


def _lookalike_map(
    registry: PeerRegistry, peers: Iterable[Peer]
) -> Dict[str, Peer]:
    """Map destination hex hashes to the peer each lookalike resembles.

    Only peers :meth:`PeerRegistry.lookalike_for` flags appear, so the
    caller can pass the result straight to :func:`_print_peer_table`
    or annotate JSON output.
    """
    found: Dict[str, Peer] = {}
    for peer in peers:
        collided = registry.lookalike_for(peer)
        if collided is not None:
            found[peer.hexhash] = collided
    return found


def _make_config(
    args: argparse.Namespace,
    *,
    announce_interval: float = DEFAULT_ANNOUNCE_INTERVAL,
    announce_interval_live: bool = False,
) -> TauConfig:
    return TauConfig.resolve(
        data_dir=getattr(args, "data_dir", None),
        rns_config_dir=getattr(args, "rns_config", None),
        name=getattr(args, "name", None),
        rns_loglevel=_rns_log_level(args),
        announce_interval=announce_interval,
        announce_interval_live=announce_interval_live,
    )


def _read_message(args: argparse.Namespace) -> str:
    if getattr(args, "file", None):
        try:
            return Path(args.file).expanduser().read_text(encoding="utf-8")
        except OSError as exc:
            raise NodeError(f"could not read {args.file}: {exc}") from exc
    text = getattr(args, "message", None)
    if text is None:
        text = getattr(args, "text", None)
    if text is not None:
        return text
    if not sys.stdin.isatty():
        return sys.stdin.read()
    raise NodeError("provide a message, --file, or pipe text on stdin")


def _location_from_args(args: argparse.Namespace) -> Optional[Dict[str, Any]]:
    """Parse a ``--location`` flag, or ``None`` when it was not given."""
    raw = getattr(args, "location", None)
    if raw is None:
        return None
    try:
        return parse_location_arg(raw)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc


def _message_text_with_location(
    args: argparse.Namespace, location: Optional[Dict[str, Any]]
) -> str:
    """Message text for a send, defaulting a bare pin to its label.

    A ``--location`` without message text, a --file, or piped stdin
    sends the place name (or the coordinates) as the text, so `tau
    send --location "..."` works on its own; the pin itself rides as
    a structured field either way.
    """
    if (
        location is not None
        and getattr(args, "message", None) is None
        and getattr(args, "text", None) is None
        and getattr(args, "file", None) is None
        and sys.stdin.isatty()
    ):
        return location_default_text(location)
    return _read_message(args)


def _detect_image_mime(data: bytes) -> Optional[str]:
    """Identify an image by its magic bytes, or ``None`` when unknown."""
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def _read_image(path: Path, data_dir: Path) -> Dict[str, Any]:
    """Read, validate and store an image for a Tan post.

    Returns the attachment dict the signed post carries — hash, size,
    mime and name.  The bytes go into the local media store first, so
    the author can serve them when a peer asks.
    """
    from .protocol import sanitize_file_name

    source = Path(path).expanduser()
    try:
        data = source.read_bytes()
    except OSError as exc:
        raise NodeError(f"could not read {source}: {exc}") from exc
    if len(data) < 1 or len(data) > MAX_IMAGE_BYTES:
        raise NodeError(
            f"image of {len(data)} bytes is outside the 1..{MAX_IMAGE_BYTES} "
            "byte limit"
        )
    mime = _detect_image_mime(data)
    if mime is None:
        raise NodeError(
            "image must be a JPEG, PNG, GIF or WebP file "
            f"(unsupported format: {source.name})"
        )
    name = sanitize_file_name(source.name)
    if not name:
        raise NodeError(f"image file name {source.name!r} is not usable")
    digest = hashlib.sha256(data).hexdigest()
    try:
        store_media(data_dir, digest, data, pinned=True)
    except TanError as exc:
        raise NodeError(str(exc)) from exc
    return {"sha256": digest, "size": len(data), "mime": mime, "name": name}


# -- commands --------------------------------------------------------------


def _prompt_setup_value(prompt, default, *, input_fn=input, validate=None):
    """Ask one setup question, re-prompting while ``validate`` rejects it.

    Enter accepts ``default``.  EOF cancels the whole setup: callers run
    before any file is written, so nothing is left behind.
    """
    while True:
        try:
            raw = input_fn(f"{prompt} [{default}]: ")
        except EOFError:
            raise NodeError("setup cancelled — nothing was written") from None
        answer = raw.strip() or default
        if validate is None:
            return answer
        try:
            return validate(answer)
        except ValueError as exc:
            print(f"  {exc} — try again.")


def _prompt_setup_yes_no(prompt, default, *, input_fn=input) -> bool:
    """Ask one yes/no setup question; Enter accepts ``default``."""
    hint = "Y/n" if default else "y/N"
    while True:
        try:
            raw = input_fn(f"{prompt} [{hint}]: ")
        except EOFError:
            raise NodeError("setup cancelled — nothing was written") from None
        answer = raw.strip().lower()
        if not answer:
            return default
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("  Please answer y or n.")


def _validate_setup_host_port(value, *, default_host):
    """Parse a setup address answer, returning it normalized as host:port."""
    host, port = parse_host_port(value, default_host=default_host)
    return f"{host}:{port}"


def run_setup_dialogue(args, config, *, input_fn=input) -> None:
    """Interactive first-run setup for `tau init`.

    Asks for the node name, LAN discovery, and optional TCP interfaces,
    writing the answers back onto ``args`` so ``cmd_init`` proceeds with
    them.  Runs before any file is written, so cancelling (Ctrl-C /
    Ctrl-D) leaves nothing behind.  ``input_fn`` is injectable for tests.
    """
    print("First-time setup — press Enter to accept the [default].")
    if getattr(args, "name", None) is None:
        args.name = sanitize_name(
            _prompt_setup_value("Node name", config.name, input_fn=input_fn),
            fallback=config.name,
        )
    auto_interface = _prompt_setup_yes_no(
        "Discover peers on the local network (AutoInterface)",
        True,
        input_fn=input_fn,
    )
    args.no_auto_interface = not auto_interface
    if _prompt_setup_yes_no(
        "Listen for TCP connections from other nodes", False, input_fn=input_fn
    ):
        args.tcp_listen = _prompt_setup_value(
            "Listen address",
            f"0.0.0.0:{PROTOCOL_TCP_PORT}",
            input_fn=input_fn,
            validate=lambda value: _validate_setup_host_port(
                value, default_host="0.0.0.0"
            ),
        )
    if _prompt_setup_yes_no(
        "Connect to another node over TCP", False, input_fn=input_fn
    ):
        args.tcp_connect = _prompt_setup_value(
            "Connect to (HOST:PORT)",
            f"127.0.0.1:{PROTOCOL_TCP_PORT}",
            input_fn=input_fn,
            validate=lambda value: _validate_setup_host_port(
                value, default_host="127.0.0.1"
            ),
        )


def _setup_dialogue_wanted(args, config) -> bool:
    """Whether `tau init` should run the interactive setup dialogue.

    Only for a first run on a terminal: scripts (piped stdin, ``--json``),
    explicit interface flags, and ``--yes`` all keep the silent defaults,
    and re-running init on an existing node never re-prompts.
    """
    if getattr(args, "yes", False):
        return False
    if _json_mode(args):
        return False
    if config.rns_config_file.exists():
        return False
    if (
        getattr(args, "tcp_listen", None) is not None
        or getattr(args, "tcp_connect", None) is not None
        or getattr(args, "no_auto_interface", False)
    ):
        return False
    return sys.stdin.isatty()


def cmd_init(args: argparse.Namespace) -> int:
    config = _make_config(args)
    if _setup_dialogue_wanted(args, config):
        run_setup_dialogue(args, config)
    config.prepare()

    auto_interface = not getattr(args, "no_auto_interface", False)
    changed = write_rns_config(
        config.rns_config_file,
        tcp_listen=getattr(args, "tcp_listen", None),
        tcp_connect=getattr(args, "tcp_connect", None),
        auto_interface=auto_interface,
        loglevel=config.rns_loglevel,
        force=bool(getattr(args, "force", False)),
    )
    if getattr(args, "name", None):
        write_node_metadata(config.data_dir, args.name)
        config = _make_config(args)  # refresh the resolved name

    identity = load_or_create_identity(config.identity_path)
    destination = destination_hash(identity).hex()
    payload = {
        "name": config.name,
        "destination": destination,
        "identity": identity.hexhash,
        "data_dir": str(config.data_dir),
        "rns_config": str(config.rns_config_file),
        "config_written": changed,
        "auto_interface": auto_interface,
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f'Node "{config.name}" is ready.')
        print(f"  destination: {destination}")
        print(f"  identity:    {identity.hexhash}")
        print(f"  data dir:    {config.data_dir}")
        print(
            f"  config:      {config.rns_config_file}"
            + ("" if changed else " (existing)")
        )
        print()
        print("Start a node with:  tau serve")
        print("Find peers with:    tau discover")
    return _EXIT_OK


def _node_callbacks(args: argparse.Namespace):
    json_output = _json_mode(args)

    def on_peer(peer: Peer) -> None:
        if json_output:
            _emit_event(args, "peer", peer=peer.public_dict())
        else:
            print(
                f"[{_stamp()}] peer {peer.display_name} "
                f"({short_hash(peer.hexhash)}) at {peer.hops} hop(s)",
                flush=True,
            )

    def on_message(record: Dict[str, Any]) -> None:
        if json_output:
            _emit_event(args, "message", message=record)
        else:
            sender = record.get("from", "unknown")
            destination = short_hash(record.get("destination") or "")
            suffix = f" ({destination})" if destination else ""
            text = record.get("text", "")
            if record.get("kind") == "group":
                group_name = record.get("group_name") or "group"
                print(
                    f"[{_stamp()}] group message from {sender}{suffix}"
                    f" in {group_name}:",
                    flush=True,
                )
            else:
                print(f"[{_stamp()}] message from {sender}{suffix}:", flush=True)
            for line in str(text).splitlines() or [""]:
                print(f"  {line}", flush=True)

    def on_file(record: Dict[str, Any]) -> None:
        if json_output:
            _emit_event(args, "file", file=record)
        else:
            sender = record.get("from", "unknown")
            destination = short_hash(record.get("destination") or "")
            suffix = f" ({destination})" if destination else ""
            print(
                f"[{_stamp()}] file from {sender}{suffix}: "
                f"{record.get('name')} ({record.get('size')} bytes)",
                flush=True,
            )
            forwarded_from = clean_forwarded_from_or_none(
                record.get("forwarded_from")
            )
            if forwarded_from is not None:
                print(
                    f"  forwarded from {forwarded_from['name']}",
                    flush=True,
                )
            print(f"  saved to {record.get('path')}", flush=True)

    return on_peer, on_message, on_file


def _default_web_dir() -> Optional[Path]:
    override = os.environ.get("TAU_WEB_DIR")
    if override:
        return Path(override).expanduser()
    candidate = Path(__file__).resolve().parent.parent / "web" / "out"
    return candidate if candidate.exists() else None


def _print_tan_image(post: Dict[str, Any]) -> None:
    """Print a post's image attachment line, when it has one."""
    image = post.get("image")
    if not isinstance(image, dict):
        return
    size = int(image.get("size") or 0)
    size_text = f"{size / 1024:.1f} KiB" if size >= 1024 else f"{size} B"
    print(
        f"  🖼 {image.get('name')} ({size_text}, {image.get('mime')}) — "
        f"download with `tau tan media {image.get('sha256')}`"
    )


def _print_tan_location(post: Dict[str, Any], indent: str = "  ") -> None:
    """Print a post's place attachment lines, when it has one.

    A hand-edited record with a bad pin just shows its text: the
    cleaner drops it instead of breaking the display, like a bad DM
    pin in :func:`cmd_messages`.
    """
    location = clean_location(post.get("location"))
    if location is None:
        return
    name = location.get("name")
    label = name if isinstance(name, str) and name else "Location"
    print(f"{indent}📍 {label} — {location['lat']}, {location['lon']}")
    print(f"{indent}{location_maps_url(location)}")


def _print_tan_posts(
    posts: Sequence[Dict[str, Any]],
    *,
    json_output: bool,
    empty_message: str = 'No posts yet. Try `tau tan post "hello"` or `tau tan sync --explore`.',
) -> None:
    if json_output:
        _emit({"posts": list(posts), "count": len(posts)}, True)
        return
    if not posts:
        print(empty_message)
        return
    for post in posts:
        name = post.get("name") or "anonymous"
        author = short_hash(str(post.get("author", "")))
        when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        via = f" via #{post['via_tag']}" if post.get("via_tag") else ""
        print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}{' (edited)' if post.get('edited') else ''}{' 📌' if post.get('pinned') else ''}{via}")
        if post.get("kind") == "reply":
            parent = post.get("reply_to")
            if isinstance(parent, dict):
                if parent.get("blocked"):
                    print("  in reply to a blocked author's post")
                elif parent.get("deleted"):
                    print("  in reply to a deleted post")
                elif parent.get("muted"):
                    print("  in reply to a muted post")
                else:
                    parent_text = str(parent.get("text") or "")
                    if len(parent_text) > 80:
                        parent_text = parent_text[:80] + "…"
                    print(
                        f"  in reply to {parent.get('name') or 'anonymous'}: "
                        f"{parent_text}"
                    )
            else:
                print(f"  in reply to {short_hash(str(post.get('target', '')))}")
        if post.get("kind") == "repost":
            original = post.get("repost_of")
            if isinstance(original, dict):
                if original.get("deleted"):
                    print("  reposted a deleted post")
                else:
                    print(
                        f"  reposted {original.get('name') or 'anonymous'} "
                        f"({short_hash(str(original.get('author', '')))}):"
                    )
                    for line in str(original.get("text") or "").splitlines() or [""]:
                        print(f"  {line}")
            else:
                print(
                    f"  reposted {short_hash(str(post.get('target', '')))}"
                    " (original not cached yet)"
                )
        else:
            for line in str(post.get("text", "")).splitlines() or [""]:
                print(f"  {line}")
        if post.get("kind") == "quote":
            quoted = post.get("quoted")
            if isinstance(quoted, dict):
                if quoted.get("blocked"):
                    print("  quoting a blocked author's post")
                elif quoted.get("deleted"):
                    print("  quoting a deleted post")
                else:
                    print(f"  quoting {quoted.get('name') or 'anonymous'}:")
                    for line in str(quoted.get("text") or "").splitlines() or [""]:
                        print(f"    {line}")
            else:
                print(f"  quoting {short_hash(str(post.get('target', '')))}")
        if post.get("kind") == "poll":
            _print_poll_options(post)
        _print_tan_image(post)
        _print_tan_location(post)
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"  ♥ {likes}{suffix}")
        reposts = int(post.get("reposts") or 0)
        if reposts or post.get("reposted"):
            suffix = " (you reposted this)" if post.get("reposted") else ""
            print(f"  ↻ {reposts}{suffix}")
        quotes = int(post.get("quotes") or 0)
        if quotes:
            print(f"  ❝ {quotes}")


def _print_tan_thread(
    posts: Sequence[Dict[str, Any]],
    *,
    json_output: bool,
) -> None:
    """Print a thread as returned by :func:`tan.thread`, indented by depth."""
    if json_output:
        _emit({"posts": list(posts), "count": len(posts)}, True)
        return
    if not posts:
        print("No posts in this thread.")
        return
    for post in posts:
        indent = "  " * int(post.get("depth") or 0)
        name = post.get("name") or "anonymous"
        author = short_hash(str(post.get("author", "")))
        when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        print(
            f"{indent}[{when}] {name} ({author}) "
            f"{short_hash(str(post.get('hash', '')))}"
            f"{' (edited)' if post.get('edited') else ''}"
        )
        for line in str(post.get("text", "")).splitlines() or [""]:
            print(f"{indent}  {line}")
        if post.get("kind") == "quote":
            quoted = post.get("quoted")
            if isinstance(quoted, dict):
                if quoted.get("blocked"):
                    print(f"{indent}  quoting a blocked author's post")
                elif quoted.get("deleted"):
                    print(f"{indent}  quoting a deleted post")
                else:
                    print(f"{indent}  quoting {quoted.get('name') or 'anonymous'}:")
                    for line in str(quoted.get("text") or "").splitlines() or [""]:
                        print(f"{indent}    {line}")
            else:
                print(
                    f"{indent}  quoting "
                    f"{short_hash(str(post.get('target', '')))}"
                )
        if post.get("kind") == "poll":
            _print_poll_options(post, indent=f"{indent}  ")
        _print_tan_location(post, indent=f"{indent}  ")
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"{indent}  ♥ {likes}{suffix}")
        reposts = int(post.get("reposts") or 0)
        if reposts or post.get("reposted"):
            suffix = " (you reposted this)" if post.get("reposted") else ""
            print(f"{indent}  ↻ {reposts}{suffix}")
        quotes = int(post.get("quotes") or 0)
        if quotes:
            print(f"{indent}  ❝ {quotes}")


def _print_saved_posts(
    posts: Sequence[Dict[str, Any]],
    *,
    json_output: bool,
) -> None:
    """Print saved posts, newest save first."""
    if json_output:
        _emit({"saved": list(posts), "count": len(posts)}, True)
        return
    if not posts:
        print("No saved posts yet. Try `tau tan save <hash>`.")
        return
    for post in posts:
        name = post.get("name") or "anonymous"
        author = short_hash(str(post.get("author", "")))
        when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        saved = datetime.fromtimestamp(float(post.get("saved_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}{' (edited)' if post.get('edited') else ''}")
        print(f"  saved {saved}")
        if post.get("deleted"):
            print("  (deleted post)")
            continue
        for line in str(post.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
        if post.get("kind") == "poll":
            _print_poll_options(post)
        _print_tan_image(post)
        _print_tan_location(post)
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"  ♥ {likes}{suffix}")


def _print_hidden_posts(
    posts: Sequence[Dict[str, Any]],
    *,
    json_output: bool,
) -> None:
    """Print hidden posts, newest hide first."""
    if json_output:
        _emit({"hidden": list(posts), "count": len(posts)}, True)
        return
    if not posts:
        print("No hidden posts yet. Try `tau tan hide <hash>`.")
        return
    for post in posts:
        name = post.get("name") or "anonymous"
        author = short_hash(str(post.get("author", "")))
        when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        hidden = datetime.fromtimestamp(float(post.get("hidden_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}{' (edited)' if post.get('edited') else ''}")
        print(f"  hidden {hidden}")
        if post.get("deleted"):
            print("  (deleted post)")
            continue
        for line in str(post.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
        if post.get("kind") == "poll":
            _print_poll_options(post)


def _print_pinned_posts(
    posts: Sequence[Dict[str, Any]],
    *,
    json_output: bool,
) -> None:
    """Print pinned posts, newest pin first."""
    if json_output:
        _emit({"pinned": list(posts), "count": len(posts)}, True)
        return
    if not posts:
        print("No pinned posts yet. Try `tau tan pin <hash>` on one of your posts.")
        return
    for post in posts:
        name = post.get("name") or "anonymous"
        author = short_hash(str(post.get("author", "")))
        when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        pinned = datetime.fromtimestamp(float(post.get("pinned_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))} 📌")
        print(f"  pinned {pinned}")
        if post.get("deleted"):
            print("  (deleted post)")
            continue
        if post.get("kind") == "repost":
            original = post.get("repost_of")
            if isinstance(original, dict):
                if original.get("deleted"):
                    print("  reposted a deleted post")
                else:
                    print(
                        f"  reposted {original.get('name') or 'anonymous'} "
                        f"({short_hash(str(original.get('author', '')))}):"
                    )
                    for line in str(original.get("text") or "").splitlines() or [""]:
                        print(f"  {line}")
            else:
                print(
                    f"  reposted {short_hash(str(post.get('target', '')))}"
                    " (original not cached yet)"
                )
        else:
            for line in str(post.get("text", "")).splitlines() or [""]:
                print(f"  {line}")
        if post.get("kind") == "quote":
            quoted = post.get("quoted")
            if isinstance(quoted, dict):
                if quoted.get("blocked"):
                    print("  quoting a blocked author's post")
                elif quoted.get("deleted"):
                    print("  quoting a deleted post")
                else:
                    print(f"  quoting {quoted.get('name') or 'anonymous'}:")
                    for line in str(quoted.get("text") or "").splitlines() or [""]:
                        print(f"    {line}")
            else:
                print(f"  quoting {short_hash(str(post.get('target', '')))}")
        if post.get("kind") == "poll":
            _print_poll_options(post)
        _print_tan_image(post)
        _print_tan_location(post)
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"  ♥ {likes}{suffix}")


def _print_poll_options(
    post: Dict[str, Any], *, indent: str = "  "
) -> None:
    """Print a poll's options with their tallies, when known.

    Options come from the poll entry itself, so they print even where
    no tally was attached; counts print only when the ``poll``
    annotation is present.
    """
    options = _decode_options(post.get("options")) or []
    tally = post.get("poll") if isinstance(post.get("poll"), dict) else None
    counts = tally.get("counts") if tally else None
    voted = tally.get("voted") if tally else None
    for index, option in enumerate(options):
        line = f"{indent}{index + 1}. {option}"
        if counts is not None and 0 <= index < len(counts):
            votes = int(counts[index])
            line += f" — {votes} vote{'s' if votes != 1 else ''}"
            if voted is not None and voted == index:
                line += " (your vote)"
        print(line)
    if tally is not None:
        total = int(tally.get("total") or 0)
        print(f"{indent}{total} vote{'s' if total != 1 else ''} total")
        if tally.get("closed"):
            print(f"{indent}closed — no more votes")


def _print_tan_poll_results(result: Dict[str, Any], *, json_output: bool) -> None:
    """Print a poll plus its current tally, as returned by tan.poll_results."""
    poll = result["poll"]
    options = result["options"]
    counts = result["counts"]
    voters = result["voters"]
    if json_output:
        _emit(
            {
                "poll": poll,
                "options": options,
                "counts": counts,
                "total": result["total"],
                "closed": result["closed"],
                "voters": voters,
            },
            True,
        )
        return
    name = poll.get("name") or "anonymous"
    author = short_hash(str(poll.get("author", "")))
    when = datetime.fromtimestamp(float(poll.get("created_at", 0))).strftime(
        "%Y-%m-%d %H:%M"
    )
    print(f"[{when}] {name} ({author}) {short_hash(str(poll.get('hash', '')))}")
    for line in str(poll.get("text", "")).splitlines() or [""]:
        print(f"  {line}")
    if result.get("closed"):
        print("  closed — no more votes")
    by_option: Dict[int, List[Dict[str, Any]]] = {}
    for voter in voters:
        by_option.setdefault(int(voter["option"]), []).append(voter)
    for index, option in enumerate(options):
        votes = int(counts[index]) if index < len(counts) else 0
        print(f"  {index + 1}. {option} — {votes} vote{'s' if votes != 1 else ''}")
        for voter in by_option.get(index, []):
            voter_name = voter.get("name") or "anonymous"
            print(
                f"      {voter_name} ({short_hash(str(voter.get('author', '')))})"
            )
    if not voters:
        print("No votes yet.")


def _print_tan_likes(result: Dict[str, Any], *, json_output: bool) -> None:
    """Print a post plus its current likers, as returned by tan.likers."""
    post = result["post"]
    likers = result["likers"]
    if json_output:
        _emit({"post": post, "likers": likers, "count": len(likers)}, True)
        return
    name = post.get("name") or "anonymous"
    author = short_hash(str(post.get("author", "")))
    when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
        "%Y-%m-%d %H:%M"
    )
    print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}")
    for line in str(post.get("text", "")).splitlines() or [""]:
        print(f"  {line}")
    if not likers:
        print("No likes yet.")
        return
    print(f"♥ {len(likers)} like(s):")
    for liker in likers:
        liker_name = liker.get("name") or "anonymous"
        print(f"  {liker_name} ({short_hash(str(liker.get('author', '')))})")


def _print_tan_reposters(result: Dict[str, Any], *, json_output: bool) -> None:
    """Print a post plus its current reposters, as returned by tan.reposters."""
    post = result["post"]
    reposters = result["reposters"]
    if json_output:
        _emit({"post": post, "reposters": reposters, "count": len(reposters)}, True)
        return
    name = post.get("name") or "anonymous"
    author = short_hash(str(post.get("author", "")))
    when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
        "%Y-%m-%d %H:%M"
    )
    print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}")
    for line in str(post.get("text", "")).splitlines() or [""]:
        print(f"  {line}")
    if not reposters:
        print("No reposts yet.")
        return
    print(f"↻ {len(reposters)} repost(s):")
    for reposter in reposters:
        reposter_name = reposter.get("name") or "anonymous"
        print(f"  {reposter_name} ({short_hash(str(reposter.get('author', '')))})")


def _print_tan_quoters(result: Dict[str, Any], *, json_output: bool) -> None:
    """Print a post plus its standing quotes, as returned by tan.quoters."""
    post = result["post"]
    quoters = result["quoters"]
    if json_output:
        _emit({"post": post, "quoters": quoters, "count": len(quoters)}, True)
        return
    name = post.get("name") or "anonymous"
    author = short_hash(str(post.get("author", "")))
    when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
        "%Y-%m-%d %H:%M"
    )
    print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}")
    for line in str(post.get("text", "")).splitlines() or [""]:
        print(f"  {line}")
    if not quoters:
        print("No quotes yet.")
        return
    print(f"❝ {len(quoters)} quote(s):")
    for quoter in quoters:
        quoter_name = quoter.get("name") or "anonymous"
        quoted_when = datetime.fromtimestamp(
            float(quoter.get("quoted_at", 0))
        ).strftime("%Y-%m-%d %H:%M")
        print(
            f"  [{quoted_when}] {quoter_name} "
            f"({short_hash(str(quoter.get('author', '')))}) "
            f"{short_hash(str(quoter.get('hash', '')))}"
        )
        for line in str(quoter.get("text", "")).splitlines() or [""]:
            print(f"    {line}")


def _tan_identity(config: TauConfig) -> Any:
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    return identity


def _publish_local_channel_message(
    node: TauNode,
    *,
    channel: str,
    text: Optional[str] = None,
    reply_to: Optional[str] = None,
    to: Optional[List[str]] = None,
    ttl: int = MAX_GOSSIP_TTL,
    timeout: float = 15.0,
    expires_at: Optional[float] = None,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    # Use a separate app so per-command timeouts do not mutate the serving app.
    publisher = ChannelApp(
        node.config, node.identity, name=node.name, request_timeout=float(timeout)
    )
    publisher.node = node
    targets = [node.resolve_peer(target) for target in to] if to else None
    if text is None:
        raise ChannelError("nothing to publish")
    name = clean_channel_name(channel)
    # Creation goes through app.send so a per-channel disappearing
    # default fills the expiry in when no explicit one was passed —
    # the same path the no-publish and console sends take.
    message = publisher.send(
        name,
        text,
        expires_at=expires_at,
        poll_options=poll_options,
        poll_vote=poll_vote,
        poll_close=poll_close,
        reply_to=reply_to,
        location=location,
    )
    published = publisher.publish(
        message, targets=targets, ttl=max(0, min(int(ttl), MAX_GOSSIP_TTL))
    )
    return {"message": message, "published": published}


def _publish_local_channel_topic(
    node: TauNode,
    *,
    channel: str,
    topic: Optional[str] = None,
    to: Optional[List[str]] = None,
    ttl: int = MAX_GOSSIP_TTL,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    # Use a separate app so per-command timeouts do not mutate the serving app.
    publisher = ChannelApp(
        node.config, node.identity, name=node.name, request_timeout=float(timeout)
    )
    publisher.node = node
    targets = [node.resolve_peer(target) for target in to] if to else None
    if topic is None:
        raise ChannelError("nothing to publish")
    return publisher.set_topic(
        channel,
        topic,
        publish=True,
        targets=targets,
        ttl=max(0, min(int(ttl), MAX_GOSSIP_TTL)),
    )


def _publish_local_channel_reaction(
    node: TauNode,
    *,
    channel: str,
    target: str,
    reaction: str,
    to: Optional[List[str]] = None,
    ttl: int = MAX_GOSSIP_TTL,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    # Use a separate app so per-command timeouts do not mutate the serving app.
    publisher = ChannelApp(
        node.config, node.identity, name=node.name, request_timeout=float(timeout)
    )
    publisher.node = node
    targets = [node.resolve_peer(target) for target in to] if to else None
    name = clean_channel_name(channel)
    conn = publisher.connect()
    try:
        if not is_channel_member(conn, name):
            raise ChannelError(f"not a member of channel {name!r}; join it first")
        record = create_channel_reaction(
            conn, node.identity, name, target, reaction, name=node.name
        )
    finally:
        conn.close()
    published = publisher.publish_reaction(
        record, targets=targets, ttl=max(0, min(int(ttl), MAX_GOSSIP_TTL))
    )
    return {"reaction": record, "published": published}


def _publish_local_channel_edit(
    node: TauNode,
    *,
    channel: str,
    target: str,
    text: Optional[str] = None,
    to: Optional[List[str]] = None,
    ttl: int = MAX_GOSSIP_TTL,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    # Use a separate app so per-command timeouts do not mutate the serving app.
    publisher = ChannelApp(
        node.config, node.identity, name=node.name, request_timeout=float(timeout)
    )
    publisher.node = node
    targets = [node.resolve_peer(target) for target in to] if to else None
    if text is None:
        raise ChannelError("nothing to publish")
    name = clean_channel_name(channel)
    conn = publisher.connect()
    try:
        if not is_channel_member(conn, name):
            raise ChannelError(f"not a member of channel {name!r}; join it first")
        notice = create_channel_edit(conn, node.identity, name, target, text)
    finally:
        conn.close()
    published = publisher.publish_edit(
        notice, targets=targets, ttl=max(0, min(int(ttl), MAX_GOSSIP_TTL))
    )
    return {"edit": notice, "published": published}


def _publish_local_channel_retraction(
    node: TauNode,
    *,
    channel: str,
    digest: str,
    to: Optional[List[str]] = None,
    ttl: int = MAX_GOSSIP_TTL,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    # Use a separate app so per-command timeouts do not mutate the serving app.
    publisher = ChannelApp(
        node.config, node.identity, name=node.name, request_timeout=float(timeout)
    )
    publisher.node = node
    targets = [node.resolve_peer(target) for target in to] if to else None
    name = clean_channel_name(channel)
    return publisher.retract(
        name,
        digest,
        publish=True,
        targets=targets,
        ttl=max(0, min(int(ttl), MAX_GOSSIP_TTL)),
    )


def _publish_local_post(
    node: TauNode,
    *,
    text: Optional[str] = None,
    post_hash: Optional[str] = None,
    reply_to: Optional[str] = None,
    quote_of: Optional[str] = None,
    to: Optional[List[str]] = None,
    ttl: int = MAX_GOSSIP_TTL,
    timeout: float = 15.0,
    expires_at: Optional[float] = None,
    image: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
    channel: Optional[str] = None,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
    topic: Optional[str] = None,
    reaction_target: Optional[str] = None,
    reaction: Optional[str] = None,
    channel_edit: Optional[Dict[str, Any]] = None,
    retract_digest: Optional[str] = None,
) -> Dict[str, Any]:
    if channel_edit is not None:
        # A channel edit rides the same local publish socket as a Tan
        # post; route it before the Tan-only handling below.
        return _publish_local_channel_edit(
            node,
            channel=str(channel_edit.get("channel", "")),
            target=str(channel_edit.get("target", "")),
            text=channel_edit.get("text"),
            to=to,
            ttl=ttl,
            timeout=timeout,
        )
    if retract_digest is not None:
        # A channel retract rides the same local publish socket as a
        # channel send; route it before the Tan-only handling below.
        if channel is None:
            raise ChannelError("a channel is required to retract a message")
        return _publish_local_channel_retraction(
            node,
            channel=channel,
            digest=retract_digest,
            to=to,
            ttl=ttl,
            timeout=timeout,
        )
    if channel is not None:
        if topic is not None:
            # A channel topic rides the same local publish socket as
            # a channel message; route it before the Tan handling.
            return _publish_local_channel_topic(
                node,
                channel=channel,
                topic=topic,
                to=to,
                ttl=ttl,
                timeout=timeout,
            )
        if reaction_target is not None:
            # A channel reaction rides the same local publish socket
            # as a Tan post; route it before the Tan-only handling.
            return _publish_local_channel_reaction(
                node,
                channel=channel,
                target=reaction_target,
                reaction=reaction if reaction is not None else "",
                to=to,
                ttl=ttl,
                timeout=timeout,
            )
        # A channel send rides the same local publish socket as a Tan
        # post; route it before the Tan-only handling below.  A poll,
        # vote or close rides it the same way, as poll fields on the
        # channel message.  The channel reply target rides along: it is
        # a channel message hash, not a Tan post hash, so it must not
        # reach create_post.
        return _publish_local_channel_message(
            node,
            channel=channel,
            text=text,
            reply_to=reply_to,
            to=to,
            ttl=ttl,
            timeout=timeout,
            expires_at=expires_at,
            poll_options=poll_options,
            poll_vote=poll_vote,
            poll_close=poll_close,
            location=location,
        )
    # Use a separate app so per-command timeouts do not mutate the serving app.
    publisher = TanApp(
        node.config, node.identity, name=node.name, request_timeout=float(timeout)
    )
    publisher.node = node
    targets = [node.resolve_peer(target) for target in to] if to else None
    if post_hash is not None:
        # Re-deliver a post that is already signed and stored — a
        # draft retry, for example — so the same post goes out again
        # instead of a second copy being signed for the same text.
        stored = publisher.get_post(post_hash)
        if stored is None:
            raise TanError(f"unknown post: {post_hash!r}")
        # get_post returns the canonical post plus storage-only
        # "seq"/"linked" fields; publish() encodes the dict
        # unchanged, so strip them — _check_wire_size only
        # measures the canonical post.
        post = {k: v for k, v in stored.items() if k not in ("seq", "linked")}
    elif quote_of is not None:
        if text is None:
            raise TanError("nothing to publish")
        post = publisher.quote(quote_of, text)
    else:
        if text is None:
            raise TanError("nothing to publish")
        post = publisher.create_post(
            text, reply_to=reply_to, expires_at=expires_at, image=image,
            location=location,
        )
    published = publisher.publish(
        post, targets=targets, ttl=max(0, min(int(ttl), MAX_GOSSIP_TTL))
    )
    return {"post": post, "published": published}


def cmd_tan_post(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)

    reply_to = None
    if getattr(args, "reply_to", None):
        conn = init_db(config.social_db_path)
        try:
            reply_to = resolve_post(conn, args.reply_to)["hash"]
        finally:
            conn.close()

    expire_seconds = 0.0
    expire_text = getattr(args, "expire", None)
    if expire_text is not None:
        try:
            expire_seconds = _parse_duration(
                expire_text,
                flag="--expire",
                max_seconds=MAX_TAN_POST_EXPIRY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    expires_at = time.time() + expire_seconds if expire_text is not None else None

    image = None
    if getattr(args, "image", None) is not None:
        if reply_to is not None:
            print(
                "error: --image can only be attached to a top-level post, "
                "not a reply",
                file=sys.stderr,
            )
            return 2
        image = _read_image(args.image, config.data_dir)

    location = None
    if getattr(args, "location", None) is not None:
        if reply_to is not None:
            print(
                "error: --location can only be attached to a top-level post, "
                "not a reply",
                file=sys.stderr,
            )
            return 2
        if getattr(args, "delay", None) is not None:
            print(
                "error: --location cannot be combined with --delay — "
                "scheduled posts cannot carry an attachment yet",
                file=sys.stderr,
            )
            return 2
        location = _location_from_args(args)

    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        if getattr(args, "no_publish", False):
            print(
                "error: --delay cannot be combined with --no-publish — "
                "a scheduled post is published later, not stored only",
                file=sys.stderr,
            )
            return 2
        try:
            delay = _parse_duration(
                delay_text,
                flag="--delay",
                max_seconds=MAX_SCHEDULED_POST_DELAY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        if expires_at is not None and delay >= expire_seconds:
            print(
                "error: --delay must be shorter than --expire — the "
                "post would expire before it is published",
                file=sys.stderr,
            )
            return 2
        try:
            post = schedule_post(
                config.scheduled_posts_path,
                text=text,
                publish_at=time.time() + delay,
                reply_to=reply_to,
                to=getattr(args, "to", None),
                ttl=int(getattr(args, "ttl", MAX_GOSSIP_TTL)),
                name=config.name,
                expires_in=expire_seconds,
                image=image,
            )
        except ScheduledPostsError as exc:
            raise NodeError(str(exc)) from exc
        if json_output:
            _emit({"scheduled": post.to_dict()}, True)
        else:
            print(f"scheduled {post.id} — publishes in {_ago(delay)}")
            print("  list it with `tau tan scheduled`; a node serving")
            print("  with --social publishes it when the time comes.")
        return _EXIT_OK

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        post = app.create_post(
            text, reply_to=reply_to, expires_at=expires_at, image=image,
            location=location,
        )
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            from .local_publish import publish_running

            # The image rides the IPC request only when there is one:
            # a plain post must still publish through a running node
            # from before this flag existed.
            request = dict(
                text=text,
                reply_to=reply_to,
                to=getattr(args, "to", None),
                ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                timeout=float(getattr(args, "timeout", 15.0)),
                expires_at=expires_at,
            )
            if image is not None:
                request["image"] = image
            if location is not None:
                request["location"] = location
            result = publish_running(config.data_dir, **request)
            post, published = result["post"], result["published"]
        else:
            try:
                app = TanApp(
                    config,
                    node.identity,
                    name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                node.attach_app(app)
                result = _publish_local_post(
                    node,
                    text=text,
                    reply_to=reply_to,
                    to=getattr(args, "to", None),
                    ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    timeout=float(getattr(args, "timeout", 15.0)),
                    expires_at=expires_at,
                    image=image,
                    location=location,
                )
                post, published = result["post"], result["published"]
            finally:
                node.stop()

    # Publish-draft reads these back: a draft is only consumed once
    # every requested peer accepted the post.  It is None for
    # --no-publish, where there is no delivery to check.  The hash
    # lets a kept draft re-send the same signed post on retry
    # instead of signing a second copy.
    args._tan_post_published = published
    args._tan_post_hash = post["hash"]

    if json_output:
        _emit({"post": post, "published": published}, True)
    else:
        verb = "replied" if reply_to is not None else "posted"
        print(f"{verb} {short_hash(post['hash'])}")
        if reply_to is not None:
            print(f"  in reply to {short_hash(reply_to)}")
        if expires_at is not None:
            print(f"  expires in {_ago(expire_seconds)}")
        image = post.get("image")
        if isinstance(image, dict):
            size = int(image.get("size") or 0)
            size_text = f"{size / 1024:.1f} KiB" if size >= 1024 else f"{size} B"
            print(f"  image {image.get('name')} ({size_text}, {image.get('mime')})")
        location = clean_location(post.get("location"))
        if location is not None:
            name = location.get("name")
            label = name if isinstance(name, str) and name else "Location"
            print(f"  📍 {label} — {location['lat']}, {location['lon']}")
            print(f"  {location_maps_url(location)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_media(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    digest = str(args.sha256).strip().lower()
    if len(digest) != 64:
        print("error: sha256 must be a 64 character hex digest", file=sys.stderr)
        return 2
    try:
        bytes.fromhex(digest)
    except ValueError:
        print("error: sha256 must be a 64 character hex digest", file=sys.stderr)
        return 2
    timeout = float(getattr(args, "timeout", 60.0))
    cached = media_path(config.data_dir, digest)
    if cached is not None:
        result = {
            "ok": True,
            "sha256": digest,
            "path": str(cached),
            "size": cached.stat().st_size,
            "cached": True,
        }
    else:
        author = getattr(args, "author", None)
        if author is None:
            # Learn the author from the local post carrying this image.
            conn = init_db(config.social_db_path)
            try:
                post = find_post_by_image(conn, digest)
            finally:
                conn.close()
            if post is None:
                print(
                    "error: no local post carries that image — "
                    "pass --author <hash>",
                    file=sys.stderr,
                )
                return 2
            author = post["author"]
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            from .local_media import fetch_running

            result = fetch_running(
                config.data_dir, author=author, sha256=digest, timeout=timeout
            )
        else:
            try:
                result = node.fetch_tan_media(
                    author, digest, timeout=timeout
                )
            finally:
                node.stop()
    out_path = getattr(args, "out", None)
    if out_path is not None:
        target = Path(out_path).expanduser()
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(result["path"], target)
        except OSError as exc:
            print(f"error: could not copy to {target}: {exc}", file=sys.stderr)
            return 2
        result = dict(result, path=str(target))
    if json_output:
        _emit(result, True)
    else:
        size = int(result.get("size") or 0)
        size_text = f"{size / 1024:.1f} KiB" if size >= 1024 else f"{size} B"
        cached_text = " (already cached)" if result.get("cached") else ""
        print(f"image {digest[:12]} ({size_text}) saved to {result['path']}{cached_text}")
    return _EXIT_OK


def cmd_tan_draft(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)

    reply_to = ""
    if getattr(args, "reply_to", None):
        conn = init_db(config.social_db_path)
        try:
            reply_to = resolve_post(conn, args.reply_to)["hash"]
        finally:
            conn.close()

    expires_in = None
    expire_text = getattr(args, "expire", None)
    if expire_text is not None:
        try:
            expires_in = _parse_duration(
                expire_text,
                flag="--expire",
                max_seconds=MAX_TAN_POST_EXPIRY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

    conn = init_db(config.social_db_path)
    try:
        draft = save_draft(
            conn, text, reply_to=reply_to, expires_in=expires_in
        )
    finally:
        conn.close()
    if json_output:
        _emit({"draft": draft}, True)
    else:
        print(
            f"draft {draft['id']} saved — publish it with "
            f"`tau tan publish-draft {draft['id']}`"
        )
        if reply_to:
            print(f"  in reply to {short_hash(reply_to)}")
        if expires_in is not None:
            print(f"  expires {_ago(expires_in)} after publishing")
    return _EXIT_OK


def cmd_tan_drafts(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        drafts = list_drafts(conn, limit=getattr(args, "limit", DEFAULT_FEED_LIMIT))
    finally:
        conn.close()
    if json_output:
        _emit({"drafts": drafts}, True)
        return _EXIT_OK
    if not drafts:
        print("No Tan post drafts saved.")
        return _EXIT_OK
    for draft in drafts:
        print(f"{draft['id']}  {_stamp(draft['created_at'])}")
        if draft["claimed_at"] is not None and draft["claimed_at"] > (
            time.time() - DRAFT_CLAIM_TTL
        ):
            # A live claim means another publish-draft is working on
            # this draft right now; a stale one is just a crashed
            # publisher's leftover and is not shown as in progress.
            print("  publishing…")
        if draft["reply_to"]:
            print(f"  in reply to {short_hash(draft['reply_to'])}")
        if draft["expires_in"] is not None:
            print(f"  expires {_ago(draft['expires_in'])} after publishing")
        if draft["post_hash"]:
            owed = len(draft["pending_peers"])
            suffix = f" to {owed} peer(s)" if owed else ""
            print(
                f"  post {short_hash(draft['post_hash'])} — retry resends it{suffix}"
            )
        for line in draft["text"].splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK


def _pending_publish_peers(published: Dict[str, Any]) -> List[str]:
    """Peer hashes still owed a post: failed sends plus never-attempted ones."""
    pending = [str(err.get("peer") or "") for err in published.get("errors") or []]
    pending.extend(str(item) for item in published.get("unsent") or [])
    return [peer for peer in dict.fromkeys(pending) if peer and peer != "?"]


def _deliver_stored_post(
    config: TauConfig,
    post: Dict[str, Any],
    *,
    to: Optional[List[str]],
    ttl: int,
    timeout: float,
) -> Dict[str, Any]:
    """Deliver an already-signed post through a fresh node or the running one.

    Mirrors the publish half of ``cmd_tan_post`` but takes the post
    instead of signing a new one, so a retried ``publish-draft``
    re-sends the same post rather than signing a second copy.
    """
    request = {
        "post_hash": post["hash"],
        "to": to,
        "ttl": ttl,
        "timeout": timeout,
    }
    node = TauNode(config, name=config.name)
    try:
        node.start(exclusive=True)
    except InstanceAlreadyRunning:
        from .local_publish import publish_running

        return publish_running(config.data_dir, **request)
    else:
        try:
            return _publish_local_post(node, **request)
        finally:
            node.stop()


def _draft_claim_held(config: TauConfig, draft_id: int, token: str) -> bool:
    """True when *token* still owns the draft's live publish claim."""
    conn = init_db(config.social_db_path)
    try:
        return draft_claim_held(conn, draft_id, token, ttl=DRAFT_CLAIM_TTL)
    finally:
        conn.close()


def _draft_claim_lost(draft_id: int) -> int:
    print(
        f"error: draft {draft_id} publish claim lost — leaving the draft "
        "for its new publisher",
        file=sys.stderr,
    )
    return _EXIT_ERROR


def _refresh_draft_claim_loop(
    config: TauConfig,
    draft_id: int,
    token: str,
    stop: threading.Event,
) -> None:
    """Keep a draft's publish claim alive until *stop* is set.

    A slow publish — a long --timeout, sequential peer requests —
    must never let its own claim go stale, or a second run could
    take the draft over and sign a duplicate post while the first
    run is still delivering.  Refreshes at a third of the TTL, on
    its own connection so the publishing thread is never blocked.
    Stops quietly when the claim is gone: another publisher owns
    the draft now, and only the fencing checks in the publish path
    still apply.
    """
    conn = init_db(config.social_db_path)
    try:
        interval = max(0.05, DRAFT_CLAIM_TTL / 3)
        while not stop.wait(interval):
            try:
                if not refresh_draft_claim(conn, draft_id, token):
                    return
            except Exception:  # pragma: no cover - transient DB errors
                log.debug("draft claim refresh failed", exc_info=True)
    finally:
        conn.close()


def _retry_draft_post(
    args: argparse.Namespace,
    config: TauConfig,
    draft: Dict[str, Any],
    token: str,
) -> int:
    """Re-send a draft's already-signed post to the peers that missed it."""
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        stored = resolve_post(conn, draft["post_hash"])
    except TanError as exc:
        # The post is gone, deleted or expired: signing a fresh one
        # would hand the peers that accepted the first a duplicate,
        # so keep the draft for the author to discard by hand.
        print(
            f"error: draft {draft['id']} kept — {exc}; the draft was not deleted",
            file=sys.stderr,
        )
        return _EXIT_ERROR
    finally:
        conn.close()
    post = {k: v for k, v in stored.items() if k not in ("seq", "linked")}
    if getattr(args, "no_publish", False):
        # The post is already stored locally; there is nothing to send.
        if not _draft_claim_held(config, draft["id"], token):
            return _draft_claim_lost(draft["id"])
        conn = init_db(config.social_db_path)
        try:
            delete_draft(conn, draft["id"])
        finally:
            conn.close()
        if json_output:
            _emit({"post": post, "published": None}, True)
        else:
            print(f"stored locally (not published) {short_hash(post['hash'])}")
        return _EXIT_OK
    # An explicit --to joins the peers already owed the post instead
    # of replacing them: a retry that only reached its new peer
    # would otherwise drop the original peers from the draft.
    explicit = getattr(args, "to", None) or []
    targets = list(dict.fromkeys([*(draft["pending_peers"] or []), *explicit])) or None
    result = _deliver_stored_post(
        config,
        post,
        to=targets,
        ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
        timeout=float(getattr(args, "timeout", 15.0)),
    )
    published = result["published"]
    if json_output:
        _emit({"post": post, "published": published}, True)
    else:
        print(f"published {short_hash(post['hash'])}")
        print(f"  published to {published['delivered']}/{published['peers']} peer(s)")
        for error in published["errors"]:
            print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    if published["errors"]:
        if not _draft_claim_held(config, draft["id"], token):
            # Another publisher owns the draft now: leave it — and
            # its post association — for that publisher to retry.
            return _draft_claim_lost(draft["id"])
        conn = init_db(config.social_db_path)
        try:
            record_draft_publish(
                conn,
                draft["id"],
                post_hash=post["hash"],
                pending_peers=_pending_publish_peers(published),
            )
        finally:
            conn.close()
        print(
            f"error: draft {draft['id']} kept — delivered to "
            f"{published['delivered']}/{published['peers']} peer(s); "
            "the draft was not deleted",
            file=sys.stderr,
        )
        return _EXIT_ERROR
    if not _draft_claim_held(config, draft["id"], token):
        return _draft_claim_lost(draft["id"])
    conn = init_db(config.social_db_path)
    try:
        delete_draft(conn, draft["id"])
    finally:
        conn.close()
    return _EXIT_OK


def _publish_draft_claimed(
    args: argparse.Namespace,
    config: TauConfig,
    draft: Dict[str, Any],
    token: str,
) -> int:
    if draft["post_hash"]:
        # A previous attempt already signed this post: re-send that
        # post to the peers that missed it, never a second copy.
        return _retry_draft_post(args, config, draft, token)
    # Publish through the normal post path, so signing, storing and
    # delivery are identical for drafts and fresh posts.  The draft's
    # expiry is a duration from publish time, so it is handed over as
    # a fresh --expire value, not as the absolute time it would have
    # been had the post gone out when the draft was saved.
    args.text = draft["text"]
    args.file = None
    args.reply_to = draft["reply_to"] or None
    args.expire = (
        str(draft["expires_in"]) if draft["expires_in"] is not None else None
    )
    args.delay = None
    # The sign happens inside cmd_tan_post: re-check the lease
    # immediately before it, so a publisher that lost its claim
    # while loading the draft can never sign a second copy.
    if not _draft_claim_held(config, draft["id"], token):
        return _draft_claim_lost(draft["id"])
    result = cmd_tan_post(args)
    if result != _EXIT_OK:
        # A failed publish keeps the draft, so it can be retried or
        # discarded by hand instead of silently lost.
        return result
    # cmd_tan_post reports peer failures without failing, so check the
    # delivery result too: a draft is only consumed once every requested
    # peer accepted the post. --no-publish leaves no delivery to check.
    published = getattr(args, "_tan_post_published", None)
    if published is not None and published["errors"]:
        # Remember the signed post and who still owes it, so a retry
        # re-sends that post instead of signing a second copy.
        post_hash = getattr(args, "_tan_post_hash", "")
        if not _draft_claim_held(config, draft["id"], token):
            return _draft_claim_lost(draft["id"])
        if post_hash:
            conn = init_db(config.social_db_path)
            try:
                record_draft_publish(
                    conn,
                    draft["id"],
                    post_hash=post_hash,
                    pending_peers=_pending_publish_peers(published),
                )
            finally:
                conn.close()
        print(
            f"error: draft {draft['id']} kept — delivered to "
            f"{published['delivered']}/{published['peers']} peer(s); "
            "the draft was not deleted",
            file=sys.stderr,
        )
        return _EXIT_ERROR
    if not _draft_claim_held(config, draft["id"], token):
        return _draft_claim_lost(draft["id"])
    conn = init_db(config.social_db_path)
    try:
        delete_draft(conn, draft["id"])
    finally:
        conn.close()
    return _EXIT_OK


def cmd_tan_publish_draft(args: argparse.Namespace) -> int:
    """Publish a draft, holding its claim for the whole attempt.

    The claim is taken before anything is signed or sent, so a
    second concurrent ``publish-draft`` for the same draft loses the
    claim and publishes nothing.  A background thread renews the
    claim while the publish runs, so a slow publish never lets its
    own claim go stale for a successor to take over; the publish
    path also re-checks the claim before signing and before
    deleting the draft, so a publisher that lost its lease stops
    touching the draft.  The claim is released whenever the draft
    survives the attempt — a failed publish, a partial delivery —
    so a retry is never blocked by this run's own claim; a
    successful publish deletes the draft, making the release a
    no-op.  A claim a crashed publisher left behind goes stale
    after ``DRAFT_CLAIM_TTL`` and can be taken over.
    """
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        draft = get_draft(conn, args.draft)
        token = claim_draft(conn, draft["id"], ttl=DRAFT_CLAIM_TTL)
        if token is not None:
            # Re-read after winning the claim: a publisher that
            # finished between the first read and the claim may have
            # recorded a signed post, which this run must retry
            # instead of re-signing.
            draft = get_draft(conn, draft["id"])
    finally:
        conn.close()
    if token is None:
        print(
            f"error: draft {draft['id']} is already being published — "
            "try again when it finishes",
            file=sys.stderr,
        )
        return _EXIT_ERROR
    stop = threading.Event()
    refresher = threading.Thread(
        target=_refresh_draft_claim_loop,
        args=(config, draft["id"], token, stop),
        daemon=True,
    )
    refresher.start()
    try:
        return _publish_draft_claimed(args, config, draft, token)
    finally:
        stop.set()
        refresher.join(timeout=5)
        conn = init_db(config.social_db_path)
        try:
            release_draft_claim(conn, draft["id"], token)
        finally:
            conn.close()


def cmd_tan_discard_draft(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        draft = delete_draft(conn, args.draft)
    finally:
        conn.close()
    if json_output:
        _emit({"discarded": draft}, True)
    else:
        print(f"discarded draft {draft['id']}")
    return _EXIT_OK


def cmd_tan_scheduled(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        queue = load_scheduled_posts(config.scheduled_posts_path)
    except ScheduledPostsError as exc:
        raise NodeError(str(exc)) from exc
    if json_output:
        _emit({"scheduled": [post.to_dict() for post in queue.posts]}, True)
        return _EXIT_OK
    if not queue.posts:
        print("No Tan posts, reposts, quotes or polls scheduled.")
        return _EXIT_OK
    now = time.time()
    for post in queue.posts:
        when = _stamp(post.publish_at)
        if post.publish_at <= now:
            when += " (due — publishes on the next serve pass)"
        else:
            when += f" (in {_ago(post.publish_at - now)})"
        print(f"{post.id}  {when}")
        if post.kind == "repost":
            print(f"  repost of {short_hash(post.target_post)}")
        elif post.kind == "quote":
            print(f"  quote of {short_hash(post.target_post)}")
        elif post.kind == "poll":
            print(f"  poll · {len(post.options)} options")
        if post.reply_to is not None:
            print(f"  in reply to {short_hash(post.reply_to)}")
        if post.to:
            print(f"  to {', '.join(post.to)}")
        if post.image is not None:
            print(f"  image {post.image['name']} ({post.image['size']} bytes)")
        if post.text:
            for line in post.text.splitlines() or [""]:
                print(f"  {line}")
        if post.kind == "poll":
            for index, option in enumerate(post.options, start=1):
                print(f"    {index}. {option}")
    return _EXIT_OK


def cmd_tan_cancel_scheduled(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    deleted_local = False

    def _delete_stored_post(post: Any) -> None:
        # A publish attempt already stored the signed post locally
        # (see publish_due_scheduled_posts): dropping only the queue
        # entry would leave it in the local timeline.  Remove it with
        # the same local-only delete `tau tan delete --no-publish`
        # writes.  The post stays stored — Tan deletes are markers, so
        # the chain stays verifiable and sync keeps carrying the post
        # together with its delete entry, which is how a peer that
        # syncs afterwards learns to hide it.  Peers that received a
        # copy before the failure keep theirs — a local delete cannot
        # retract what already went out.
        nonlocal deleted_local
        if post.post_hash is None:
            return
        app = TanApp(config, _tan_identity(config), name=config.name)
        try:
            app.delete(post.post_hash)
        except TanError as exc:
            if "has been deleted" not in str(exc):
                raise NodeError(str(exc)) from exc
        else:
            deleted_local = True

    try:
        post = cancel_scheduled_post(
            config.scheduled_posts_path, args.target, on_cancel=_delete_stored_post
        )
    except ScheduledPostsError as exc:
        raise NodeError(str(exc)) from exc
    if json_output:
        _emit(
            {"cancelled": post.to_dict(), "deleted_local": deleted_local},
            True,
        )
    else:
        print(f"cancelled {post.id} — it will not be published")
        if deleted_local:
            print("  also removed the stored post from the local feed")
    return _EXIT_OK


def cmd_tan_edit_scheduled(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    if getattr(args, "text", None) is None and getattr(args, "delay", None) is None:
        print(
            "error: nothing to change — pass --text and/or --delay",
            file=sys.stderr,
        )
        return 2
    publish_at = None
    if getattr(args, "delay", None) is not None:
        try:
            delay = _parse_duration(
                args.delay,
                flag="--delay",
                max_seconds=MAX_SCHEDULED_POST_DELAY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        publish_at = time.time() + delay
    from .scheduled_posts import edit_scheduled_post

    try:
        post = edit_scheduled_post(
            config.scheduled_posts_path,
            args.target,
            text=getattr(args, "text", None),
            publish_at=publish_at,
            name=config.name,
        )
    except ScheduledPostsError as exc:
        raise NodeError(str(exc)) from exc
    if json_output:
        _emit({"edited": post.to_dict()}, True)
    else:
        print(f"edited {post.id} — publishes {_stamp(post.publish_at)}")
        for line in post.text.splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK


def cmd_tan_quote(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_post(conn, args.target)["hash"]
    finally:
        conn.close()

    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        if getattr(args, "no_publish", False):
            print(
                "error: --delay cannot be combined with --no-publish — "
                "a scheduled quote is published later, not stored only",
                file=sys.stderr,
            )
            return 2
        try:
            delay = _parse_duration(
                delay_text,
                flag="--delay",
                max_seconds=MAX_SCHEDULED_POST_DELAY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        try:
            scheduled = schedule_quote(
                config.scheduled_posts_path,
                target_post=target,
                text=text,
                publish_at=time.time() + delay,
                to=getattr(args, "to", None),
                ttl=int(getattr(args, "ttl", MAX_GOSSIP_TTL)),
                name=config.name,
            )
        except ScheduledPostsError as exc:
            raise NodeError(str(exc)) from exc
        if json_output:
            _emit({"scheduled": scheduled.to_dict()}, True)
        else:
            print(
                f"scheduled {scheduled.id} — quotes {short_hash(target)} "
                f"in {_ago(delay)}"
            )
            print("  list it with `tau tan scheduled`; a node serving")
            print("  with --social publishes it when the time comes.")
        return _EXIT_OK

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        post = app.quote(target, text)
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            from .local_publish import publish_running

            result = publish_running(
                config.data_dir, text=text, quote_of=target,
                to=getattr(args, "to", None),
                ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            post, published = result["post"], result["published"]
        else:
            try:
                app = TanApp(
                    config, node.identity, name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                node.attach_app(app)
                result = _publish_local_post(
                    node, text=text, quote_of=target,
                    to=getattr(args, "to", None),
                    ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    timeout=float(getattr(args, "timeout", 15.0)),
                )
                post, published = result["post"], result["published"]
            finally:
                node.stop()

    if json_output:
        _emit({"post": post, "published": published}, True)
    else:
        print(f"quoted {short_hash(post['hash'])}")
        print(f"  quoting {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def _cmd_tan_react(args: argparse.Namespace, *, liked: bool) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_post(conn, args.target)["hash"]
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.react(target, liked=liked)
        published = None
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            app = TanApp(
                config,
                node.identity,
                name=config.name,
                request_timeout=float(getattr(args, "timeout", 15.0)),
            )
            node.attach_app(app)
            targets = None
            if getattr(args, "to", None):
                targets = [node.resolve_peer(target) for target in args.to]
            record = app.react(target, liked=liked)
            published = app.publish(
                record,
                targets=targets,
                ttl=max(
                    0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)
                ),
            )
        finally:
            node.stop()

    if json_output:
        _emit({"reaction": record, "published": published}, True)
    else:
        verb = "liked" if liked else "unliked"
        print(f"{verb} {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_like(args: argparse.Namespace) -> int:
    return _cmd_tan_react(args, liked=True)


def cmd_tan_unlike(args: argparse.Namespace) -> int:
    return _cmd_tan_react(args, liked=False)


def cmd_tan_repost(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_post(conn, args.target)["hash"]
    finally:
        conn.close()

    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        if getattr(args, "no_publish", False):
            print(
                "error: --delay cannot be combined with --no-publish — "
                "a scheduled repost is published later, not stored only",
                file=sys.stderr,
            )
            return 2
        try:
            delay = _parse_duration(
                delay_text,
                flag="--delay",
                max_seconds=MAX_SCHEDULED_POST_DELAY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        try:
            scheduled = schedule_repost(
                config.scheduled_posts_path,
                target_post=target,
                publish_at=time.time() + delay,
                to=getattr(args, "to", None),
                ttl=int(getattr(args, "ttl", MAX_GOSSIP_TTL)),
                name=config.name,
            )
        except ScheduledPostsError as exc:
            raise NodeError(str(exc)) from exc
        if json_output:
            _emit({"scheduled": scheduled.to_dict()}, True)
        else:
            print(
                f"scheduled {scheduled.id} — reposts {short_hash(target)} "
                f"in {_ago(delay)}"
            )
            print("  list it with `tau tan scheduled`; a node serving")
            print("  with --social publishes it when the time comes.")
        return _EXIT_OK

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.repost(target)
        published = None
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            app = TanApp(
                config,
                node.identity,
                name=config.name,
                request_timeout=float(getattr(args, "timeout", 15.0)),
            )
            node.attach_app(app)
            targets = None
            if getattr(args, "to", None):
                targets = [node.resolve_peer(target) for target in args.to]
            record = app.repost(target)
            published = app.publish(
                record,
                targets=targets,
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
            )
        finally:
            node.stop()

    if json_output:
        _emit({"repost": record, "published": published}, True)
    else:
        print(f"reposted {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_share(args: argparse.Namespace) -> int:
    """Share a Tan post with a peer, a group, a broadcast list, or a
    channel, as an attributed message.

    Mirrors :func:`cmd_forward`: the share is a new DM or group message
    carrying the post's text plus an attribution line, so the recipient
    reads it on any transport with no protocol change.  Delivery reuses
    :func:`cmd_send` wholesale for a peer target — or
    :func:`cmd_group_send` for a group target, fanning out one leg per
    member, or :func:`cmd_broadcast_send` for a broadcast target, one
    DM per member, or :func:`cmd_channel_send` for a channel target.
    Either way the share is a new message, so it carries no expiry,
    kind, or reply chain from the post.  A repost shares its original
    (see :func:`resolve_share_post`); a poll shares its question and
    options.
    """
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        post = resolve_share_post(conn, args.target)
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError as exc:
        # A rules file that cannot be parsed must not silently become
        # "display everything": a corrupt file hiding a kind would let
        # a share send that kind out. A missing file still loads as
        # the default ruleset inside load_ruleset.
        raise NodeError(
            f"cannot share: the Tan rules could not be read: {exc}"
        ) from exc
    if not apply_display_rules([post], display_ruleset):
        # Like thread() and likes(): a post the owner's display rules
        # hide is not amplified, even by hash — a hidden poll's
        # question and options must not go out in a share.
        raise NodeError("cannot share a post hidden by your Tan display rules")
    author = str(post.get("author") or "")
    identity = load_identity(config.identity_path)
    own_hash = destination_hash(identity).hex() if identity is not None else None
    if author and own_hash is not None and author != own_hash:
        # A post by a blocked peer must not be amplified, mirroring the
        # forward guard.  An unreadable block-list fails closed.
        blocked = load_blocked_set(config.blocklist_path)
        if blocked is None:
            raise NodeError("cannot share: the block-list could not be read")
        if author in blocked:
            raise NodeError("cannot share a post by a blocked peer")
    if own_hash is not None and author == own_hash:
        # The post is one this node wrote; the recipient already knows
        # the sharer, so the attribution names the sharer as "you".
        label = "you"
    else:
        # Re-resolve the author so a newer alias wins, falling back to
        # the name recorded when the post arrived (or the short hash).
        peer = (
            PeerRegistry(config.peers_path).get(author)
            if author
            else None
        )
        label = peer.display_name if peer is not None else None
        if not label:
            label = str(post.get("name") or "") or None
        if not label:
            label = short_hash(author) if author else "unknown"
    # `cmd_send` reads its text from `args.message`; hand it the
    # attributed text and let it own delivery, queueing, and output.
    # A group target takes the group-send path instead: the same
    # attributed text, fanned out one leg per member.  A broadcast
    # target takes the broadcast-send path the same way: one DM per
    # member, each with the same store-and-forward queueing as a
    # one-to-one DM, so every delivery flag keeps its meaning.  A
    # channel target takes the channel-send path the same way.
    args.message = share_text(post, label)
    if getattr(args, "group", None):
        return cmd_group_send(args)
    if getattr(args, "broadcast", None):
        return cmd_broadcast_send(args)
    if getattr(args, "channel", None):
        # `--no-queue`, `--wait`, and `--discover-timeout` only mean
        # something for peer/group delivery; the channel-send path
        # never reads them, so refuse them here instead of silently
        # ignoring them — the same refusal `cmd_forward` makes.
        if getattr(args, "no_queue", False):
            raise NodeError("--no-queue does not apply to a channel share")
        if getattr(args, "wait", None) is not None:
            raise NodeError("--wait does not apply to a channel share")
        if getattr(args, "discover_timeout", None) is not None:
            raise NodeError(
                "--discover-timeout does not apply to a channel share"
            )
        # `cmd_channel_send` reads `args.to` as a publish restriction
        # ("publish only to these peers"); the share's own `--to`
        # is a peer target, unset on this branch by the parser, but
        # clear it explicitly so the two meanings can never mix.
        args.to = None
        return cmd_channel_send(args)
    return cmd_send(args)


def cmd_tan_delete(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_delete_target(conn, args.target)
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.delete(target)
        published = None
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            app = TanApp(
                config,
                node.identity,
                name=config.name,
                request_timeout=float(getattr(args, "timeout", 15.0)),
            )
            node.attach_app(app)
            targets = None
            if getattr(args, "to", None):
                targets = [node.resolve_peer(target) for target in args.to]
            record = app.delete(target)
            published = app.publish(
                record,
                targets=targets,
                ttl=max(
                    0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)
                ),
            )
        finally:
            node.stop()

    if json_output:
        _emit({"delete": record, "published": published}, True)
    else:
        print(f"deleted {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_edit(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_post(conn, args.target)["hash"]
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.edit(target, text)
        published = None
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            app = TanApp(
                config,
                node.identity,
                name=config.name,
                request_timeout=float(getattr(args, "timeout", 15.0)),
            )
            node.attach_app(app)
            targets = None
            if getattr(args, "to", None):
                targets = [node.resolve_peer(target) for target in args.to]
            record = app.edit(target, text)
            published = app.publish(
                record,
                targets=targets,
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
            )
        finally:
            node.stop()

    if json_output:
        _emit({"edit": record, "published": published}, True)
    else:
        print(f"edited {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def _resolve_poll_option(value: str, options: Sequence[str]) -> int:
    """Resolve a vote's option (1-based number or exact text) to an index.

    Raises ValueError with a message the caller prints: a number out of
    range, or text that matches no option.  Options are distinct, so
    text can match at most once.
    """
    text = str(value).strip()
    if text.isascii() and text.isdigit():
        index = int(text) - 1
        if 0 <= index < len(options):
            return index
        raise ValueError(
            f"option {text} is out of range: this poll has {len(options)} options"
        )
    for index, option in enumerate(options):
        if option == text:
            return index
    raise ValueError(f"no such option: {text!r}")


def cmd_tan_poll(args: argparse.Namespace) -> int:
    question = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)

    options = list(getattr(args, "options", None) or [])
    if len(options) < MIN_POLL_OPTIONS:
        print(
            f"error: a poll needs at least {MIN_POLL_OPTIONS} --option values",
            file=sys.stderr,
        )
        return 2
    # Validate before the node starts, so a bad option fails fast.
    try:
        options = _clean_poll_options(options)
    except TanError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    expire_seconds = 0.0
    expire_text = getattr(args, "expire", None)
    if expire_text is not None:
        try:
            expire_seconds = _parse_duration(
                expire_text,
                flag="--expire",
                max_seconds=MAX_TAN_POST_EXPIRY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    expires_at = time.time() + expire_seconds if expire_text is not None else None

    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        if getattr(args, "no_publish", False):
            print(
                "error: --delay cannot be combined with --no-publish — "
                "a scheduled poll is published later, not stored only",
                file=sys.stderr,
            )
            return 2
        try:
            delay = _parse_duration(
                delay_text,
                flag="--delay",
                max_seconds=MAX_SCHEDULED_POST_DELAY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        if expires_at is not None and delay >= expire_seconds:
            print(
                "error: --delay must be shorter than --expire — the "
                "poll would expire before it is published",
                file=sys.stderr,
            )
            return 2
        try:
            scheduled = schedule_poll(
                config.scheduled_posts_path,
                question=question,
                options=options,
                publish_at=time.time() + delay,
                to=getattr(args, "to", None),
                ttl=int(getattr(args, "ttl", MAX_GOSSIP_TTL)),
                name=config.name,
                expires_in=expire_seconds,
            )
        except ScheduledPostsError as exc:
            raise NodeError(str(exc)) from exc
        if json_output:
            _emit({"scheduled": scheduled.to_dict()}, True)
        else:
            print(f"scheduled {scheduled.id} — asks in {_ago(delay)}")
            print("  list it with `tau tan scheduled`; a node serving")
            print("  with --social publishes it when the time comes.")
        return _EXIT_OK

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.create_poll(question, options, expires_at=expires_at)
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            # The serving node owns the data directory: create the
            # poll locally, then ask the running node to publish it,
            # the same way publish-draft re-delivers a stored post.
            from .local_publish import publish_running

            app = TanApp(config, _tan_identity(config), name=config.name)
            record = app.create_poll(question, options, expires_at=expires_at)
            result = publish_running(
                config.data_dir,
                post_hash=record["hash"],
                to=getattr(args, "to", None),
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            published = result["published"]
        else:
            try:
                app = TanApp(
                    config,
                    node.identity,
                    name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                node.attach_app(app)
                targets = None
                if getattr(args, "to", None):
                    targets = [node.resolve_peer(target) for target in args.to]
                record = app.create_poll(question, options, expires_at=expires_at)
                published = app.publish(
                    record,
                    targets=targets,
                    ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
                )
            finally:
                node.stop()

    if json_output:
        _emit({"poll": record, "published": published}, True)
    else:
        print(f"polled {short_hash(record['hash'])}")
        for index, option in enumerate(record.get("options") or [], start=1):
            print(f"  {index}. {option}")
        if expires_at is not None:
            print(f"  expires in {_ago(expire_seconds)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_vote(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        poll = resolve_post(conn, args.target)
        if poll.get("kind") != KIND_POLL:
            print(f"error: {args.target!r} is not a poll", file=sys.stderr)
            return 2
        options = _decode_options(poll.get("options")) or []
        try:
            option = _resolve_poll_option(args.option, options)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.vote(poll["hash"], option)
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            # The serving node owns the data directory: cast the vote
            # locally, then ask the running node to publish it, the
            # same way publish-draft re-delivers a stored post.
            from .local_publish import publish_running

            app = TanApp(config, _tan_identity(config), name=config.name)
            record = app.vote(poll["hash"], option)
            result = publish_running(
                config.data_dir,
                post_hash=record["hash"],
                to=getattr(args, "to", None),
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            published = result["published"]
        else:
            try:
                app = TanApp(
                    config,
                    node.identity,
                    name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                node.attach_app(app)
                targets = None
                if getattr(args, "to", None):
                    targets = [node.resolve_peer(target) for target in args.to]
                record = app.vote(poll["hash"], option)
                published = app.publish(
                    record,
                    targets=targets,
                    ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
                )
            finally:
                node.stop()

    if json_output:
        _emit({"vote": record, "published": published}, True)
    else:
        print(f"voted for {options[option]!r} on {short_hash(poll['hash'])}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_close_poll(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        poll = resolve_post(conn, args.target)
        if poll.get("kind") != KIND_POLL:
            print(f"error: {args.target!r} is not a poll", file=sys.stderr)
            return 2
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.close_poll(poll["hash"])
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            # The serving node owns the data directory: close the poll
            # locally, then ask the running node to publish it, the
            # same way publish-draft re-delivers a stored post.
            from .local_publish import publish_running

            app = TanApp(config, _tan_identity(config), name=config.name)
            record = app.close_poll(poll["hash"])
            result = publish_running(
                config.data_dir,
                post_hash=record["hash"],
                to=getattr(args, "to", None),
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            published = result["published"]
        else:
            try:
                app = TanApp(
                    config,
                    node.identity,
                    name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                node.attach_app(app)
                targets = None
                if getattr(args, "to", None):
                    targets = [node.resolve_peer(target) for target in args.to]
                record = app.close_poll(poll["hash"])
                published = app.publish(
                    record,
                    targets=targets,
                    ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
                )
            finally:
                node.stop()

    if json_output:
        _emit({"close": record, "published": published}, True)
    else:
        print(f"closed poll {short_hash(poll['hash'])}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_results(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    blocked = load_blocked_set(config.blocklist_path)
    conn = init_db(config.social_db_path)
    try:
        result = poll_results(
            conn,
            args.ref,
            exclude_authors=blocked,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    _print_tan_poll_results(result, json_output=json_output)
    return _EXIT_OK


def cmd_tan_timeline(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    blocked = load_blocked_set(config.blocklist_path)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        if getattr(args, "list", None):
            posts = list_timeline(
                conn,
                args.list,
                limit=args.limit,
                viewer=viewer,
                exclude_authors=blocked,
                exclude_all=blocked is None,
                tag=getattr(args, "tag", None),
            )
        elif getattr(args, "author", None):
            author, _ = resolve_author(
                conn, args.author, registry=PeerRegistry(config.peers_path)
            )
            posts = author_timeline(
                conn,
                author,
                limit=args.limit,
                viewer=viewer,
                exclude_authors=blocked,
                exclude_all=blocked is None,
                tag=getattr(args, "tag", None),
            )
        elif getattr(args, "all", False):
            posts = timeline(
                conn,
                include_all=True,
                limit=args.limit,
                viewer=viewer,
                exclude_authors=blocked,
                exclude_all=blocked is None,
                tag=getattr(args, "tag", None),
            )
        else:
            authors = [record["author"] for record in following(conn)]
            if viewer is not None:
                authors.append(viewer)
            posts = timeline(
                conn, authors, limit=args.limit, viewer=viewer,
                exclude_authors=blocked,
                exclude_all=blocked is None,
                tag=getattr(args, "tag", None),
            )
        annotate_pinned(conn, posts)
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_posts(posts, json_output=json_output)
    return _EXIT_OK


def cmd_tan_search(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    blocked = load_blocked_set(config.blocklist_path)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        author = None
        if getattr(args, "author", None):
            author, _ = resolve_author(
                conn, args.author, registry=PeerRegistry(config.peers_path)
            )
        posts = search_posts(
            conn,
            args.query,
            author=author,
            limit=args.limit,
            viewer=viewer,
            exclude_authors=blocked,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_posts(
        posts,
        json_output=json_output,
        empty_message=f"No stored posts match {args.query!r}.",
    )
    return _EXIT_OK


def cmd_tan_top(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        author = None
        if getattr(args, "author", None):
            author, _ = resolve_author(
                conn, args.author, registry=PeerRegistry(config.peers_path)
            )
        posts = top_posts(
            conn,
            author=author,
            limit=args.limit,
            viewer=viewer,
            ruleset=display_ruleset,
        )
    finally:
        conn.close()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_posts(
        posts,
        json_output=json_output,
        empty_message="No stored posts yet.",
    )
    return _EXIT_OK


def cmd_tan_tags(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    blocked = load_blocked_set(config.blocklist_path)
    conn = init_db(config.social_db_path)
    try:
        entries = tags(
            conn,
            limit=args.limit,
            exclude_authors=blocked,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    if json_output:
        _emit({"tags": entries, "count": len(entries)}, True)
        return _EXIT_OK
    if not entries:
        print("No hashtags in stored posts yet.")
        return _EXIT_OK
    width = max(len(str(entry["count"])) for entry in entries)
    for entry in entries:
        print(f"{entry['count']:>{width}}  #{entry['tag']}")
    return _EXIT_OK


def cmd_tan_verify(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    path = Path(config.social_db_path).expanduser()
    # Audit a read-only connection to the existing file: opening through
    # init_db would create a missing store, or migrate and rewrite an old one.
    try:
        if not path.is_file():
            raise FileNotFoundError(f"no Tan store at {path}")
        conn = sqlite3.connect(
            f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=10.0
        )
        conn.row_factory = sqlite3.Row
        try:
            report = verify_store(conn)
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as exc:
        message = f"could not audit Tan store at {path}: {exc}"
        if json_output:
            _emit({"ok": False, "error": message}, True)
        else:
            print(f"tau: error: {message}", file=sys.stderr)
        return _EXIT_ERROR
    if json_output:
        _emit(report, True)
        return _EXIT_OK if report["ok"] else _EXIT_ERROR
    if report["ok"]:
        print(
            f"Tan store OK: {report['entries']} entries from "
            f"{report['authors']} authors verified."
        )
        if report["unlinked"]:
            print(
                f"{report['unlinked']} unlinked entries are waiting for "
                "their parent to arrive."
            )
        return _EXIT_OK
    for problem in report["problems"]:
        print(
            f"{_safe_id_prefix(problem['hash'])} "
            f"({short_hash(problem['author'])}): {problem['problem']}"
        )
    print(
        f"Tan store has {len(report['problems'])} problem(s) in "
        f"{report['entries']} entries."
    )
    return _EXIT_ERROR


def cmd_tan_thread(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        blocked = load_blocked_set(config.blocklist_path)
        posts = thread(
            conn,
            args.ref,
            viewer=viewer,
            exclude_authors=blocked,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_thread(posts, json_output=json_output)
    return _EXIT_OK


def cmd_tan_show(args: argparse.Namespace) -> int:
    """Show one Tan post's full details: verification, counts, viewer state.

    Read-only: the Tan store is opened read-only and the identity is
    loaded without hardening, so invoking show never creates, migrates
    or chmods a file; no node is started and nothing is sent.
    Visibility mirrors :func:`tan.thread` and :func:`tan.likers`: an
    unlinked post is refused, a post from a blocked author is refused,
    and a reply beneath a deleted, expired or blocked ancestor is
    refused (via :func:`tan.ancestry_hidden`); a muted author or a
    hidden post still shows, flagged, because the caller named it
    explicitly — the same way a muted author anchors a thread.
    """
    config = _make_config(args)
    json_output = _json_mode(args)
    blocked = load_blocked_set(config.blocklist_path)
    path = Path(config.social_db_path).expanduser()
    if not path.is_file():
        raise NodeError(f"no Tan store at {path}; nothing to show yet")
    try:
        conn = open_db_readonly(path)
    except (OSError, sqlite3.Error) as exc:
        raise NodeError(f"could not read Tan store at {path}: {exc}") from exc
    try:
        identity = load_identity(config.identity_path, harden=False)
        viewer = destination_hash(identity).hex() if identity is not None else None
        post = resolve_post(conn, args.ref)
        if not post.get("linked"):
            # Like thread(): a cached entry still awaiting its chain
            # predecessor is not shown.
            raise TanError(f"post has not linked yet: {args.ref!r}")
        if blocked is None:
            raise TanError("cannot show post: the block-list is unusable")
        blocked_set = set(blocked)
        if post.get("author") in blocked_set:
            raise TanError(f"post is from a blocked author: {args.ref!r}")
        if ancestry_hidden(conn, post, blocked_set):
            raise TanError(
                "post hangs beneath a deleted, expired, or blocked ancestor"
            )
        silenced = muted_author_set(conn)
        hidden_hashes = hidden_post_set(conn)
        # The same display context the timeline passes, so a parent
        # preview never leaks muted, blocked or hidden content.
        annotate_reactions(conn, [post], viewer=viewer)
        annotate_replies(
            conn,
            [post],
            muted_keywords=_muted_keyword_list(conn),
            exclude_authors=silenced,
            blocked_authors=blocked_set,
            hidden_posts=hidden_hashes,
        )
        annotate_quote_counts(conn, [post])
        annotate_quotes(
            conn,
            [post],
            muted_keywords=_muted_keyword_list(conn),
            muted_tags=_muted_tag_list(conn),
            exclude_authors=silenced,
            blocked_authors=blocked_set,
            hidden_posts=hidden_hashes,
        )
        annotate_repost_counts(conn, [post], viewer=viewer)
        kept = annotate_reposts(
            conn,
            [post],
            muted_keywords=_muted_keyword_list(conn),
            exclude_authors=silenced,
            blocked_authors=blocked_set,
            hidden_posts=hidden_hashes,
        )
        if not kept:
            # A repost whose original is blocked, muted, keyword-muted
            # or hidden is dropped by the annotator instead of being
            # kept with a redacted preview — refuse it rather than
            # rendering the repost with a misleading "not cached" label.
            raise TanError(
                "repost's original is blocked, muted, or hidden: "
                f"{args.ref!r}"
            )
        annotate_saved(conn, [post])
        annotate_pinned(conn, [post])
        # Standing, unexpired replies only: linked, not deleted, not
        # past their expiry — the same definition the thread view uses
        # for a post's visible conversation.
        row = conn.execute(
            "SELECT COUNT(*) FROM posts WHERE kind = 'reply' AND linked = 1 "
            f"AND target = ? AND {_NOT_DELETED} "
            "AND (expires_at IS NULL OR expires_at > ?)",
            (str(post["hash"]), time.time()),
        ).fetchone()
        replies = int(row[0]) if row is not None else 0
        try:
            verify_post(dict(post))
            verification = "verified"
        except TanError as exc:
            verification = f"invalid: {exc}"
        # Verify the original signed post first: the edit replacement
        # below must not be verified as the signed payload.
        annotate_edits(conn, [post])
        author = str(post.get("author") or "")
        author_muted = author in silenced
        author_followed = author in {
            record["author"] for record in following(conn)
        }
        hidden = str(post["hash"]) in hidden_hashes
    except sqlite3.Error as exc:
        raise NodeError(f"could not read Tan store at {path}: {exc}") from exc
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    if not apply_display_rules([post], display_ruleset):
        # Like likers(): a post the owner's display rules hide is not shown.
        raise TanError(f"post is hidden by your Tan display rules: {args.ref!r}")
    # Previews inherit the same display rules: a referenced post whose
    # kind the owner does not display is redacted before either output
    # path runs, so its text cannot leak through a visible reply,
    # repost or quote.  A preview without a kind is redacted too — its
    # kind cannot be proven displayed.
    for preview_key in ("reply_to", "repost_of", "quoted"):
        preview = post.get(preview_key)
        if isinstance(preview, dict):
            kind = preview.get("kind")
            if kind is None or not display_ruleset.displays(str(kind)):
                preview["text"] = ""
                preview["hidden"] = True
    post["verification"] = verification
    post["replies"] = replies
    post["author_muted"] = author_muted
    post["author_followed"] = author_followed
    post["hidden"] = hidden
    if json_output:
        _emit({"post": post}, True)
        return _EXIT_OK
    name = post.get("name") or "anonymous"
    author_hash = str(post.get("author") or "")
    try:
        created_at = float(post.get("created_at", 0))
    except (TypeError, ValueError):
        # A hand-edited record with a non-numeric timestamp shows as
        # unknown instead of breaking the detail view.
        created_at = 0.0
    try:
        age = _ago(time.time() - created_at)
    except (OverflowError, OSError, ValueError, TypeError):
        age = "unknown"
    print(
        f"{name} ({short_hash(author_hash)}) — "
        f"{short_hash(str(post.get('hash', '')))}"
        f"{' (edited)' if post.get('edited') else ''}"
    )
    print(f"  hash: {post.get('hash', '')}")
    print(f"  created: {_format_message_time(created_at)} ({age} ago)")
    print(f"  kind: {post.get('kind')}")
    print(f"  chain: {verification}")
    if post.get("kind") == "reply":
        parent = post.get("reply_to")
        if isinstance(parent, dict):
            if parent.get("blocked"):
                print("  in reply to a blocked author's post")
            elif parent.get("deleted"):
                print("  in reply to a deleted post")
            elif parent.get("muted"):
                print("  in reply to a muted post")
            elif parent.get("expired"):
                print("  in reply to an expired post")
            elif parent.get("hidden"):
                print("  in reply to a hidden post")
            else:
                parent_text = str(parent.get("text") or "")
                if len(parent_text) > 80:
                    parent_text = parent_text[:80] + "…"
                print(
                    f"  in reply to {parent.get('name') or 'anonymous'}: "
                    f"{parent_text}"
                )
        else:
            print(f"  in reply to {short_hash(str(post.get('target', '')))}")
    if post.get("kind") == "repost":
        original = post.get("repost_of")
        if isinstance(original, dict):
            if original.get("deleted"):
                print("  reposted a deleted post")
            elif original.get("expired"):
                print("  reposted an expired post")
            elif original.get("hidden"):
                print("  reposted a hidden post")
            else:
                print(
                    f"  reposted {original.get('name') or 'anonymous'} "
                    f"({short_hash(str(original.get('author', '')))}):"
                )
                for line in str(original.get("text") or "").splitlines() or [""]:
                    print(f"  {line}")
        else:
            print(
                f"  reposted {short_hash(str(post.get('target', '')))}"
                " (original not cached yet)"
            )
    else:
        for line in str(post.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
    if post.get("kind") == "quote":
        quoted = post.get("quoted")
        if isinstance(quoted, dict):
            if quoted.get("blocked"):
                print("  quoting a blocked author's post")
            elif quoted.get("deleted"):
                print("  quoting a deleted post")
            elif quoted.get("muted"):
                print("  quoting a muted author's post")
            elif quoted.get("expired"):
                print("  quoting an expired post")
            elif quoted.get("hidden"):
                print("  quoting a hidden post")
            else:
                print(f"  quoting {quoted.get('name') or 'anonymous'}:")
                for line in str(quoted.get("text") or "").splitlines() or [""]:
                    print(f"    {line}")
        else:
            print(f"  quoting {short_hash(str(post.get('target', '')))}")
    if post.get("kind") == "poll":
        _print_poll_options(post)
    _print_tan_image(post)
    _print_tan_location(post)
    # Engagement lines print only when their annotation survived the
    # display rules: a ruleset hiding a kind strips its count, and a
    # missing count must not read as zero activity.
    if "likes" in post:
        likes = int(post.get("likes") or 0)
        liked = " (you liked this)" if post.get("liked") else ""
        print(f"  likes: {likes}{liked}")
    if "reposts" in post:
        reposts = int(post.get("reposts") or 0)
        reposted = " (you reposted this)" if post.get("reposted") else ""
        print(f"  reposts: {reposts}{reposted}")
    if "quotes" in post:
        print(f"  quotes: {int(post.get('quotes') or 0)}")
    print(f"  replies: {replies}")
    expires_at = post.get("expires_at")
    if isinstance(expires_at, (int, float)):
        try:
            print(
                "  expires: disappears in "
                f"{_ago(float(expires_at) - time.time())}"
            )
        except (OverflowError, OSError, ValueError, TypeError):
            print("  expires: unknown")
    print(
        f"  saved: {'yes' if post.get('saved') else 'no'}, "
        f"pinned: {'yes' if post.get('pinned') else 'no'}, "
        f"hidden: {'yes' if hidden else 'no'}"
    )
    print(
        f"  author: followed {'yes' if author_followed else 'no'}, "
        f"muted {'yes' if author_muted else 'no'}"
    )
    return _EXIT_OK


def cmd_tan_likes(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        blocked = load_blocked_set(config.blocklist_path)
        result = likers(
            conn,
            args.ref,
            exclude_authors=blocked,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    if not apply_display_rules([result["post"]], display_ruleset):
        # Like thread() and search(): a post the owner's display rules
        # hide is not shown here either.
        raise TanError(f"post is hidden by your Tan display rules: {args.ref!r}")
    _print_tan_likes(result, json_output=json_output)
    return _EXIT_OK


def cmd_tan_reposts(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        blocked = load_blocked_set(config.blocklist_path)
        # Like the web route: muted authors are hidden as well as blocked.
        hidden_authors = set(blocked or ()) | muted_author_set(conn)
        result = reposters(
            conn,
            args.ref,
            exclude_authors=hidden_authors,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    if not apply_display_rules([result["post"]], display_ruleset):
        # Like thread() and search(): a post the owner's display rules
        # hide is not shown here either.
        raise TanError(f"post is hidden by your Tan display rules: {args.ref!r}")
    _print_tan_reposters(result, json_output=json_output)
    return _EXIT_OK


def cmd_tan_quotes(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        blocked = load_blocked_set(config.blocklist_path)
        # Like the web route: muted authors are hidden as well as blocked.
        hidden_authors = set(blocked or ()) | muted_author_set(conn)
        result = quoters(
            conn,
            args.ref,
            exclude_authors=hidden_authors,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    if not apply_display_rules([result["post"]], display_ruleset):
        # Like thread() and search(): a post the owner's display rules
        # hide is not shown here either.
        raise TanError(f"post is hidden by your Tan display rules: {args.ref!r}")
    if not display_ruleset.displays(KIND_QUOTE):
        # Quotes the owner hides disclose no quoters, as the web route does.
        result["quoters"] = []
    _print_tan_quoters(result, json_output=json_output)
    return _EXIT_OK


def cmd_tan_mentions(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        # Mentions are defined relative to this node's identity: a
        # reply to one of your posts.  Without an identity there is
        # nothing of yours to be replied to.
        identity = _tan_identity(config)
        viewer = destination_hash(identity).hex()
        posts = mentions(conn, viewer, limit=args.limit)
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_posts(
        posts,
        json_output=json_output,
        empty_message="No replies to your posts yet.",
    )
    return _EXIT_OK


def cmd_tan_follow(args: argparse.Namespace) -> int:
    config = _make_config(args)
    config.prepare()
    conn = init_db(config.social_db_path)
    try:
        author, name = resolve_author(
            conn, args.target, registry=PeerRegistry(config.peers_path)
        )
        record = follow(conn, author, name=name)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        print(f"following {name or author} ({short_hash(author)})")
    return _EXIT_OK


def cmd_tan_unfollow(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        author, name = resolve_author(
            conn, args.target, registry=PeerRegistry(config.peers_path)
        )
        removed = unfollow(conn, author)
    finally:
        conn.close()
    if _json_mode(args):
        _emit({"author": author, "unfollowed": removed}, True)
    else:
        if removed:
            print(f"unfollowed {name or author} ({short_hash(author)})")
        else:
            print(f"was not following {name or author} ({short_hash(author)})")
    return _EXIT_OK


def cmd_tan_following(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        records = following(conn)
        for record in records:
            record.update(author_stats(conn, record["author"]))
    finally:
        conn.close()

    if _json_mode(args):
        _emit({"following": records, "count": len(records)}, True)
    else:
        if not records:
            print("Not following anyone. Try `tau tan follow alice`.")
        for record in records:
            name = record.get("name") or "unknown"
            print(
                f"{name:<20} {short_hash(record['author']):<10} "
                f"{record.get('posts', 0)} post(s)"
            )
    return _EXIT_OK


def cmd_tan_followers(args: argparse.Namespace) -> int:
    """List the authors who follow this node.

    Followers are derived from the peers' signed follow lists this
    node has already verified and stored — the same lists the sync
    depth uses — so this never does network I/O.  A follower whose
    list was never fetched does not appear yet; fetch it first with
    ``tau tan follows-of`` or a sync.
    """
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )
    own_author = destination_hash(identity).hex()
    store = FollowListStore(config.follow_lists_path)
    lists = {
        doc["author"]: doc["authors"]
        for doc in store.all()
        if isinstance(doc, dict)
        and isinstance(doc.get("author"), str)
        and isinstance(doc.get("authors"), list)
    }
    registry = PeerRegistry(config.peers_path)
    conn = init_db(config.social_db_path)
    try:
        records = followers(conn, lists, own_author, registry=registry)
    finally:
        conn.close()

    if _json_mode(args):
        _emit({"followers": records, "count": len(records)}, True)
    else:
        if not records:
            print(
                "No followers yet — nobody with a stored follow list "
                "follows you. Try `tau tan sync` to fetch follow lists."
            )
        for record in records:
            name = record.get("name") or "unknown"
            followed = "following" if record.get("followed") else ""
            print(
                f"{name:<20} {short_hash(record['author']):<10} "
                f"{record.get('posts', 0)} post(s)  {followed}".rstrip()
            )
    return _EXIT_OK


def _print_follow_list(
    doc: Dict[str, Any],
    registry: Optional[PeerRegistry] = None,
    *,
    indent: str = "",
) -> None:
    """Print one follow-list document in text form.

    Names come from the peer registry when it knows the author;
    otherwise the destination hash speaks for itself.
    """
    print(
        f"{indent}version: {doc['version']} "
        f"({_format_message_time(doc['updated_at'])})"
    )
    if not doc["authors"]:
        print(f"{indent}follows: —")
        return
    print(f"{indent}follows ({len(doc['authors'])}):")
    for author in doc["authors"]:
        name = None
        if registry is not None:
            peer = registry.get(author)
            if peer is not None:
                name = peer.display_name
        if name:
            print(f"{indent}  {name:<20} {short_hash(author)}")
        else:
            print(f"{indent}  {author}")


def cmd_tan_publish_follows(args: argparse.Namespace) -> int:
    """Publish this node's follow list as a signed document.

    The list is built from the local Tan follows and signed with the
    node identity, so peers can fetch and verify it over ``/tau/follows``
    — the piece the sync-depth setting needs to expand a sync to the
    authors your follows follow.  The version bumps on every publish;
    publishing after unfollowing everyone writes an empty list, which
    is a real statement, not an error.
    """
    config = _make_config(args)
    config.prepare()
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )
    conn = init_db(config.social_db_path)
    try:
        authors = [record["author"] for record in following(conn)]
    finally:
        conn.close()
    existing = load_local_follow_list(config.follow_list_path)
    version = existing["version"] + 1 if existing is not None else 1
    try:
        doc = build_follow_list(identity, authors=authors, version=version)
    except FollowListError as exc:
        raise NodeError(str(exc)) from exc
    save_local_follow_list(config.follow_list_path, doc)
    if _json_mode(args):
        _emit(public_follow_list(doc), True)
    else:
        print(
            f"Published follow list version {version} "
            f"({len(doc['authors'])} author(s))."
        )
        print("  Peers can fetch it with `tau tan follows-of <your name>`.")
    return _EXIT_OK


def cmd_tan_follows_of(args: argparse.Namespace) -> int:
    """Show who a peer follows, fetching their signed follow list.

    A live fetch is attempted first (unless ``--cached``): the peer
    answers with its follow list only when it holds a newer version
    than this node.  When the fetch fails, the stored copy is shown
    instead and the failure is named; when nothing is stored at all,
    the fetch failure is the error.
    """
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    try:
        peer = registry.resolve(args.target)
    except PeerError as exc:
        raise NodeError(str(exc)) from exc
    if peer is None:
        raise NodeError(f"no peer matches {args.target!r}")
    store = FollowListStore(config.follow_lists_path)
    fetch_error: Optional[str] = None
    fetched = False
    if not getattr(args, "cached", False):
        node = TauNode(config, name=config.name)
        node.start()
        try:
            node.request_follows(peer, timeout=args.timeout)
            fetched = True
        except NodeError as exc:
            fetch_error = str(exc)
        finally:
            node.stop()
        # The node saved any newer list it fetched; re-read the
        # store rather than trusting the in-memory copy above.
        store = FollowListStore(config.follow_lists_path)
    follow_list = store.get(peer.hexhash)
    if follow_list is None:
        if fetch_error is not None:
            raise NodeError(
                f"could not fetch a follow list from {peer.display_name}: "
                f"{fetch_error}"
            )
        if _json_mode(args):
            _emit(
                {
                    "peer": peer.hexhash,
                    "name": peer.display_name,
                    "follow_list": None,
                },
                True,
            )
        else:
            print(f"{peer.display_name} has not published a follow list yet.")
        return _EXIT_OK
    if _json_mode(args):
        _emit(
            {
                "peer": peer.hexhash,
                "name": peer.display_name,
                "follow_list": public_follow_list(follow_list),
                "fetched": fetched,
                "fetch_error": fetch_error,
            },
            True,
        )
    else:
        print(f"{peer.display_name} follows:")
        _print_follow_list(follow_list, registry, indent="  ")
        if fetch_error is not None:
            print(f"  (showing the stored copy — fetch failed: {fetch_error})")
    return _EXIT_OK


def cmd_tan_authors(args: argparse.Namespace) -> int:
    config = _make_config(args)
    limit = getattr(args, "limit", 50)
    if limit is not None and limit < 0:
        raise NodeError("--limit must not be negative")
    # A read-only listing must not create the store on a fresh data dir.
    if not config.social_db_path.exists():
        records: List[Dict[str, Any]] = []
    else:
        conn = open_readonly_db(config.social_db_path)
        try:
            records = list_authors(conn, sort=args.sort, limit=limit)
        finally:
            conn.close()

    if _json_mode(args):
        _emit({"authors": records, "count": len(records)}, True)
        return _EXIT_OK
    if not records:
        print("No authors in the local Tan store yet. Post or sync first.")
        return _EXIT_OK
    for record in records:
        name = record.get("name") or "unknown"
        last = (
            _format_message_time(record["last_post"])
            if record["last_post"]
            else "never"
        )
        followed = "following" if record.get("followed") else ""
        print(
            f"{name:<20} {short_hash(record['author']):<10} "
            f"{record.get('posts', 0)} post(s)  last {last}  {followed}".rstrip()
        )
    return _EXIT_OK


def cmd_tan_suggest(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        suggestions = suggest_authors(conn, viewer=viewer, limit=args.limit)
    finally:
        conn.close()

    if json_output:
        _emit({"suggestions": suggestions, "count": len(suggestions)}, True)
    else:
        if not suggestions:
            print("No suggestions yet. Follow an author or sync more posts first.")
        for item in suggestions:
            name = item.get("name") or "unknown"
            print(
                f"{name:<20} {short_hash(item['author']):<10} "
                f"{item.get('posts', 0)} post(s), {item.get('likes', 0)} like(s) "
                f"— {item.get('reason', '')}"
            )
    return _EXIT_OK


def _resolve_mute_target(
    conn: sqlite3.Connection, target: str, registry: PeerRegistry
) -> Tuple[str, str]:
    """Resolve a mute/unmute target, falling back to stored names.

    The peer registry does not know authors whose posts are only cached,
    and a muted author may have no cached posts at all — so after the
    registry/hash/prefix lookup fails, try the name stored with the
    mute, then the name on the author's cached posts.
    """
    try:
        return resolve_author(conn, target, registry=registry)
    except TanError as exc:
        match = resolve_muted_name(conn, target)
        if match is not None:
            return match
        try:
            return resolve_cached_author(conn, target)
        except TanError:
            raise exc from None


def cmd_tan_mute(args: argparse.Namespace) -> int:
    config = _make_config(args)
    config.prepare()
    conn = init_db(config.social_db_path)
    try:
        author, name = _resolve_mute_target(
            conn, args.target, PeerRegistry(config.peers_path)
        )
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        if viewer is not None and author == viewer:
            raise TanError("cannot mute yourself")
        record = mute(conn, author, name=name)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        print(f"muted {name or author} ({short_hash(author)})")
    return _EXIT_OK


def cmd_tan_unmute(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        author, name = _resolve_mute_target(
            conn, args.target, PeerRegistry(config.peers_path)
        )
        removed = unmute(conn, author)
    finally:
        conn.close()
    if _json_mode(args):
        _emit({"author": author, "unmuted": removed}, True)
    else:
        if removed:
            print(f"unmuted {name or author} ({short_hash(author)})")
        else:
            print(f"was not muted: {name or author} ({short_hash(author)})")
    return _EXIT_OK


def cmd_tan_muted(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        records = muted(conn)
        for record in records:
            record.update(author_stats(conn, record["author"]))
    finally:
        conn.close()

    if _json_mode(args):
        _emit({"muted": records, "count": len(records)}, True)
    else:
        if not records:
            print("No muted authors. Mute one with `tau tan mute alice`.")
        for record in records:
            name = record.get("name") or "unknown"
            when = datetime.fromtimestamp(float(record["muted_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(
                f"{name:<20} {short_hash(record['author']):<10} "
                f"{record.get('posts', 0)} post(s) muted {when}"
            )
    return _EXIT_OK


def cmd_tan_lists(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    cmd = getattr(args, "lists_cmd", None)
    registry = PeerRegistry(config.peers_path)
    conn = init_db(config.social_db_path)
    try:
        if cmd is None:
            records = tan_lists(conn)
            if json_output:
                _emit({"lists": records, "count": len(records)}, True)
            else:
                if not records:
                    print("No lists yet. Create one with `tau tan lists create friends`.")
                for record in records:
                    count = record["members"]
                    print(
                        f"{record['name']:<20} {count} member(s)"
                    )
            return _EXIT_OK
        if cmd == "create":
            config.prepare()
            record = create_list(conn, args.name)
            if json_output:
                _emit(record, True)
            else:
                print(f"created list {record['name']!r}")
            return _EXIT_OK
        if cmd == "delete":
            removed = delete_list(conn, args.name)
            if json_output:
                _emit(
                    {"name": args.name.strip(), "deleted": removed}, True
                )
            else:
                if removed:
                    print(f"deleted list {args.name.strip()!r}")
                else:
                    print(f"no list named {args.name.strip()!r}")
            return _EXIT_OK
        if cmd == "show":
            records = list_members(conn, args.name)
            for record in records:
                record.update(author_stats(conn, record["author"]))
                peer = registry.get(record["author"])
                if peer is not None and peer.display_name:
                    record["name"] = peer.display_name
            display = next(
                (
                    entry["name"]
                    for entry in tan_lists(conn)
                    if entry["name"].casefold() == args.name.strip().casefold()
                ),
                args.name.strip(),
            )
            if json_output:
                _emit(
                    {
                        "name": display,
                        "members": records,
                        "count": len(records),
                    },
                    True,
                )
            else:
                if not records:
                    print(f"List {display!r} is empty.")
                for record in records:
                    name = record.get("name") or "unknown"
                    print(
                        f"{name:<20} {short_hash(record['author']):<10} "
                        f"{record.get('posts', 0)} post(s)"
                    )
            return _EXIT_OK
        if cmd in ("add", "remove"):
            config.prepare()
            followed = {
                record["author"] for record in following(conn)
            }
            results = []
            for target in args.authors:
                author, name = resolve_author(conn, target, registry=registry)
                if cmd == "add":
                    record = add_to_list(
                        conn, args.name, author, author_name=name
                    )
                    record["followed"] = author in followed
                    results.append(record)
                else:
                    removed = remove_from_list(conn, args.name, author)
                    results.append(
                        {
                            "list": args.name.strip(),
                            "author": author,
                            "name": name,
                            "removed": removed,
                        }
                    )
            if json_output:
                _emit({"results": results, "count": len(results)}, True)
            else:
                for record in results:
                    label = record["name"] or record["author"]
                    if cmd == "add":
                        note = (
                            " (not followed — a list is curation, not a follow)"
                            if not record["followed"]
                            else ""
                        )
                        if record["added"]:
                            print(
                                f"added {label} ({short_hash(record['author'])}) "
                                f"to {record['list']!r}{note}"
                            )
                        else:
                            print(
                                f"{label} ({short_hash(record['author'])}) "
                                f"is already in {record['list']!r}"
                            )
                    else:
                        if record["removed"]:
                            print(
                                f"removed {label} ({short_hash(record['author'])}) "
                                f"from {record['list']!r}"
                            )
                        else:
                            print(
                                f"{label} ({short_hash(record['author'])}) "
                                f"was not in {record['list']!r}"
                            )
            return _EXIT_OK
    finally:
        conn.close()
    return _EXIT_OK


def cmd_tan_mute_keyword(args: argparse.Namespace) -> int:
    config = _make_config(args)
    config.prepare()
    conn = init_db(config.social_db_path)
    try:
        record = mute_keyword(conn, args.keyword)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        print(f"muted keyword {record['display']!r}")
    return _EXIT_OK


def cmd_tan_unmute_keyword(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        removed = unmute_keyword(conn, args.keyword)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(
            {
                "keyword": " ".join(args.keyword.split()).casefold(),
                "unmuted": removed,
            },
            True,
        )
    else:
        if removed:
            print(f"unmuted keyword {args.keyword.strip()!r}")
        else:
            print(f"keyword {args.keyword.strip()!r} was not muted")
    return _EXIT_OK


def cmd_tan_muted_keywords(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        records = muted_keywords(conn)
    finally:
        conn.close()

    if _json_mode(args):
        _emit({"muted_keywords": records, "count": len(records)}, True)
    else:
        if not records:
            print("No muted keywords. Try `tau tan mute-keyword WORD`.")
        for record in records:
            when = datetime.fromtimestamp(float(record["added_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(f"{record['display']} (muted {when})")
    return _EXIT_OK


def cmd_tan_follow_tag(args: argparse.Namespace) -> int:
    config = _make_config(args)
    config.prepare()
    conn = init_db(config.social_db_path)
    try:
        record = follow_tag(conn, args.tag)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        print(f"following tag {record['display']!r}")
    return _EXIT_OK


def cmd_tan_unfollow_tag(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        removed = unfollow_tag(conn, args.tag)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(
            {
                "tag": clean_tag(args.tag),
                "unfollowed": removed,
            },
            True,
        )
    else:
        if removed:
            print(f"unfollowed tag {args.tag.strip()!r}")
        else:
            print(f"tag {args.tag.strip()!r} was not followed")
    return _EXIT_OK


def cmd_tan_followed_tags(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        records = followed_tags(conn)
    finally:
        conn.close()

    if _json_mode(args):
        _emit({"followed_tags": records, "count": len(records)}, True)
    else:
        if not records:
            print("No followed tags. Try `tau tan follow-tag #TAG`.")
        for record in records:
            when = datetime.fromtimestamp(float(record["added_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(f"{record['display']} (followed {when})")
    return _EXIT_OK


def cmd_tan_mute_tag(args: argparse.Namespace) -> int:
    config = _make_config(args)
    config.prepare()
    conn = init_db(config.social_db_path)
    try:
        record = mute_tag(conn, args.tag)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        print(f"muted tag {record['display']!r}")
    return _EXIT_OK


def cmd_tan_unmute_tag(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        removed = unmute_tag(conn, args.tag)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(
            {
                "tag": clean_tag(args.tag),
                "unmuted": removed,
            },
            True,
        )
    else:
        if removed:
            print(f"unmuted tag {args.tag.strip()!r}")
        else:
            print(f"tag {args.tag.strip()!r} was not muted")
    return _EXIT_OK


def cmd_tan_muted_tags(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        records = muted_tags(conn)
    finally:
        conn.close()

    if _json_mode(args):
        _emit({"muted_tags": records, "count": len(records)}, True)
    else:
        if not records:
            print("No muted tags. Try `tau tan mute-tag #TAG`.")
        for record in records:
            when = datetime.fromtimestamp(float(record["added_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(f"{record['display']} (muted {when})")
    return _EXIT_OK


def cmd_tan_save(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        record = save_post(conn, args.ref)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        if record.get("already_saved"):
            when = datetime.fromtimestamp(float(record["saved_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(f"already saved {short_hash(str(record['hash']))} at {when}")
        else:
            print(f"saved {short_hash(str(record['hash']))}")
    return _EXIT_OK


def cmd_tan_unsave(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        record = unsave_post(conn, args.ref)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        if record["unsaved"]:
            print(f"unsaved {short_hash(str(record['hash']))}")
        else:
            print(f"was not saved: {short_hash(str(record['hash']))}")
    return _EXIT_OK


def cmd_tan_saved(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        posts, _ = annotated_saved_posts(
            conn, limit=args.limit, viewer=viewer, ruleset=display_ruleset
        )
    finally:
        conn.close()
    _print_saved_posts(posts, json_output=json_output)
    return _EXIT_OK
    return _EXIT_OK


def cmd_tan_hide(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        record = hide_post(conn, args.ref)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        if record.get("already_hidden"):
            when = datetime.fromtimestamp(float(record["hidden_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(f"already hidden {short_hash(str(record['hash']))} at {when}")
        else:
            print(f"hidden {short_hash(str(record['hash']))}")
    return _EXIT_OK


def cmd_tan_unhide(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        record = unhide_post(conn, args.ref)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        if record["unhidden"]:
            print(f"unhidden {short_hash(str(record['hash']))}")
        else:
            print(f"was not hidden: {short_hash(str(record['hash']))}")
    return _EXIT_OK


def cmd_tan_hidden(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        posts = hidden_posts(conn, limit=args.limit)
    finally:
        conn.close()
    _print_hidden_posts(posts, json_output=json_output)
    return _EXIT_OK


def cmd_tan_pin(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        identity = _tan_identity(config)
        owner = destination_hash(identity).hex()
        record = pin_post(conn, args.ref, owner=owner)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        if record.get("already_pinned"):
            when = datetime.fromtimestamp(float(record["pinned_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(f"already pinned {short_hash(str(record['hash']))} at {when}")
        else:
            print(f"pinned {short_hash(str(record['hash']))}")
    return _EXIT_OK


def cmd_tan_unpin(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        record = unpin_post(conn, args.ref)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        if record["unpinned"]:
            print(f"unpinned {short_hash(str(record['hash']))}")
        else:
            print(f"was not pinned: {short_hash(str(record['hash']))}")
    return _EXIT_OK


def cmd_tan_pinned(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    blocked = load_blocked_set(config.blocklist_path)
    conn = init_db(config.social_db_path)
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        posts, _ = annotated_pinned_posts(
            conn,
            limit=args.limit,
            viewer=viewer,
            ruleset=display_ruleset,
            exclude_authors=blocked,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    _print_pinned_posts(posts, json_output=json_output)
    return _EXIT_OK


def cmd_tan_rules(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        ruleset = load_ruleset(config.tan_rules_path)
    except RulesError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_ERROR
    data = ruleset.to_dict()
    changed = False

    author_args = (
        ("allow_authors", getattr(args, "allow_author", None)),
        ("block_authors", getattr(args, "block_author", None)),
    )
    removal_args = (
        ("allow_authors", getattr(args, "unallow_author", None)),
        ("block_authors", getattr(args, "unblock_author", None)),
    )
    if any(values for _, values in author_args + removal_args):
        conn = init_db(config.social_db_path)
        try:
            registry = PeerRegistry(config.peers_path)
            for field_name, values in author_args:
                for value in values or []:
                    author, _ = resolve_author(conn, value, registry=registry)
                    if author not in data[field_name]:
                        data[field_name].append(author)
                        changed = True
            for field_name, values in removal_args:
                for value in values or []:
                    author, _ = resolve_author(conn, value, registry=registry)
                    if author in data[field_name]:
                        data[field_name].remove(author)
                        changed = True
        finally:
            conn.close()

    if getattr(args, "default", None) is not None:
        data["default"] = args.default
        changed = True
    followed_only = getattr(args, "followed_only", None)
    if followed_only is not None:
        data["allow_followed"] = bool(followed_only)
        changed = True
    if getattr(args, "all_kinds", False):
        data["kinds"] = None
        changed = True
    elif getattr(args, "kinds", None) is not None:
        data["kinds"] = [
            part.strip() for part in str(args.kinds).split(",") if part.strip()
        ]
        changed = True

    if changed:
        try:
            ruleset = Ruleset.from_dict(data)
        except RulesError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        data = ruleset.to_dict()
        save_ruleset(config.tan_rules_path, ruleset)

    if json_output:
        payload = ruleset.to_dict()
        payload["path"] = str(config.tan_rules_path)
        _emit(payload, True)
    else:
        print(f"Tan admission rules ({config.tan_rules_path}):")
        print(f"  default: {data['default']}")
        print(
            f"  followed authors admitted: {'yes' if data['allow_followed'] else 'no'}"
        )
        kinds = data["kinds"]
        print(
            f"  kinds displayed: {', '.join(kinds) if kinds else 'all'}"
            " (display-only; all kinds are stored)"
        )
        for label, field_name in (
            ("allow", "allow_authors"),
            ("block", "block_authors"),
        ):
            authors = data[field_name]
            print(f"  {label} list: {len(authors)} author(s)")
            for author in authors:
                print(f"    {author}")
        if changed:
            print("  (a running serve applies saved rules when it restarts)")
    return _EXIT_OK


def cmd_tan_sync(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_or_create_identity(config.identity_path)
    node = TauNode(config, name=config.name)
    node.start(announce=False)
    try:
        app = TanApp(
            config,
            identity,
            name=config.name,
            request_timeout=float(getattr(args, "timeout", 15.0)),
        )
        node.attach_app(app)
        targets = None
        if getattr(args, "to", None):
            targets = [node.resolve_peer(target) for target in args.to]

        authors = None
        if getattr(args, "author", None):
            conn = app.connect()
            try:
                authors = [
                    resolve_author(
                        conn, target, registry=PeerRegistry(config.peers_path)
                    )[0]
                    for target in args.author
                ]
            finally:
                conn.close()

        summary = app.sync(
            node,
            targets=targets,
            authors=authors,
            limit=int(getattr(args, "limit", DEFAULT_FEED_LIMIT)),
            explore=bool(getattr(args, "explore", False)),
            timeout=float(getattr(args, "timeout", 15.0)),
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(summary, True)
    else:
        print(
            f"synced {summary['stored']} new post(s) "
            f"from {summary['peers']} peer(s); "
            f"{summary['duplicates']} duplicate(s)"
        )
        for error in summary["errors"]:
            print(f"  {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK
def _print_channel_messages(
    messages: List[Dict[str, Any]],
    *,
    json_output: bool,
    channel: str,
    reactions: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> None:
    from .channels import summarize_reactions

    if json_output:
        if reactions:
            for message in messages:
                message["reactions"] = reactions.get(str(message.get("hash", "")), [])
        _emit({"channel": channel, "messages": messages, "count": len(messages)}, True)
        return
    if not messages:
        print(f"No stored messages in #{channel}.")
        return
    for message in messages:
        # Votes and closes are not chat: they ride as messages so
        # peers can tally them, but they render on the poll itself.
        if is_channel_poll_vote(message) or is_channel_poll_close(message):
            continue
        name = message.get("name") or "anonymous"
        author = short_hash(str(message.get("author", "")))
        edited = " (edited)" if message.get("edited") else ""
        digest = short_hash(str(message.get("hash", "")))
        print(f"[{_stamp(message.get('created_at'))}] {name} ({author}){edited} [{digest}]:")
        if message.get("reply_to"):
            parent = message.get("reply_parent")
            if parent:
                parent_text = str(parent.get("text", "")).splitlines()
                excerpt = parent_text[0] if parent_text else ""
                print(
                    f"  ↩ replying to {parent.get('name') or 'anonymous'}: {excerpt}"
                )
            else:
                print(
                    "  ↩ replying to "
                    f"{short_hash(str(message['reply_to']))} "
                    "(message not stored yet)"
                )
        for line in str(message.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
        tally = message.get("poll")
        if isinstance(tally, dict) and is_channel_poll(message):
            options = tally.get("options") or []
            counts = tally.get("counts") or []
            total = tally.get("total") or 0
            closed = bool(tally.get("closed"))
            my_vote = tally.get("my_vote")
            state = " — closed" if closed else ""
            print(f"  Poll ({total} vote{'s' if total != 1 else ''}{state}):")
            for position, option in enumerate(options):
                count = counts[position] if position < len(counts) else 0
                mine = " <-- your vote" if my_vote == position else ""
                plural = "s" if count != 1 else ""
                print(f"    {position + 1}. {option} — {count} vote{plural}{mine}")
            print(f"    #{short_hash(str(message.get('hash', '')))}")
        summary = summarize_reactions(
            (reactions or {}).get(str(message.get("hash", "")), [])
        )
        if summary:
            joined = "  ".join(
                f"{item['reaction']}×{item['count']}" for item in summary
            )
            print(f"  {joined}")


def cmd_channel_join(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        membership = app.join(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if json_output:
        _emit(membership, True)
    else:
        print(f"joined #{membership['channel']}")
        print(f"  fetch its history with `tau channel sync {membership['channel']}`")
    return _EXIT_OK


def cmd_channel_leave(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        left = app.leave(args.channel)
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if json_output:
        _emit({"channel": name, "left": left}, True)
    else:
        if left:
            print(f"left #{name} (cached messages kept)")
        else:
            print(f"not a member of #{name}")
    return _EXIT_OK


def cmd_channel_list(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    memberships = app.memberships()
    pinned = load_pinned_set(config.pinned_path, "channel")
    unread = app.unread_counts()
    for membership in memberships:
        membership["pinned"] = membership["channel"] in pinned
        membership["unread"] = unread.get(membership["channel"], 0)
    memberships = sorted(memberships, key=lambda m: not m["pinned"])
    if json_output:
        _emit({"channels": memberships, "count": len(memberships)}, True)
        return _EXIT_OK
    if not memberships:
        print("No channels joined yet — join one with `tau channel join <name>`.")
        return _EXIT_OK
    for membership in memberships:
        count = membership["messages"]
        noun = "message" if count == 1 else "messages"
        pinned_suffix = " [pinned]" if membership["pinned"] else ""
        unread_count = membership["unread"]
        unread_suffix = (
            f", {unread_count} unread" if unread_count > 0 else ""
        )
        print(
            f"#{membership['channel']} — {count} cached {noun}{unread_suffix}, "
            f"joined {_ago(time.time() - membership['joined_at'])} ago"
            f"{pinned_suffix}"
        )
    return _EXIT_OK


def cmd_channel_show(args: argparse.Namespace) -> int:
    """Show one channel's full details: topic, counts, last message.

    Read-only: the channel store is read from disk, no node is
    started and nothing is sent.  A channel that was left still
    shows its cached messages with ``joined: no``.
    """
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    membership = next(
        (item for item in app.memberships() if item["channel"] == name), None
    )
    topic = app.topic(name)
    # The count is the same raw stored-row count `channel list`
    # reports, so a left channel (membership gone, cache kept) reads
    # the same number it showed while joined.
    conn = init_channel_db(config.channels_db_path)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE channel = ?", (name,)
        ).fetchone()
        message_count = int(row[0]) if row is not None else 0
        # A stored topic row proves the channel exists even when the
        # topic itself is hidden (blocked setter, unreadable blocklist).
        topic_row = conn.execute(
            "SELECT 1 FROM topics WHERE channel = ?", (name,)
        ).fetchone()
        has_topic_row = topic_row is not None
        if membership is None and message_count == 0 and not has_topic_row:
            print(f"error: unknown channel: {args.channel!r}", file=sys.stderr)
            return 2
        unread = app.unread_counts().get(name, 0)
        pinned = name in load_pinned_set(config.pinned_path, "channel")
        pinned_messages = app.pins(name)
        blocked = load_blocked_set(config.blocklist_path)
        # Votes and closes are not chat: they ride as messages so peers
        # can tally them, but they never stand in as the last message.
        # Page newest-first until a visible chat message turns up, so
        # blocked authors and poll records cannot push it off the page.
        # A block-list that cannot be read fails closed: the last
        # message stays hidden, like the timeline does, instead of
        # showing one we cannot vet.
        last: Optional[Dict[str, Any]] = None
        if blocked is not None:
            before: Optional[float] = None
            before_hash: Optional[str] = None
            while last is None:
                page, has_more = channel_history_page(
                    conn,
                    name,
                    limit=50,
                    before=before,
                    before_hash=before_hash,
                )
                for message in page:
                    if str(message.get("author", "")) in blocked:
                        continue
                    if is_channel_poll_vote(message) or is_channel_poll_close(
                        message
                    ):
                        continue
                    last = message
                    break
                if last is not None or not has_more or not page:
                    break
                tail = page[-1]
                before = tail["created_at"]
                before_hash = tail["hash"]
    finally:
        conn.close()
    if json_output:
        _emit(
            {
                "channel": {
                    "channel": name,
                    "joined": membership is not None,
                    "joined_at": (
                        membership["joined_at"] if membership else None
                    ),
                    "messages": message_count,
                    "unread": unread,
                    "pinned": pinned,
                    "pinned_messages": len(pinned_messages),
                    "topic": topic,
                    "last_message": last,
                }
            },
            True,
        )
        return _EXIT_OK
    registry = PeerRegistry(config.peers_path)
    print(f"#{name}")
    if membership is not None:
        print(
            f"  joined: yes "
            f"({_ago(time.time() - membership['joined_at'])} ago)"
        )
    else:
        print("  joined: no (cached messages kept)")
    if topic is not None:
        author_hash = str(topic.get("author") or "")
        author = registry.get(author_hash) if author_hash else None
        author_name = (
            author.display_name
            if author is not None
            else (author_hash[:8] if author_hash else "unknown")
        )
        try:
            updated = _ago(time.time() - float(topic["updated_at"]))
        except (OverflowError, OSError, ValueError, TypeError, KeyError):
            # Non-finite or out-of-range stored timestamps cannot age.
            updated = "unknown"
        print(f"  topic: {topic['topic']}")
        print(f"    set by {author_name} ({author_hash[:8]}), {updated} ago")
    else:
        print("  topic: no topic set")
    noun = "message" if message_count == 1 else "messages"
    print(f"  messages: {message_count} cached {noun}, {unread} unread")
    pin_noun = "message" if len(pinned_messages) == 1 else "messages"
    print(f"  pinned messages: {len(pinned_messages)} {pin_noun}")
    print(f"  conversation pinned: {'yes' if pinned else 'no'}")
    if last is None:
        print("  last message: no messages yet")
    else:
        author_hash = str(last.get("author") or "")
        author_name = last.get("name") or "anonymous"
        first_line = str(last.get("text") or "").splitlines()
        excerpt = first_line[0] if first_line else ""
        try:
            last_age = _ago(time.time() - float(last["created_at"]))
        except (OverflowError, OSError, ValueError, TypeError, KeyError):
            # Non-finite or out-of-range stored timestamps cannot age.
            last_age = "unknown"
        print(
            f"  last message: {author_name} ({short_hash(author_hash)}), "
            f"{last_age} ago: {excerpt}"
        )
    return _EXIT_OK


def cmd_channel_mark_read(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    try:
        result = app.mark_read(name)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if json_output:
        _emit(result, True)
    elif result["cleared"] == 0:
        print(f"#{name} has no unread messages.")
    else:
        cleared = result["cleared"]
        noun = "message" if cleared == 1 else "messages"
        print(f"marked #{name} read — cleared {cleared} unread {noun}.")
    return _EXIT_OK


def cmd_channel_send(args: argparse.Namespace) -> int:
    # Validate the pin before reading the message, so an invalid
    # --location fails at once instead of blocking on stdin.
    location = _location_from_args(args)
    if location is not None and getattr(args, "reply_to", None):
        print(
            "error: --location can only be attached to a top-level message, "
            "not a reply",
            file=sys.stderr,
        )
        return 2
    if location is not None and getattr(args, "delay", None) is not None:
        print(
            "error: --location cannot be combined with --delay — "
            "scheduled channel messages cannot carry a location pin yet",
            file=sys.stderr,
        )
        return 2
    text = _message_text_with_location(args, location)
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    expires_at = None
    expire_text = getattr(args, "expire", None)
    if expire_text is not None:
        try:
            expires_at = time.time() + _parse_duration(
                expire_text, flag="--expire"
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        if getattr(args, "no_publish", False):
            print(
                "error: --delay cannot be combined with --no-publish — "
                "a scheduled message is published later, not stored only",
                file=sys.stderr,
            )
            return 2
        try:
            delay = _parse_duration(
                delay_text,
                flag="--delay",
                max_seconds=MAX_SCHEDULED_CHANNEL_DELAY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        # Refuse to queue for a channel we have not joined: the
        # publish pass would fail every attempt the same way.
        conn = init_channel_db(config.channels_db_path)
        try:
            member = is_channel_member(conn, name)
        finally:
            conn.close()
        if not member:
            print(
                f"error: not a member of channel {name!r}; join it first",
                file=sys.stderr,
            )
            return 2
        try:
            entry = schedule_channel_message(
                config.scheduled_channel_messages_path,
                channel=name,
                text=text,
                publish_at=time.time() + delay,
                to=getattr(args, "to", None),
                ttl=int(getattr(args, "ttl", MAX_GOSSIP_TTL)),
                name=config.name,
                disappearing_path=config.disappearing_path,
            )
        except ScheduledChannelError as exc:
            raise NodeError(str(exc)) from exc
        if json_output:
            _emit({"scheduled": entry.to_dict()}, True)
        else:
            print(f"scheduled {entry.id} — publishes in {_ago(delay)}")
            print("  list it with `tau channel scheduled`; a node serving")
            print("  with --channels publishes it when the time comes.")
        return _EXIT_OK

    if getattr(args, "no_publish", False):
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        try:
            message = app.send(
                name,
                text,
                expires_at=expires_at,
                reply_to=getattr(args, "reply_to", None),
                location=location,
            )
        except ChannelError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            from .local_publish import publish_running

            result = publish_running(
                config.data_dir,
                channel=name,
                text=text,
                reply_to=getattr(args, "reply_to", None),
                to=getattr(args, "to", None),
                ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                timeout=float(getattr(args, "timeout", 15.0)),
                expires_at=expires_at,
                location=location,
            )
            message, published = result["message"], result["published"]
        else:
            try:
                app = ChannelApp(
                    config,
                    node.identity,
                    name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                app.attach(node)
                result = _publish_local_channel_message(
                    node,
                    channel=name,
                    text=text,
                    reply_to=getattr(args, "reply_to", None),
                    to=getattr(args, "to", None),
                    ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    timeout=float(getattr(args, "timeout", 15.0)),
                    expires_at=expires_at,
                    location=location,
                )
                message, published = result["message"], result["published"]
            finally:
                node.stop()

    if json_output:
        _emit({"message": message, "published": published}, True)
    else:
        print(f"sent to #{name} {short_hash(message['hash'])}")
        pin = clean_location(message.get("location"))
        if pin is not None:
            label = pin.get("name") or "Location"
            print(f"  📍 {label} — {pin['lat']}, {pin['lon']}")
            print(f"  {location_maps_url(pin)}")
        if message.get("expires_at") is not None:
            print(
                "  disappears in "
                f"{_ago(float(message['expires_at']) - time.time())}"
            )
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_channel_react(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if getattr(args, "clear", False):
        if getattr(args, "reaction", None):
            print("error: --clear takes no reaction", file=sys.stderr)
            return 2
        token = ""
    else:
        token = getattr(args, "reaction", None)
        if token is None:
            print("error: a reaction is required (or --clear to retract yours)", file=sys.stderr)
            return 2

    if getattr(args, "no_publish", False):
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        try:
            record = app.react(name, args.target, token)
        except ChannelError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            from .local_publish import publish_running

            result = publish_running(
                config.data_dir,
                channel=name,
                reaction_target=args.target,
                reaction=token,
                to=getattr(args, "to", None),
                ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            record, published = result["reaction"], result["published"]
        else:
            try:
                app = ChannelApp(
                    config,
                    node.identity,
                    name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                app.attach(node)
                result = _publish_local_channel_reaction(
                    node,
                    channel=name,
                    target=args.target,
                    reaction=token,
                    to=getattr(args, "to", None),
                    ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    timeout=float(getattr(args, "timeout", 15.0)),
                )
                record, published = result["reaction"], result["published"]
            finally:
                node.stop()

    if json_output:
        _emit({"reaction": record, "published": published}, True)
    else:
        if token:
            print(
                f"reacted {token} to {short_hash(record['target'])} in #{name}"
            )
        else:
            print(f"retracted your reaction to {short_hash(record['target'])} in #{name}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_channel_read(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    blocked = load_blocked_set(config.blocklist_path)
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    messages = app.history(name, limit=int(getattr(args, "limit", DEFAULT_CHANNEL_LIMIT)))
    if blocked is None:
        # The block-list failed closed: hide everything, like the
        # timeline does, instead of showing messages we cannot vet.
        messages = []
        reactions: Dict[str, List[Dict[str, Any]]] = {}
    else:
        messages = [
            message
            for message in messages
            if str(message.get("author", "")) not in blocked
        ]
        reactions = app.reactions(
            name, [str(message.get("hash", "")) for message in messages]
        )
    # Votes and closes are not chat: hide them from the listing and
    # render their tally on the poll instead, from the full history.
    messages = [
        message
        for message in messages
        if not is_channel_poll_vote(message)
        and not is_channel_poll_close(message)
    ]
    _attach_channel_poll_tallies(
        app, messages, _own_destination_hash(config), blocked=blocked
    )
    # History comes newest first; reading is oldest first.
    messages = list(reversed(messages))
    # Resolve each reply's parent from the local store so the reader
    # sees who and what is being answered.  A parent from a blocked
    # author stays hidden, like a blocked message itself; a parent
    # that has not arrived yet simply has no quote.
    conn = init_channel_db(config.channels_db_path)
    try:
        for message in messages:
            if not message.get("reply_to"):
                continue
            parent = channel_reply_parent(conn, message)
            if parent is None:
                continue
            if blocked is not None and str(parent.get("author", "")) in blocked:
                continue
            message["reply_parent"] = {
                "hash": parent["hash"],
                "name": parent.get("name", ""),
                "text": parent.get("text", ""),
            }
    finally:
        conn.close()
    _print_channel_messages(messages, json_output=json_output, channel=name, reactions=reactions)
    return _EXIT_OK


def cmd_channel_search(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    query = str(getattr(args, "query", "") or "")
    if not query.strip():
        print("error: search text must not be empty", file=sys.stderr)
        return 2
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    blocked = load_blocked_set(config.blocklist_path)
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    if blocked is None:
        # The block-list failed closed: hide everything, like the
        # timeline does, instead of showing messages we cannot vet.
        messages = []
    else:
        messages = app.search(
            name,
            query,
            limit=int(getattr(args, "limit", DEFAULT_CHANNEL_LIMIT)),
            exclude_authors=blocked,
        )
    # Search results stay newest first, like `tan search`: the most
    # recent match is the one the reader usually wants.
    if json_output:
        _emit(
            {
                "channel": name,
                "query": query.strip(),
                "messages": messages,
                "count": len(messages),
            },
            True,
        )
        return _EXIT_OK
    if not messages:
        print(f"No messages in #{name} matching {query.strip()!r}.")
        return _EXIT_OK
    _print_channel_messages(messages, json_output=False, channel=name)
    return _EXIT_OK


def cmd_search(args: argparse.Namespace) -> int:
    """Search stored chat history by keyword, newest first.

    Without a scope flag this searches everywhere: DM and group
    messages (sent and received) plus every joined channel, merged
    into one newest-first list.  ``--peer`` narrows to one DM peer,
    ``--group`` to one group (history spans renames, like
    ``tau messages``), ``--channel`` to one channel.  The match is a
    case-insensitive substring over message text, the same promise
    ``tau tan search`` and ``tau channel search`` make.

    Block rules match the history views: a blocked sender's received
    messages stay hidden while outbound stays visible, and a
    block-list that cannot be read hides received messages (fail
    closed) while outbound stays visible.  The query is trimmed once,
    so a padded search matches the same text in every source.
    Reactions on a group message are filtered against the group's
    pinned roster, like ``tau messages``.  Expired disappearing
    messages are never returned — the inbox purges them on load and
    channel search applies the history lifecycle.  A search is a
    read-only lookup: it advances no read cursor and sends no
    receipts, like ``tau messages --search``.
    """
    config = _make_config(args)
    json_output = _json_mode(args)
    query = str(getattr(args, "query", "") or "")
    if not query.strip():
        print("error: search text must not be empty", file=sys.stderr)
        return 2
    query = query.strip()
    limit = getattr(args, "limit", 20)
    if limit is not None and limit < 0:
        raise NodeError("--limit must not be negative")
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    # A corrupt mutes file fails open here, like `tau messages`: the
    # conversations count as unmuted (with a warning), so their unread
    # markers read the same in search as in the listing.
    muted_peers = muted_peers_for(config.muted_path)
    muted_groups = expand_muted_groups(
        muted_groups_for(config.muted_path),
        GroupStore(config.groups_path).list_groups(),
    )
    quiet = quiet_hours_active(config.quiet_hours_path)
    peer_hash = None
    group_ids = None
    channel_name = None
    if getattr(args, "peer", None):
        peer_hash = _resolve_inbox_peer(
            inbox, registry, args.peer, direction=None
        )
    if getattr(args, "group", None):
        store = GroupStore(config.groups_path)
        group = store.find(args.group)
        # History spans renames: messages stored under a predecessor
        # id still belong to this group.
        group_ids = store.lineage(group["id"])
    if getattr(args, "channel", None):
        try:
            channel_name = clean_channel_name(args.channel)
        except ChannelError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    blocked = load_blocked_set(config.blocklist_path)
    own_hash = _own_destination_hash(config)
    inbox_hits: List[Dict[str, Any]] = []
    channel_hits: List[Dict[str, Any]] = []
    app: Optional[ChannelApp] = None
    if channel_name is None:
        # A search is an explicit lookup, so ignored-group and
        # archived mail still shows, like `tau messages --search`.
        entries = inbox.list_messages(
            limit=None,
            peer=peer_hash,
            group=group_ids,
            query=query,
            direction=None,
            exclude_dm_from=muted_peers,
            exclude_group=muted_groups,
            quiet=quiet,
        )
        if peer_hash is not None:
            # A --peer filter is a DM filter: group posts from
            # that sender are the group's mail, not the DM thread.
            entries = [
                entry for entry in entries
                if entry.get("kind") != "group"
            ]
        # Blocked senders' history stays stored but is hidden locally,
        # like `tau messages`; unblocking restores the view.  The filter
        # applies to received messages only: sent messages are this
        # node's own, not a blocked sender's, so blocking the recipient
        # never hides them — and an unreadable list hides received hits
        # (fail closed) while leaving sent ones visible, the same rule
        # `tau pinned` applies.
        if blocked is None:
            entries = [
                entry for entry in entries if entry.get("direction") == "out"
            ]
        elif blocked:
            entries = [
                entry
                for entry in entries
                if entry.get("direction") == "out"
                or entry.get("destination") not in blocked
            ]
        # Votes and closes are not chat: they ride as records so
        # peers can tally them, but they never print as messages.
        inbox_hits = [
            entry
            for entry in entries
            if not is_group_poll_vote(entry)
            and not is_group_poll_close(entry)
            and not is_dm_poll_vote(entry)
            and not is_dm_poll_close(entry)
        ]
        # Poll questions render with their tally, like `tau messages`.
        _attach_group_poll_tallies(inbox, inbox_hits, own_hash)
        _attach_dm_poll_tallies(inbox, inbox_hits, own_hash)
        # A stored reaction from a non-member must never be displayed
        # on a group message, like `tau messages`: filter the listed
        # reactions against each group's pinned roster before anything
        # prints or is emitted as JSON.  With no pinned roster there
        # is nothing to check against.
        rosters = pinned_rosters(
            GroupStore(config.groups_path),
            {str(entry.get("group")) for entry in inbox_hits if entry.get("group")},
        )
        for entry in inbox_hits:
            group = entry.get("group")
            if group and entry.get("reactions"):
                entry["reactions"] = visible_reactions(
                    entry["reactions"], rosters.get(str(group))
                )
    if channel_name is not None or (peer_hash is None and group_ids is None):
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        names = (
            [channel_name]
            if channel_name is not None
            else [item["channel"] for item in app.memberships()]
        )
        for name in names:
            hits = app.search(
                name,
                query,
                limit=int(limit) if limit is not None else DEFAULT_CHANNEL_LIMIT,
                exclude_authors=blocked,
                # The list cannot be read: restrict the search to
                # this node's posts before the limit applies, so
                # newer peer matches cannot crowd an older own post
                # out of the results.
                only_authors={own_hash} if blocked is None else None,
            )
            if blocked is None:
                # The list cannot be read, so received mail stays
                # hidden (fail closed) — but this node's own posts
                # are not a blocked sender's, so they stay visible,
                # the same rule the DM branch applies.
                hits = [
                    hit
                    for hit in hits
                    if str(hit.get("author", "")) == (own_hash or "")
                ]
            channel_hits.extend(hits)
        channel_hits = [
            message
            for message in channel_hits
            if not is_channel_poll_vote(message)
            and not is_channel_poll_close(message)
        ]
        if channel_hits:
            _attach_channel_poll_tallies(
                app, channel_hits, own_hash, blocked=blocked
            )
            # Resolve each reply's parent from the local store so the
            # reader sees who and what is being answered, like
            # `tau channel read` does.
            conn = init_channel_db(config.channels_db_path)
            try:
                for message in channel_hits:
                    if not message.get("reply_to"):
                        continue
                    parent = channel_reply_parent(conn, message)
                    if parent is None:
                        continue
                    if blocked is None:
                        # The list cannot be read, so a quote must
                        # come from this node — a received parent
                        # cannot be vetted, the same fail-closed rule
                        # the hits themselves follow.
                        if str(parent.get("author", "")) != (own_hash or ""):
                            continue
                    elif str(parent.get("author", "")) in blocked:
                        continue
                    message["reply_parent"] = {
                        "hash": parent["hash"],
                        "name": parent.get("name", ""),
                        "text": parent.get("text", ""),
                    }
            finally:
                conn.close()
    for entry in inbox_hits:
        entry["source"] = "group" if entry.get("kind") == "group" else "dm"
    for message in channel_hits:
        message["source"] = "channel"
    # Merge newest first across sources: inbox entries stamp
    # received_at, channel messages created_at.
    def _hit_time(hit: Dict[str, Any]) -> float:
        stamp = (
            hit.get("created_at")
            if hit.get("source") == "channel"
            else hit.get("received_at")
        )
        try:
            return float(stamp or 0)
        except (TypeError, ValueError):
            return 0.0

    hits = sorted(
        [*inbox_hits, *channel_hits], key=_hit_time, reverse=True
    )
    shown = hits if limit is None else hits[:limit]
    if json_output:
        _emit(
            {"query": query, "messages": shown, "count": len(shown)},
            True,
        )
        return _EXIT_OK
    if not shown:
        if channel_name is not None:
            print(f"No messages in #{channel_name} matching {query!r}.")
        else:
            print(f"No messages matching {query!r}.")
        return _EXIT_OK
    for hit in shown:
        if hit.get("source") == "channel":
            print(f"Channel #{hit.get('channel')}")
            _print_channel_messages(
                [hit], json_output=False, channel=str(hit.get("channel"))
            )
            continue
        if hit.get("kind") == "group":
            print(f"Group {hit.get('group_name') or 'group'}")
        else:
            destination = str(hit.get("destination") or "")
            peer = registry.get(destination)
            name = (
                peer.display_name
                if peer is not None
                else hit.get("from") or "unknown"
            )
            print(f"DM with {name} ({short_hash(destination)})")
        _print_message_entry(
            inbox, registry, hit, blocked=blocked, own_hash=own_hash
        )
    return _EXIT_OK


def cmd_channel_edit(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if getattr(args, "no_publish", False):
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        try:
            notice = app.edit(name, args.target, text)
        except ChannelError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            from .local_publish import publish_running

            result = publish_running(
                config.data_dir,
                channel_edit={
                    "channel": name,
                    "target": args.target,
                    "text": text,
                },
                to=getattr(args, "to", None),
                ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            notice, published = result["edit"], result["published"]
        else:
            try:
                app = ChannelApp(
                    config,
                    node.identity,
                    name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                app.attach(node)
                result = _publish_local_channel_edit(
                    node,
                    channel=name,
                    target=args.target,
                    text=text,
                    to=getattr(args, "to", None),
                    ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    timeout=float(getattr(args, "timeout", 15.0)),
                )
                notice, published = result["edit"], result["published"]
            finally:
                node.stop()

    if json_output:
        _emit({"edit": notice, "published": published}, True)
    else:
        print(f"edited #{name} {short_hash(notice['target'])}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_channel_edits(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    edits = app.edits(name, limit=int(getattr(args, "limit", MAX_FEED_EDITS)))
    if json_output:
        _emit({"channel": name, "edits": edits, "count": len(edits)}, True)
        return _EXIT_OK
    if not edits:
        print(f"No stored edits in #{name}.")
        return _EXIT_OK
    for edit in edits:
        author = short_hash(str(edit.get("author", "")))
        target = short_hash(str(edit.get("target", "")))
        print(f"[{_stamp(edit.get('created_at'))}] {target} by {author}:")
        for line in str(edit.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK


def cmd_channel_pin(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        pin = app.pin(args.channel, args.digest)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if json_output:
        _emit(pin, True)
    else:
        print(f"pinned {short_hash(pin['hash'])} in #{pin['channel']}")
    return _EXIT_OK


def cmd_channel_unpin(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        unpinned = app.unpin(args.channel, args.digest)
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if json_output:
        _emit(
            {"channel": name, "hash": args.digest, "unpinned": unpinned}, True
        )
    else:
        if unpinned:
            print(f"unpinned {short_hash(args.digest)} in #{name}")
        else:
            print(f"not pinned in #{name}: {short_hash(args.digest)}")
    return _EXIT_OK


def cmd_channel_pins(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    pins = app.pins(name)
    if json_output:
        _emit({"channel": name, "pins": pins, "count": len(pins)}, True)
        return _EXIT_OK
    if not pins:
        print(f"No pinned messages in #{name}.")
        return _EXIT_OK
    _print_channel_messages(pins, json_output=False, channel=name)
    return _EXIT_OK


def cmd_channel_scheduled(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        queue = load_scheduled_channel_messages(
            config.scheduled_channel_messages_path
        )
    except ScheduledChannelError as exc:
        raise NodeError(str(exc)) from exc
    entries = queue.messages
    channel_filter = getattr(args, "channel", None)
    if channel_filter is not None:
        try:
            channel_filter = clean_channel_name(channel_filter)
        except ChannelError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        entries = tuple(
            entry for entry in entries if entry.channel == channel_filter
        )
    if json_output:
        _emit({"scheduled": [entry.to_dict() for entry in entries]}, True)
        return _EXIT_OK
    if not entries:
        if channel_filter is not None:
            print(f"No channel messages or polls scheduled for #{channel_filter}.")
        else:
            print("No channel messages or polls scheduled.")
        return _EXIT_OK
    now = time.time()
    for entry in entries:
        when = _stamp(entry.publish_at)
        if entry.publish_at <= now:
            when += " (due — publishes on the next serve pass)"
        else:
            when += f" (in {_ago(entry.publish_at - now)})"
        print(f"{entry.id}  #{entry.channel}  {when}")
        if entry.kind == "poll":
            print(f"  poll · {len(entry.options)} options")
        if entry.to:
            print(f"  to {', '.join(entry.to)}")
        for line in entry.text.splitlines() or [""]:
            print(f"  {line}")
        if entry.kind == "poll":
            for index, option in enumerate(entry.options, start=1):
                print(f"    {index}. {option}")
    return _EXIT_OK


def cmd_channel_cancel_scheduled(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    deleted_local = False

    def _delete_stored_message(entry: Any) -> None:
        # A publish attempt already stored the signed message locally
        # (see publish_due_scheduled_channel_messages): dropping only
        # the queue entry would leave it in the local channel history.
        # Peers that received a copy before the failure keep theirs —
        # a local delete cannot retract what already went out.
        nonlocal deleted_local
        if entry.message_hash is None:
            return
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        if app.delete_message(entry.message_hash):
            deleted_local = True

    try:
        entry = cancel_scheduled_channel_message(
            config.scheduled_channel_messages_path,
            args.target,
            on_cancel=_delete_stored_message,
        )
    except ScheduledChannelError as exc:
        raise NodeError(str(exc)) from exc
    if json_output:
        _emit(
            {"cancelled": entry.to_dict(), "deleted_local": deleted_local},
            True,
        )
    else:
        print(f"cancelled {entry.id} — it will not be published")
        if deleted_local:
            print("  also removed the stored message from the local channel")
    return _EXIT_OK


def cmd_channel_edit_scheduled(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    if getattr(args, "text", None) is None and getattr(args, "delay", None) is None:
        print(
            "error: nothing to change — pass --text and/or --delay",
            file=sys.stderr,
        )
        return 2
    publish_at = None
    if getattr(args, "delay", None) is not None:
        try:
            delay = _parse_duration(
                args.delay,
                flag="--delay",
                max_seconds=MAX_SCHEDULED_CHANNEL_DELAY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        publish_at = time.time() + delay
    try:
        entry = edit_scheduled_channel_message(
            config.scheduled_channel_messages_path,
            args.target,
            text=getattr(args, "text", None),
            publish_at=publish_at,
            name=config.name,
        )
    except ScheduledChannelError as exc:
        raise NodeError(str(exc)) from exc
    if json_output:
        _emit({"edited": entry.to_dict()}, True)
    else:
        print(f"edited {entry.id} — publishes {_stamp(entry.publish_at)}")
        for line in entry.text.splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK


def cmd_channel_sync(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_or_create_identity(config.identity_path)
    node = TauNode(config, name=config.name)
    node.start(announce=False)
    try:
        app = ChannelApp(
            config,
            identity,
            name=config.name,
            request_timeout=float(getattr(args, "timeout", 15.0)),
        )
        # Direct attach, not attach_app: a serving node already
        # carries Tan through node.app, and channels are a second
        # application layer on the same destination.
        app.attach(node)
        targets = None
        if getattr(args, "to", None):
            targets = [node.resolve_peer(target) for target in args.to]

        if getattr(args, "channel", None):
            try:
                name = clean_channel_name(args.channel)
            except ChannelError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 2
            summary = app.sync(
                node,
                name,
                targets=targets,
                limit=int(getattr(args, "limit", DEFAULT_CHANNEL_LIMIT)),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            summaries = {name: summary}
        else:
            result = app.sync_all(
                node,
                targets=targets,
                limit=int(getattr(args, "limit", DEFAULT_CHANNEL_LIMIT)),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            summaries = result["channels"]
    finally:
        node.stop()

    if _json_mode(args):
        _emit({"channels": summaries}, True)
    else:
        if not summaries:
            print("No channels joined yet — join one with `tau channel join <name>`.")
            return _EXIT_OK
        for name, summary in summaries.items():
            line = (
                f"#{name}: synced {summary['stored']} new message(s) "
                f"from {summary['peers']} peer(s); "
                f"{summary['duplicates']} duplicate(s)"
            )
            reactions = summary.get("reactions_stored", 0)
            if reactions:
                noun = "reaction" if reactions == 1 else "reactions"
                line += f"; {reactions} new {noun}"
            if summary.get("edited"):
                line += f"; {summary['edited']} edit(s) applied"
            if summary.get("retracted"):
                line += f"; {summary['retracted']} retraction(s)"
            print(line)
            for error in summary["errors"]:
                print(f"  {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def _send_channel_poll_message(
    args: argparse.Namespace,
    config: TauConfig,
    channel: str,
    text: str,
    *,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Post one channel poll, vote, or close, like ``cmd_channel_send``.

    The same three routes as a plain channel send (the serving node
    via IPC, else a one-shot local node, else local-only storage)
    with the poll payload attached.
    """
    name = clean_channel_name(channel)
    if getattr(args, "no_publish", False):
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        message = app.send(
            name,
            text,
            poll_options=poll_options,
            poll_vote=poll_vote,
            poll_close=poll_close,
        )
        return {"message": message, "published": None}
    node = TauNode(config, name=config.name)
    try:
        node.start(exclusive=True)
    except InstanceAlreadyRunning:
        from .local_publish import publish_running

        request: Dict[str, Any] = {
            "channel": name,
            "text": text,
            "to": getattr(args, "to", None),
            "ttl": getattr(args, "ttl", MAX_GOSSIP_TTL),
            "timeout": float(getattr(args, "timeout", 15.0)),
        }
        if poll_options is not None:
            request["poll_options"] = poll_options
        if poll_vote is not None:
            request["poll_vote"] = poll_vote
        if poll_close is not None:
            request["poll_close"] = poll_close
        return publish_running(config.data_dir, **request)
    try:
        app = ChannelApp(
            config,
            node.identity,
            name=config.name,
            request_timeout=float(getattr(args, "timeout", 15.0)),
        )
        app.attach(node)
        return _publish_local_channel_message(
            node,
            channel=name,
            text=text,
            to=getattr(args, "to", None),
            ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
            timeout=float(getattr(args, "timeout", 15.0)),
            poll_options=poll_options,
            poll_vote=poll_vote,
            poll_close=poll_close,
        )
    finally:
        node.stop()


def _report_channel_poll_send(
    args: argparse.Namespace,
    result: Dict[str, Any],
    summary: str,
) -> int:
    """Report a channel poll/vote/close send, like ``cmd_channel_send``."""
    message = result.get("message") or {}
    published = result.get("published")
    if _json_mode(args):
        _emit(result, True)
        return _EXIT_OK
    print(summary)
    print(f"  #{short_hash(str(message.get('hash', '')))}")
    if published is None:
        print("  stored locally (not published)")
    else:
        print(
            f"  published to {published['delivered']}/{published['peers']} peer(s)"
        )
        for error in published["errors"]:
            print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_channel_poll(args: argparse.Namespace) -> int:
    config = _make_config(args)
    try:
        options = clean_poll_options(list(args.options))
    except PostError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        if getattr(args, "no_publish", False):
            print(
                "error: --delay cannot be combined with --no-publish — "
                "a scheduled poll is published later, not stored only",
                file=sys.stderr,
            )
            return 2
        try:
            delay = _parse_duration(
                delay_text,
                flag="--delay",
                max_seconds=MAX_SCHEDULED_CHANNEL_DELAY,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        # Refuse to queue for a channel we have not joined: the
        # publish pass would fail every attempt the same way.
        conn = init_channel_db(config.channels_db_path)
        try:
            member = is_channel_member(conn, name)
        finally:
            conn.close()
        if not member:
            print(
                f"error: not a member of channel {name!r}; join it first",
                file=sys.stderr,
            )
            return 2
        try:
            entry = schedule_channel_poll(
                config.scheduled_channel_messages_path,
                channel=name,
                question=args.question,
                options=options,
                publish_at=time.time() + delay,
                to=getattr(args, "to", None),
                ttl=int(getattr(args, "ttl", MAX_GOSSIP_TTL)),
                name=config.name,
                disappearing_path=config.disappearing_path,
            )
        except ScheduledChannelError as exc:
            raise NodeError(str(exc)) from exc
        if _json_mode(args):
            _emit({"scheduled": entry.to_dict()}, True)
        else:
            print(f"scheduled {entry.id} — asks in {_ago(delay)}")
            print("  list it with `tau channel scheduled`; a node serving")
            print("  with --channels publishes it when the time comes.")
        return _EXIT_OK
    try:
        result = _send_channel_poll_message(
            args, config, name, args.question, poll_options=options
        )
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    summary = f"poll posted to #{name}: {args.question}"
    return _report_channel_poll_send(args, result, summary)


def _resolve_channel_poll_for_vote(
    args: argparse.Namespace, config: TauConfig, name: str
) -> Dict[str, Any]:
    """Resolve ``tau channel vote`` / ``close-poll`` to one stored poll.

    Raises :class:`ChannelError` when the reference is empty, matches
    nothing, or matches more than one poll.
    """
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    history = app.all_history(name)
    return resolve_channel_poll(history, args.poll)


def cmd_channel_vote(args: argparse.Namespace) -> int:
    config = _make_config(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        poll = _resolve_channel_poll_for_vote(args, config, name)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    history = app.all_history(name)
    own_hash = _own_destination_hash(config)
    tally = channel_poll_tally(history, poll, own_hash=own_hash)
    if tally["closed"]:
        print("error: this poll is closed", file=sys.stderr)
        return 2
    options = tally["options"]
    choice = str(args.option).strip()
    index: Optional[int] = None
    if choice.isdigit():
        index = int(choice) - 1
    else:
        for position, option in enumerate(options):
            if option == choice:
                index = position
                break
    if index is None or not 0 <= index < len(options):
        print(
            "error: option must be a number 1-"
            f"{len(options)} or one of: {', '.join(options)}",
            file=sys.stderr,
        )
        return 2
    identity = channel_poll_identity(poll)
    if identity is None:
        print(
            "error: this poll has no usable identity;"
            " it may predate polls or lack an identity",
            file=sys.stderr,
        )
        return 2
    try:
        result = _send_channel_poll_message(
            args,
            config,
            name,
            f"voted: {options[index]}",
            poll_vote={
                "target": {"author": identity[0], "sent_at": identity[1]},
                "option": index,
            },
        )
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    summary = f"voted for {options[index]!r} on {poll.get('text')!r} in #{name}"
    return _report_channel_poll_send(args, result, summary)


def cmd_channel_close_poll(args: argparse.Namespace) -> int:
    config = _make_config(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        poll = _resolve_channel_poll_for_vote(args, config, name)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    own_hash = _own_destination_hash(config)
    identity = channel_poll_identity(poll)
    if identity is None:
        print(
            "error: this poll has no usable identity;"
            " it may predate polls or lack an identity",
            file=sys.stderr,
        )
        return 2
    if own_hash is None or identity[0] != own_hash.lower():
        print(
            "error: only the poll's creator can close it",
            file=sys.stderr,
        )
        return 2
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    history = app.all_history(name)
    tally = channel_poll_tally(history, poll, own_hash=own_hash)
    if tally["closed"]:
        print("error: this poll is already closed", file=sys.stderr)
        return 2
    try:
        result = _send_channel_poll_message(
            args,
            config,
            name,
            "closed the poll",
            poll_close={"author": identity[0], "sent_at": identity[1]},
        )
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    summary = f"closed the poll {poll.get('text')!r} in #{name}"
    return _report_channel_poll_send(args, result, summary)


def cmd_channel_topic(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if getattr(args, "clear", False) and getattr(args, "text", None) is not None:
        print("error: --clear cannot be combined with topic text", file=sys.stderr)
        return 2

    # Show mode: no text, no --file, no --clear.  Setting from stdin
    # is deliberately not offered: an empty topic clears, so a bare
    # `tau channel topic NAME` with stdin closed must never be read
    # as a clear.
    show = (
        getattr(args, "text", None) is None
        and not getattr(args, "file", None)
        and not getattr(args, "clear", False)
    )
    if show:
        try:
            app = ChannelApp(config, _tan_identity(config), name=config.name)
            record = app.topic(name)
        except ChannelError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        if json_output:
            _emit({"channel": name, "topic": record}, True)
        elif record is None:
            print(f"#{name} has no topic set")
        else:
            print(f"#{name}: {record['topic']}")
        return _EXIT_OK

    try:
        topic = "" if getattr(args, "clear", False) else _read_message(args)
    except NodeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if not getattr(args, "clear", False) and not topic.strip():
        print(
            "error: topic text must not be empty "
            "(use --clear to remove the topic)",
            file=sys.stderr,
        )
        return 2

    if getattr(args, "no_publish", False):
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        try:
            result = app.set_topic(name, topic)
        except ChannelError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            from .local_publish import publish_running

            result = publish_running(
                config.data_dir,
                channel=name,
                topic=topic,
                to=getattr(args, "to", None),
                ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            published = result["published"]
        else:
            try:
                result = _publish_local_channel_topic(
                    node,
                    channel=name,
                    topic=topic,
                    to=getattr(args, "to", None),
                    ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    timeout=float(getattr(args, "timeout", 15.0)),
                )
                published = result["published"]
            finally:
                node.stop()

    if json_output:
        _emit(
            {
                "channel": name,
                "topic": result["topic"],
                "cleared": result["cleared"],
                "published": published,
            },
            True,
        )
    else:
        if result["cleared"]:
            print(f"cleared the topic of #{name}")
        else:
            print(f"topic of #{name}: {result['topic']['topic']}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_channel_retract(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        if getattr(args, "no_publish", False):
            app = ChannelApp(config, _tan_identity(config), name=config.name)
            result = app.retract(name, args.digest)
            notice, published = result["retraction"], result["published"]
        else:
            node = TauNode(config, name=config.name)
            try:
                node.start(exclusive=True)
            except InstanceAlreadyRunning:
                from .local_publish import publish_running

                result = publish_running(
                    config.data_dir,
                    retract_digest=args.digest,
                    channel=name,
                    to=getattr(args, "to", None),
                    ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    timeout=float(getattr(args, "timeout", 15.0)),
                )
                notice, published = result["retraction"], result["published"]
            else:
                try:
                    app = ChannelApp(
                        config,
                        node.identity,
                        name=config.name,
                        request_timeout=float(getattr(args, "timeout", 15.0)),
                    )
                    app.attach(node)
                    result = app.retract(
                        name,
                        args.digest,
                        publish=True,
                        targets=(
                            [node.resolve_peer(target) for target in args.to]
                            if getattr(args, "to", None)
                            else None
                        ),
                        ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    )
                    notice, published = result["retraction"], result["published"]
                finally:
                    node.stop()
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if json_output:
        _emit({"channel": name, "retraction": notice, "published": published}, True)
    else:
        print(f"retracted {short_hash(notice['hash'])} from #{name}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_channel_retractions(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    notices = app.retractions(name, limit=int(getattr(args, "limit", DEFAULT_CHANNEL_LIMIT)))
    if json_output:
        _emit({"channel": name, "retractions": notices, "count": len(notices)}, True)
        return _EXIT_OK
    if not notices:
        print(f"No retractions recorded in #{name}.")
        return _EXIT_OK
    for notice in notices:
        print(
            f"[{_stamp(notice.get('created_at'))}] "
            f"{short_hash(str(notice.get('author', '')))} retracted "
            f"{short_hash(str(notice.get('hash', '')))}"
        )
    return _EXIT_OK


def cmd_tan_export(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        if getattr(args, "all", False):
            if getattr(args, "author", None):
                raise TanError("--author and --all cannot be combined")
            document = export_entries(conn, all_authors=True)
            scope = "all authors"
        else:
            if getattr(args, "author", None):
                try:
                    author, _ = resolve_author(
                        conn,
                        args.author,
                        registry=PeerRegistry(config.peers_path),
                    )
                except TanError as exc:
                    # The peer registry does not know authors whose posts
                    # are only cached; fall back to the name on the posts.
                    try:
                        author, _ = resolve_cached_author(conn, args.author)
                    except TanError:
                        raise exc from None
            else:
                identity = _tan_identity(config)
                author = destination_hash(identity).hex()
            document = export_entries(conn, author=author)
            scope = f"author {short_hash(author)}"
    finally:
        conn.close()
    count = len(document["entries"])
    text = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True)
    if getattr(args, "output", None):
        atomic_write_text(args.output, text + "\n")
        print(f"Exported {count} entries ({scope}) to {args.output}")
    else:
        print(text)
    return _EXIT_OK


def cmd_tan_import(args: argparse.Namespace) -> int:
    config = _make_config(args)
    if str(args.file) == "-":
        raw = sys.stdin.read()
    else:
        raw = args.file.read_text(encoding="utf-8")
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TanError(f"export file is not valid JSON: {exc}") from exc
    conn = init_db(config.social_db_path)
    try:
        result = import_entries(conn, document)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(result, True)
    else:
        print(
            f"Imported {result['imported']} entries "
            f"({result['present']} already present, "
            f"{result['skipped']} skipped)"
        )
        for error in result["errors"]:
            ref = short_hash(error["hash"]) if error["hash"] else "?"
            print(f"  skipped {ref}: {error['error']}")
    return _EXIT_OK if result["skipped"] == 0 else _EXIT_ERROR


def _refresh_served_name(
    config: TauConfig, node: Any, args: argparse.Namespace, *, no_announce: bool
) -> None:
    """Pick up a rename that landed between config resolution and the serve lock.

    Once the serving lock is held no rename can write, so the stored name is
    stable here. A hand-edited ``node.json`` bypasses ``init``'s validation,
    so the re-read name is sanitised exactly as config resolution does.
    An explicit ``--name`` override wins over the stored name.
    """
    if getattr(args, "name", None) is not None:
        return
    stored_name = load_node_name(config.data_dir)
    if not stored_name:
        return
    stored_name = sanitize_name(stored_name)
    if stored_name != node.name:
        node.name = stored_name
        if not no_announce:
            node.announce()
def cmd_bundle_export(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    include_tan = bool(getattr(args, "tan", False))
    tan_conn = None
    try:
        if include_tan:
            tan_conn = init_db(config.social_db_path)
        document, summary = build_bundle(
            identity=identity,
            sender_name=config.name,
            mailbox=mailbox,
            registry=registry,
            tan_conn=tan_conn,
            include_tan=include_tan,
            max_bytes=int(
                getattr(args, "max_bytes", DEFAULT_MAX_BUNDLE_BYTES)
            ),
        )
    finally:
        if tan_conn is not None:
            tan_conn.close()
    atomic_write_bytes(
        args.file, serialize_bundle(document), restrict_parent=False
    )
    if _json_mode(args):
        _emit(summary, True)
    else:
        tan_text = (
            f" and {summary['tan']} Tan post(s)" if include_tan else ""
        )
        print(
            f"Exported bundle with {summary['mail']} sealed message(s)"
            f"{tan_text} ({summary['bytes']} bytes) to {args.file}"
        )
        for skipped in summary["skipped"]:
            print(f"  skipped {_safe_id_prefix(skipped['id'])}: {skipped['reason']}")
    return _EXIT_OK


def _bundle_tan_admit(
    config: TauConfig, identity: Any, tan_conn: Any
) -> Any:
    """Admission check for Tan posts carried in a bundle.

    Uses the same ruleset the network path applies.  A ruleset that
    cannot be parsed fails closed, denying remote posts, as ``TanApp``
    does.
    """
    try:
        ruleset = load_ruleset(config.tan_rules_path)
    except RulesError as exc:
        print(
            f"tau: warning: Tan ruleset unusable ({exc}); denying remote Tan posts",
            file=sys.stderr,
        )
        ruleset = Ruleset(default_allow=False)
    own_author = _author_from_identity(identity) if identity is not None else None
    followed: List[str] = []
    if ruleset.allow_followed:
        followed = [record["author"] for record in following(tan_conn)]
    return lambda author: ruleset.decide(
        author, own_author=own_author, followed=followed
    )


def cmd_bundle_import(args: argparse.Namespace) -> int:
    config = _make_config(args)
    max_bytes = int(getattr(args, "max_bytes", DEFAULT_MAX_BUNDLE_BYTES))
    if max_bytes <= 0:
        raise NodeError("max bundle size must be positive")
    # Read at most one byte past the cap, so an oversized input is
    # detected without being loaded whole.
    if str(args.file) == "-":
        raw = sys.stdin.buffer.read(max_bytes + 1)
    else:
        try:
            with Path(args.file).expanduser().open("rb") as handle:
                raw = handle.read(max_bytes + 1)
        except OSError as exc:
            raise NodeError(f"could not read {args.file}: {exc}") from exc
    if len(raw) > max_bytes:
        raise NodeError(f"bundle is over the {max_bytes} byte cap")
    document = parse_bundle(raw, max_bytes=max_bytes)
    mailbox = Mailbox(config.mailbox_path)
    tan_conn = None
    try:
        if document.get("tan") is not None:
            tan_conn = init_db(config.social_db_path)
        admit = None
        if tan_conn is not None:
            identity = load_identity(config.identity_path)
            admit = _bundle_tan_admit(config, identity, tan_conn)
        result = apply_bundle(
            mailbox=mailbox, tan_conn=tan_conn, document=document, admit=admit
        )
    finally:
        if tan_conn is not None:
            tan_conn.close()
    if _json_mode(args):
        _emit(result, True)
    else:
        tan = result["tan"]
        tan_text = ""
        if tan is not None:
            tan_text = (
                f"; Tan: {tan['imported']} imported, "
                f"{tan['present']} already present, "
                f"{tan['skipped']} skipped"
            )
        print(
            f"Imported {result['imported']} sealed message(s) "
            f"({result['present']} already held, "
            f"{result['skipped']} skipped){tan_text}"
        )
        for error in result["errors"]:
            ref = _safe_id_prefix(error["id"]) if error["id"] else "?"
            print(f"  skipped {ref}: {error['error']}")
    tan_clean = result["tan"] is None or result["tan"]["skipped"] == 0
    return _EXIT_OK if result["skipped"] == 0 and tan_clean else _EXIT_ERROR


def _bundle_inspect_names(
    config: TauConfig, summary: Dict[str, Any]
) -> Dict[str, str]:
    """Best-effort display names for the hashes in an inspect summary.

    Names are a convenience only — the registry is read, never
    written, and a missing or unreadable registry leaves every hash
    unnamed rather than failing the inspect.
    """
    try:
        registry = PeerRegistry(config.peers_path)
    except (OSError, ValueError):
        return {}
    hashes = set()
    for value in (
        [summary.get("exporter")]
        + [entry.get("to") for entry in summary.get("mail", [])]
        + [
            entry.get("author")
            for entry in summary.get("tan", [])
            if entry.get("author")
        ]
    ):
        # Only strings can be looked up; a malformed recipient (a
        # list or object in a signed bundle) stays unnamed instead
        # of crashing the lookup with an unhashable value.
        if isinstance(value, str) and value:
            hashes.add(value)
    names: Dict[str, str] = {}
    for hexhash in hashes:
        try:
            peer = registry.get(hexhash)
        except (ValueError, TypeError):
            continue
        if peer is not None:
            names[hexhash] = peer.display_name
    return names


def cmd_bundle_inspect(args: argparse.Namespace) -> int:
    """Describe a bundle file without importing it.

    Read-only by construction: no identity is loaded and no mailbox
    or Tan store is opened, so this works on a data directory that
    has never run ``tau init`` and leaves local state untouched.  The
    container signature is verified by :func:`parse_bundle` before
    anything is listed, so a tampered bundle is rejected whole.
    """
    max_bytes = int(getattr(args, "max_bytes", DEFAULT_MAX_BUNDLE_BYTES))
    if max_bytes <= 0:
        raise NodeError("max bundle size must be positive")
    # Read at most one byte past the cap, so an oversized input is
    # detected without being loaded whole.
    if str(args.file) == "-":
        raw = sys.stdin.buffer.read(max_bytes + 1)
    else:
        try:
            with Path(args.file).expanduser().open("rb") as handle:
                raw = handle.read(max_bytes + 1)
        except OSError as exc:
            raise NodeError(f"could not read {args.file}: {exc}") from exc
    if len(raw) > max_bytes:
        raise NodeError(f"bundle is over the {max_bytes} byte cap")
    document = parse_bundle(raw, max_bytes=max_bytes)
    summary = inspect_bundle(document)
    try:
        names = _bundle_inspect_names(_make_config(args), summary)
    except (OSError, ValueError):
        names = {}
    exporter = summary.get("exporter")
    summary["exporter_name"] = (
        names.get(exporter) if isinstance(exporter, str) else None
    )
    for entry in summary["mail"]:
        recipient = entry.get("to")
        entry["to_name"] = (
            names.get(recipient) if isinstance(recipient, str) else None
        )
    for entry in summary["tan"]:
        author = entry.get("author")
        entry["author_name"] = (
            names.get(author) if isinstance(author, str) else None
        )
    if _json_mode(args):
        _emit(summary, True)
        return _EXIT_OK

    def _when(value: Any) -> str:
        if isinstance(value, (int, float)) and math.isfinite(value):
            try:
                return datetime.fromtimestamp(float(value)).strftime(
                    "%Y-%m-%d %H:%M"
                )
            except (OverflowError, OSError, ValueError):
                return "out of range"
        return "unknown"

    def _who(hexhash: Any, name: Optional[str]) -> str:
        if name:
            return name
        if isinstance(hexhash, str) and hexhash:
            return _safe_id_prefix(hexhash)
        return "?"

    print(f"Bundle {summary.get('format')} v{summary.get('version')} — signature valid")
    print(f"  exporter: {_who(exporter, summary['exporter_name'])}")
    print(f"  created: {_when(summary.get('created_at'))}")
    print(f"Sealed mail ({summary['mail_count']}):")
    if not summary["mail"]:
        print("  (none)")
    for entry in summary["mail"]:
        if not entry["valid"]:
            ref = _safe_id_prefix(str(entry.get("id") or "")) or "?"
            print(
                f"  {ref} -> "
                f"{_who(entry.get('to'), entry.get('to_name'))} "
                f"— invalid: {entry.get('error')}"
            )
            continue
        print(
            f"  {_safe_id_prefix(str(entry.get('id') or ''))} -> "
            f"{_who(entry.get('to'), entry.get('to_name'))}, "
            f"expires {_when(entry.get('expires_at'))}"
        )
    print(f"Tan posts ({summary['tan_count']}):")
    if summary.get("tan_error"):
        print(f"  Tan section unusable: {summary['tan_error']}")
    if not summary["tan"]:
        print("  (none)")
    for entry in summary["tan"]:
        digest = entry.get("hash")
        label = _safe_id_prefix(str(digest)) if digest else "?"
        if entry["verified"]:
            print(
                f"  {label} {entry.get('kind')} by "
                f"{_who(entry.get('author'), entry.get('author_name'))} "
                f"({_when(entry.get('created_at'))}) — signature valid"
            )
        else:
            # Raw metadata survives verification failure, labelled
            # unverified: the author/kind/time are claims from the
            # entry itself, not authenticated facts.
            author = _who(entry.get("author"), entry.get("author_name"))
            if entry.get("author"):
                author += " (unverified)"
            detail = ""
            if entry.get("kind") or entry.get("created_at") is not None:
                detail = (
                    f" {_safe_text(str(entry.get('kind') or '?'))}"
                    f" ({_when(entry.get('created_at'))})"
                )
            print(
                f"  {label} by {author}{detail}"
                f" — signature invalid: "
                f"{_safe_text(str(entry.get('error') or ''))}"
            )
    return _EXIT_OK


def _serve_announce_config(args: argparse.Namespace) -> TauConfig:
    """Resolve the announce interval for `tau serve` (see #619).

    An explicit --announce-interval pins the value for the run; with
    no flag the saved setting applies, live — the announce loop
    re-reads it each cycle.
    """
    flag_interval = getattr(args, "announce_interval", None)
    if flag_interval is None:
        probe = _make_config(args)
        return _make_config(
            args,
            announce_interval=announce_interval(probe.data_dir),
            announce_interval_live=True,
        )
    return _make_config(args, announce_interval=max(0.0, float(flag_interval)))


def cmd_serve(args: argparse.Namespace) -> int:
    if getattr(args, "lxmf_propagation", False) and not getattr(
        args, "lxmf", False
    ):
        raise NodeError("--lxmf-propagation requires --lxmf")
    config = _serve_announce_config(args)

    bridge = None
    web_settings = None
    http_bind = getattr(args, "http", None)
    if http_bind:
        from .webapi import parse_bind

        web_dir = getattr(args, "web_dir", None) or _default_web_dir()
        try:
            host, port = parse_bind(http_bind)
        except ValueError as exc:
            raise NodeError(str(exc)) from exc
        web_settings = (host, port, web_dir)

    console_peer, console_message, console_file = _node_callbacks(args)
    node = TauNode(
        config,
        name=config.name,
        on_message=console_message,
        on_peer=console_peer,
        on_file=console_file,
    )
    if web_settings is not None:
        from .webapi import (
            WebBridge,
            WebBridgeError,
            check_web_console_bind,
            file_arrival_text,
        )

        host, port, web_dir = web_settings
        try:
            bridge = WebBridge(
                node,
                host=host,
                port=port,
                web_dir=web_dir,
                allow_remote=bool(getattr(args, "http_allow_remote", False)),
            )
        except WebBridgeError as exc:
            raise NodeError(str(exc)) from exc

        def on_peer(peer: Peer) -> None:
            console_peer(peer)
            bridge.record_peer(peer)

        def on_message(record: Dict[str, Any]) -> None:
            console_message(record)
            bridge.record_inbound(record)

        def on_message_record_only(record: Dict[str, Any]) -> None:
            # The live event is withheld, so the terminal stays quiet
            # too — but the console history is written, so the copy
            # is not invisible there.
            bridge.record_inbound(record, publish=False)

        def on_file(record: Dict[str, Any]) -> None:
            console_file(record)
            # The console lists messages, not files: record the arrival
            # as an inbound message so the file shows up in its feed.
            # The file metadata rides along so the feed can offer the
            # bytes back as a download (GET /api/files/<sender>/<name>).
            bridge.record_inbound(
                {
                    "id": record.get("id"),
                    "destination": record.get("destination"),
                    "from": record.get("from"),
                    "text": file_arrival_text(record),
                    "sent_at": record.get("sent_at"),
                    "received_at": record.get("received_at"),
                    "file": {
                        "name": record.get("name"),
                        "size": record.get("size"),
                        "sha256": record.get("sha256"),
                        **(
                            {"forwarded_from": record["forwarded_from"]}
                            if isinstance(record.get("forwarded_from"), dict)
                            else {}
                        ),
                    },
                }
            )

        node.on_peer = on_peer
        node.on_message = on_message
        node.on_message_record_only = on_message_record_only
        node.on_file = on_file

        def on_retract(record: Dict[str, Any]) -> None:
            # The node already deleted the inbox record; drop the
            # console's own copy so a retracted DM leaves the feed
            # and never comes back after a restart.  A queued retract
            # deletes this node's own sent copy, so the dispatch
            # (inbound vs sent) lives in _drop_retracted_console_copy.
            _drop_retracted_console_copy(bridge, record)

        node.on_retract = on_retract
    # The data-directory lock is the more specific guard, so probe it
    # before the port check: a second serve on the same directory
    # reports the instance error even though it would also trip the
    # port check below. node.start(exclusive=True) re-checks it
    # authoritatively when it takes the lock for real.
    probe = InstanceLock(config.instance_lock_path, config.data_dir)
    probe.acquire()
    probe.release()
    # A port already held by another node would leave Reticulum's TCP
    # server interface silently dead (panic_on_interface_error = No),
    # so fail fast here with the port named instead of serving deaf.
    check_tcp_listen_ports(config)
    # Same for the web console: its bind used to fail only after the node
    # had already started, with a bare "[Errno 98] Address already in
    # use". Probe first so the error names the console address.
    if web_settings is not None:
        host, port, _ = web_settings
        try:
            check_web_console_bind(host, port)
        except WebBridgeError as exc:
            raise NodeError(str(exc)) from exc
    no_announce = bool(getattr(args, "no_announce", False))
    node.start(
        announce=not no_announce,
        exclusive=True,
        lxmf=bool(getattr(args, "lxmf", False)),
        lxmf_propagation=bool(getattr(args, "lxmf_propagation", False)),
    )
    _refresh_served_name(config, node, args, no_announce=no_announce)

    social_app = None
    if getattr(args, "social", False):

        def on_tan_post(record: Dict[str, Any]) -> None:
            if _json_mode(args):
                _emit_event(args, "post", post=record)
            else:
                name = record.get("name") or "anonymous"
                author = short_hash(str(record.get("author", "")))
                print(
                    f"[{_stamp(record.get('created_at'))}] post from {name} ({author}):",
                    flush=True,
                )
                for line in str(record.get("text", "")).splitlines() or [""]:
                    print(f"  {line}", flush=True)
            if bridge is not None:
                try:
                    bridge.record_tan_post(record)
                except Exception:  # pragma: no cover - optional web layer
                    log.debug(
                        "Could not forward Tan post to web console", exc_info=True
                    )

        social_app = TanApp(
            config,
            node.identity,
            # The node's name, not the resolved config name: a rename that
            # landed during startup refreshes node.name (see
            # _refresh_served_name), and posts must carry the same name the
            # node announces with.
            name=node.name,
            on_post=on_tan_post,
        )
        node.attach_app(social_app)

    channel_app = None
    if getattr(args, "channels", False):

        def on_channel_message(record: Dict[str, Any]) -> None:
            if _json_mode(args):
                _emit_event(args, "channel-message", message=record)
            else:
                name = record.get("name") or "anonymous"
                author = short_hash(str(record.get("author", "")))
                print(
                    f"[#{record.get('channel', '?')}] "
                    f"[{_stamp(record.get('created_at'))}] {name} ({author}):",
                    flush=True,
                )
                for line in str(record.get("text", "")).splitlines() or [""]:
                    print(f"  {line}", flush=True)
            if bridge is not None:
                try:
                    bridge.record_channel_message(record)
                except Exception:  # pragma: no cover - optional web layer
                    log.debug(
                        "Could not forward channel message to web console",
                        exc_info=True,
                    )

        # Direct attach, not attach_app: the node already carries Tan
        # through node.app when --social is also given, and channels
        # are a second application layer on the same destination.
        channel_app = ChannelApp(
            config,
            node.identity,
            name=node.name,
            on_message=on_channel_message,
        )
        channel_app.attach(node)
        node.channel_app = channel_app

    console_url = None
    if bridge is not None:
        try:
            console_url = bridge.start()
        except WebBridgeError as exc:
            # The pre-start probe covers the usual collision; a bind race
            # can still land here. Stop the node cleanly instead of
            # exiting with it running, and keep the named error.
            node.stop()
            raise NodeError(str(exc)) from exc

    stop = threading.Event()
    previous_handlers = _install_stop_handlers(stop)

    social_interval = max(0.0, float(getattr(args, "social_sync_interval", 0.0) or 0.0))
    if social_app is not None and social_interval > 0:

        def social_sync_loop() -> None:
            while not stop.wait(social_interval):
                try:
                    summary = social_app.sync(
                        node, limit=DEFAULT_FEED_LIMIT, explore=True
                    )
                    if summary.get("stored"):
                        log.info("Tan sync stored %s new post(s)", summary["stored"])
                except Exception:  # pragma: no cover - network dependent
                    log.warning("Tan background sync failed", exc_info=True)

        threading.Thread(
            target=social_sync_loop, name="tau-tan-sync", daemon=True
        ).start()

    publish_server = None
    if (social_app is not None or channel_app is not None) and hasattr(
        socket, "AF_UNIX"
    ):
        from .local_publish import PublishServer

        try:
            publish_server = PublishServer(
                config.data_dir,
                lambda **request: _publish_local_post(node, **request),
            )
        except BaseException:
            if bridge is not None:
                bridge.stop()
            node.stop()
            _restore_stop_handlers(previous_handlers)
            raise

    media_server = None
    if social_app is not None and hasattr(socket, "AF_UNIX"):
        from .local_media import MediaServer

        try:
            media_server = MediaServer(
                config.data_dir,
                lambda **request: node.fetch_tan_media(**request),
            )
        except BaseException:
            if publish_server is not None:
                publish_server.stop()
            if bridge is not None:
                bridge.stop()
            node.stop()
            _restore_stop_handlers(previous_handlers)
            raise

    send_server = None
    fanout_server = None
    if hasattr(socket, "AF_UNIX"):
        from .local_send import FanoutServer, SendServer

        try:
            send_server = SendServer(
                config.data_dir,
                lambda **request: _dispatch_local_send(
                    node, bridge=bridge, **request
                ),
            )
        except Exception:
            # The send socket is best-effort: a data directory too long
            # for a Unix socket path, or a filesystem without socket
            # support, must not stop the node from serving. One-shot
            # sends fail closed in that case — the lock probe refuses to
            # start a second instance on this identity — so no send is
            # lost silently.
            log.warning("Could not start the local send server", exc_info=True)
        if send_server is not None:
            try:
                # The fan-out worker: group sends, retractions, edits
                # and typing run on their own socket so a slow fan-out
                # cannot block the main send socket (issue #455). It
                # shares the dispatch — and the node — with the main
                # server. Best-effort like the main server: when it
                # cannot bind, fan-out ops fall back to the main socket.
                fanout_server = FanoutServer(
                    config.data_dir,
                    lambda **request: _dispatch_local_send(
                        node, bridge=bridge, **request
                    ),
                )
            except Exception:
                log.warning(
                    "Could not start the local fan-out server", exc_info=True
                )

    info = node.info()
    send_socket = send_server is not None
    if _json_mode(args):
        _emit_event(args, "ready", node=info, send_socket=send_socket)
        if console_url is not None:
            _emit_event(args, "web", url=console_url, api=f"{console_url}api/")
        if social_app is not None:
            _emit_event(
                args,
                "social",
                enabled=True,
                db=str(config.social_db_path),
                feed="/tan/feed",
            )
        if channel_app is not None:
            _emit_event(
                args,
                "channels",
                enabled=True,
                db=str(config.channels_db_path),
                feed="/tau/channel/feed",
            )
    else:
        print(f'tau node "{info["name"]}" ready', flush=True)
        print(f"  destination: {info['destination']}")
        print(f"  rns:         {info['rns_version']}")
        lxmf_info = info.get("lxmf") or {}
        if lxmf_info.get("enabled"):
            print(f"  lxmf:        delivery {lxmf_info.get('delivery')}")
            if lxmf_info.get("propagation"):
                print(
                    f"  lxmf:        propagation {lxmf_info.get('propagation')}"
                )
        print(f"  data dir:    {config.data_dir}")
        if not send_socket and hasattr(socket, "AF_UNIX"):
            # Best-effort serve without a send socket: say so up front,
            # so the operator can see one-shot sends will fail while
            # this node serves instead of wondering why.
            print(
                "  send:        socket unavailable — tau send fails while "
                "this node serves"
            )
        mailbox_counts = info.get("mailbox") or {}
        if mailbox_counts.get("outbox") or mailbox_counts.get("held"):
            print(
                f"  mail:        {mailbox_counts.get('outbox', 0)} queued, "
                f"{mailbox_counts.get('held', 0)} held for others"
            )
        if console_url is not None:
            print(f"  console:     {console_url}")
            print(f"  api:         {console_url}api/")
        if social_app is not None:
            print(f"  social:      Tan feed at /tan/feed")
            if social_interval > 0:
                print(f"  social sync: every {social_interval:g}s")
        print("Listening for peers and messages. Press Ctrl-C to stop.", flush=True)

    duration = getattr(args, "duration", None)
    try:
        if duration is not None:
            stop.wait(max(0.0, float(duration)))
        else:
            while not stop.wait(0.5):
                pass
    finally:
        if fanout_server is not None:
            fanout_server.stop()
        if send_server is not None:
            send_server.stop()
        if publish_server is not None:
            publish_server.stop()
        if media_server is not None:
            media_server.stop()
        if bridge is not None:
            bridge.stop()
        node.stop()
        _restore_stop_handlers(previous_handlers)
    return _EXIT_OK


def cmd_discover(args: argparse.Namespace) -> int:
    config = _make_config(args, announce_interval=0.0)

    def on_peer(peer: Peer) -> None:
        if not _json_mode(args):
            print(
                f"[{_stamp()}] {peer.display_name} ({peer.hexhash}) hops={peer.hops}",
                flush=True,
            )

    node = TauNode(config, name=config.name, on_peer=on_peer)
    node.start(announce=False)
    duration = max(0.0, float(getattr(args, "duration", 15.0)))
    stop = threading.Event()
    previous_handlers = _install_stop_handlers(stop)
    try:
        stop.wait(duration)
    finally:
        node.stop()
        _restore_stop_handlers(previous_handlers)

    max_age_days = getattr(args, "max_age", None)
    max_age = float(max_age_days) * 86400.0 if max_age_days is not None else None
    peers = node.registry.all(max_age=max_age)
    lookalikes = _lookalike_map(node.registry, peers)
    if _json_mode(args):
        payload = [peer.to_dict() for peer in peers]
        for entry in payload:
            collided = lookalikes.get(entry["destination"])
            entry["lookalike_of"] = (
                collided.display_name if collided is not None else None
            )
        _emit(
            {
                "peers": payload,
                "count": len(peers),
                "duration": duration,
            },
            True,
        )
    else:
        print()
        _print_peer_table(peers, lookalikes=lookalikes)
        print(f"\n{len(peers)} peer(s) in {duration:.1f}s")
    return _EXIT_OK


def _print_heard_table(
    records: List[Dict[str, Any]], *, now: Optional[float] = None
) -> None:
    if not records:
        print("No non-tau announces heard.")
        return
    timestamp = now if now is not None else time.time()
    header = ("DESTINATION", "HOPS", "SEEN", "LAST SEEN")
    table: List[tuple] = [header]
    for record in records:
        table.append(
            (
                str(record.get("destination") or ""),
                str(record.get("hops", 0)),
                str(record.get("count", 1)),
                _ago(max(0.0, timestamp - float(record.get("last_seen", timestamp)))),
            )
        )
    widths = [max(len(row[i]) for row in table) for i in range(len(header))]
    for row in table:
        print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))


def cmd_heard(args: argparse.Namespace) -> int:
    duration = max(0.0, float(getattr(args, "duration", 15.0)))
    if not math.isfinite(duration):
        raise ValueError("--duration must be a finite number of seconds")
    config = _make_config(args, announce_interval=0.0)

    def on_foreign(record: Dict[str, Any]) -> None:
        if not _json_mode(args):
            print(
                f"[{_stamp()}] {record.get('destination')} "
                f"hops={record.get('hops', 0)}",
                flush=True,
            )

    node = TauNode(config, name=config.name, on_foreign=on_foreign)
    node.start(announce=False)
    stop = threading.Event()
    previous_handlers = _install_stop_handlers(stop)
    try:
        stop.wait(duration)
    finally:
        node.stop()
        _restore_stop_handlers(previous_handlers)

    heard = node.foreign_announces()
    if _json_mode(args):
        _emit(
            {
                "heard": heard,
                "count": len(heard),
                "duration": duration,
            },
            True,
        )
    else:
        print()
        _print_heard_table(heard)
        print(f"\n{len(heard)} non-tau announce(s) in {duration:.1f}s")
    return _EXIT_OK


def cmd_peers(args: argparse.Namespace) -> int:
    config = _make_config(args)
    if getattr(args, "exchange", False):
        return cmd_peers_exchange(args, config)
    registry = PeerRegistry(config.peers_path)
    max_age_days = getattr(args, "max_age", None)
    max_age = float(max_age_days) * 86400.0 if max_age_days is not None else None
    peers = registry.all(max_age=max_age)
    # Pinned conversations stay at the top; the registry order is kept
    # within each half, so the sort is stable.
    pinned = load_pinned_set(config.pinned_path, "peer")
    peers = sorted(peers, key=lambda peer: peer.hexhash not in pinned)
    safety_numbers: Optional[Dict[str, str]] = None
    if getattr(args, "safety_number", False):
        identity = load_identity(config.identity_path)
        if identity is None:
            raise NodeError(
                f"no identity at {config.identity_path}; run `tau init` first"
            )
        local_key = identity.get_public_key()
        safety_numbers = {
            peer.hexhash: safety_number(local_key, peer.public_key)
            for peer in peers
        }
    if _json_mode(args):
        payload = [peer.to_dict() for peer in peers]
        lookalikes = _lookalike_map(registry, peers)
        for entry in payload:
            collided = lookalikes.get(entry["destination"])
            entry["lookalike_of"] = (
                collided.display_name if collided is not None else None
            )
            entry["pinned"] = entry["destination"] in pinned
        if safety_numbers is not None:
            for entry in payload:
                entry["safety_number"] = safety_numbers[entry["destination"]]
        _emit({"peers": payload, "count": len(peers)}, True)
    else:
        _print_peer_table(
            peers,
            safety_numbers=safety_numbers,
            lookalikes=_lookalike_map(registry, peers),
            pinned=pinned,
        )
    return _EXIT_OK


def cmd_peers_verify(args: argparse.Namespace) -> int:
    """Mark a peer verified after comparing safety numbers out of band."""
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )
    registry = PeerRegistry(config.peers_path)
    try:
        peer = registry.resolve(args.target)
    except PeerError as exc:
        raise NodeError(str(exc)) from exc
    if peer is None:
        raise NodeError(f"no peer matches {args.target!r}")
    number = safety_number(identity.get_public_key(), peer.public_key)
    previously_verified = peer.is_verified
    previous_fingerprint = peer.verified_fingerprint
    registry.mark_verified(peer.destination_hash, number)
    registry.save()
    if _json_mode(args):
        _emit(
            {
                "peer": peer.hexhash,
                "name": peer.display_name,
                "safety_number": number,
                "verified_at": peer.verified_at,
                "reverified": previously_verified,
                "fingerprint_changed": previously_verified
                and previous_fingerprint != number,
            },
            True,
        )
    else:
        print(f"Verified {peer.display_name} ({short_hash(peer.hexhash)}).")
        print(f"  safety number: {number}")
        if previously_verified and previous_fingerprint != number:
            print(
                "  note: the recorded safety number changed since the last "
                "verification (the local identity changed); updated."
            )
        print(
            "  Compare this number with the peer over another channel; "
            "if it matches, the key is theirs."
        )
    return _EXIT_OK


def cmd_peers_forget(args: argparse.Namespace) -> int:
    """Remove one peer from the local registry.

    A serving node keeps its own in-memory registry and rewrites the file
    on its next save, so forgetting refuses while one is running on this
    data directory.  The mailbox transaction is held across the fresh
    safety checks and the registry removal, mirroring ``_save_peers``:
    a one-shot send cannot queue mail for the peer between the check
    and the removal.  Mail sweeps iterate registered peers, so forgetting
    a peer with queued mail would strand those messages until it is
    rediscovered (or they expire): refuse unless ``--force`` is passed.
    """
    config = _make_config(args)
    config.data_dir.mkdir(parents=True, exist_ok=True)
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    try:
        lock.acquire()
    except InstanceAlreadyRunning as exc:
        raise NodeError(
            "a tau node is already serving on this data directory; "
            "stop it before forgetting a peer"
        ) from exc
    try:
        mailbox = Mailbox(config.mailbox_path)
        with mailbox.transaction():
            registry = PeerRegistry(config.peers_path)
            try:
                peer = registry.resolve(args.target)
            except PeerError as exc:
                raise NodeError(str(exc)) from exc
            if peer is None:
                raise NodeError(f"no peer matches {args.target!r}")
            queued = mailbox.outbox_list(to=peer.destination_hash)
            if queued and not getattr(args, "force", False):
                raise NodeError(
                    f"{peer.display_name} still has {len(queued)} queued "
                    "message(s); forgetting this peer would strand them "
                    "until it is rediscovered. Deliver or cancel the mail "
                    "first, or pass --force to forget anyway."
                )
            if peer.hexhash in mailbox.pending_ack_holders():
                raise NodeError(
                    f"{peer.display_name} still has mail acknowledgements owed; "
                    "wait for the next mail sweep to send them, then try again"
                )
            registry.remove(peer.destination_hash)
            registry.save()
    finally:
        lock.release()
    if _json_mode(args):
        _emit(
            {
                "peer": peer.hexhash,
                "name": peer.display_name,
                "forgotten": peer.to_dict(),
                "queued_messages": len(queued),
            },
            True,
        )
    else:
        print(f"Forgot {peer.display_name} ({short_hash(peer.hexhash)}).")
        if queued:
            print(
                f"  warning: {len(queued)} queued message(s) for this peer "
                "will not be attempted until it is rediscovered."
            )
        print("  The next genuine announce from this peer re-adds it.")
    return _EXIT_OK


def cmd_peers_alias(args: argparse.Namespace) -> int:
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    peer = registry.set_alias(args.peer, args.alias)
    if _json_mode(args):
        _emit(peer.to_dict(), True)
    else:
        print(f"alias {peer.alias!r} set for {peer.name} ({peer.hexhash[:8]})")
    return _EXIT_OK


def cmd_peers_unalias(args: argparse.Namespace) -> int:
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    peer = registry.clear_alias(args.peer)
    if _json_mode(args):
        _emit(peer.to_dict(), True)
    else:
        print(f"removed alias for {peer.name} ({peer.hexhash[:8]})")
    return _EXIT_OK


def cmd_peers_note(args: argparse.Namespace) -> int:
    """Save, show, or clear a private note on a known peer.

    Local-only: the note lives on the peer record in the registry and
    is never sent to anyone.  With TEXT it saves the note, with
    ``--clear`` it removes it, and with neither it prints the current
    note (or that none is set).
    """
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    if args.clear and args.text is not None:
        raise PeerError("cannot save a note and --clear it at once")
    if args.clear:
        peer = registry.clear_note(args.peer)
        if _json_mode(args):
            _emit(peer.to_dict(), True)
        else:
            print(f"removed note for {peer.name} ({peer.hexhash[:8]})")
        return _EXIT_OK
    if args.text is not None:
        peer = registry.set_note(args.peer, args.text)
        if _json_mode(args):
            _emit(peer.to_dict(), True)
        else:
            print(
                f"note for {peer.name} ({peer.hexhash[:8]}): {peer.note}"
            )
        return _EXIT_OK
    peer = registry.resolve(args.peer)
    if peer is None:
        raise PeerError(f"unknown peer {args.peer!r}")
    if _json_mode(args):
        _emit(peer.to_dict(), True)
    elif peer.note:
        print(f"note for {peer.name} ({peer.hexhash[:8]}): {peer.note}")
    else:
        print(f"no note set for {peer.name} ({peer.hexhash[:8]})")
    return _EXIT_OK


def cmd_peers_show(args: argparse.Namespace) -> int:
    """Show everything the local stores know about one peer.

    Read-only and offline: no node is started and nothing is sent.  The
    registry answers identity and liveness; the mailbox and inbox answer
    how much mail and how many messages involve this peer.
    """
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    target = args.target
    peer = registry.resolve(target)
    if peer is None:
        raise PeerError(f"unknown peer: {target!r}")
    mailbox = Mailbox(config.mailbox_path)
    inbox = Inbox(config.inbox_path)
    queued = mailbox.outbox_list(to=peer.destination_hash)
    held = mailbox.held_for(peer.destination_hash)
    received = inbox.list_messages(peer=peer.hexhash, limit=None)
    now = time.time()
    stored_profile = ProfileStore(config.profiles_path).get(peer.hexhash)
    payload = {
        "name": peer.name,
        "destination": peer.hexhash,
        "lxmf": peer.lxmf_hash,
        "public_key": peer.public_key.hex(),
        "first_seen": peer.first_seen,
        "last_seen": peer.last_seen,
        "hops": peer.hops,
        "protocol": peer.protocol,
        "caps": list(peer.caps),
        "via_exchange": peer.via_exchange,
        "alias": peer.alias,
        "note": peer.note,
        "queued": len(queued),
        "held": len(held),
        "received": len(received),
        "profile": public_profile(stored_profile),
    }
    if _json_mode(args):
        _emit(payload, True)
        return _EXIT_OK
    print(f"name:         {peer.display_name}")
    print(f"destination:  {peer.hexhash}")
    print(f"note:         {peer.note or '—'}")
    print(f"lxmf:         {peer.lxmf_hash or '—'}")
    print(f"public key:   {peer.public_key.hex()}")
    print(
        f"first seen:   {_format_message_time(peer.first_seen)}"
        f" ({_ago(now - peer.first_seen)} ago)"
    )
    print(
        f"last seen:    {_format_message_time(peer.last_seen)}"
        f" ({_ago(now - peer.last_seen)} ago)"
    )
    print(f"hops:         {peer.hops}")
    print(f"protocol:     {peer.protocol}")
    print(f"caps:         {', '.join(peer.caps) if peer.caps else '—'}")
    print(
        "via exchange: "
        + ("yes — never heard directly" if peer.via_exchange else "no")
    )
    print(f"queued mail:  {len(queued)} message(s) waiting for this peer")
    print(f"held mail:    {len(held)} sealed message(s) held for this peer")
    print(f"received:     {len(received)} message(s) from this peer in the inbox")
    if stored_profile is not None:
        print("profile:")
        _print_profile(stored_profile, indent="  ")
    return _EXIT_OK


def _print_profile(doc: Dict[str, Any], *, indent: str = "") -> None:
    """Print one profile document in text form."""
    print(f"{indent}name:    {doc['name']}")
    print(f"{indent}bio:     {doc['bio'] or '—'}")
    print(f"{indent}avatar:  {doc['avatar'] or '—'}")
    if doc["links"]:
        for link in doc["links"]:
            print(f"{indent}link:    {link}")
    else:
        print(f"{indent}link:    —")
    print(
        f"{indent}version: {doc['version']} "
        f"({_format_message_time(doc['created_at'])})"
    )


def cmd_profile_set(args: argparse.Namespace) -> int:
    """Publish a new version of this node's signed public profile.

    A flag left out keeps the value the current profile carries (or
    the node name for the display name when no profile is published
    yet), so a small change does not wipe the rest of the card.  The
    new version is written to the data directory; a serving node
    picks it up at its next announce, and peers that see the newer
    version fetch the document over a link.
    """
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )
    existing = load_local_profile(config.profile_path)
    name = (
        args.display_name
        if args.display_name is not None
        else (existing["name"] if existing is not None else config.name)
    )
    bio = (
        args.bio
        if args.bio is not None
        else (existing["bio"] if existing is not None else "")
    )
    avatar = (
        resolve_avatar(args.avatar)
        if args.avatar is not None
        else (existing["avatar"] if existing is not None else None)
    )
    links = (
        list(args.links)
        if args.links is not None
        else (list(existing["links"]) if existing is not None else [])
    )
    version = existing["version"] + 1 if existing is not None else 1
    try:
        doc = build_profile(
            identity,
            name=name,
            bio=bio,
            avatar=avatar,
            links=links,
            version=version,
        )
    except ProfileError as exc:
        raise NodeError(str(exc)) from exc
    save_local_profile(config.profile_path, doc)
    if _json_mode(args):
        _emit(public_profile(doc), True)
    else:
        print(f"Published profile version {version} for {doc['name']}.")
        _print_profile(doc, indent="  ")
        print(
            "  Peers will see it at the next announce "
            "(within ~5 minutes), or fetch it on demand."
        )
    return _EXIT_OK


def cmd_profile_show(args: argparse.Namespace) -> int:
    """Show this node's published public profile, if one is set."""
    config = _make_config(args)
    doc = load_local_profile(config.profile_path)
    if doc is None:
        if _json_mode(args):
            _emit({"profile": None}, True)
        else:
            print(
                "No profile published yet. Publish one with "
                "`tau profile set --bio ... --link ...`."
            )
        return _EXIT_OK
    if _json_mode(args):
        _emit(public_profile(doc), True)
    else:
        _print_profile(doc)
    return _EXIT_OK


def _print_page(
    doc: Dict[str, Any],
    *,
    indent: str = "",
    page_files_dir: Optional[Path] = None,
) -> None:
    """Print one page document in text form (header + raw Markdown).

    Linked files are listed after the text, with whether this node
    already holds their bytes — a fetched page lists its files even
    before any of them is downloaded.
    """
    print(f"{indent}title:   {doc['title'] or '—'}")
    print(
        f"{indent}version: {doc['version']} "
        f"({_format_message_time(doc['updated_at'])})"
    )
    print(f"{indent}author:  {doc['author']}")
    print()
    print(doc["markdown"])
    files = doc.get("files", [])
    if files:
        print()
        print(f"{indent}files:")
        for entry in files:
            held = (
                load_page_file(page_files_dir, entry["sha256"]) is not None
                if page_files_dir is not None
                else False
            )
            state = "downloaded" if held else "not fetched"
            print(
                f"{indent}  - {entry['name']} "
                f"({entry['size']} bytes, {state})"
            )


def cmd_page_publish(args: argparse.Namespace) -> int:
    """Publish a new version of this node's signed personal page.

    The Markdown file is signed and written to the data directory; a
    serving node picks it up at its next fetch, and peers fetch the
    document over a link.  The title defaults to the file's first
    ``# `` heading.  Each ``--file`` attaches one small file to the
    page (at most five, 32 KiB each): its bytes are stored under
    their SHA-256 and its name, hash and size are signed into the
    page, so peers fetch exactly the bytes the author listed.  The
    file list is the new version's list in full — republishing
    without ``--file`` publishes a page with no linked files.
    """
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )
    try:
        markdown = Path(args.file).expanduser().read_text(encoding="utf-8")
    except OSError as exc:
        raise NodeError(f"could not read page file: {exc}") from exc
    attached_files: List[tuple] = []
    for attached in getattr(args, "attach", None) or []:
        try:
            data = Path(attached).expanduser().read_bytes()
        except OSError as exc:
            raise NodeError(f"could not read page file: {exc}") from exc
        try:
            entry = make_page_file_entry(Path(attached).name, data)
        except PageError as exc:
            raise NodeError(str(exc)) from exc
        attached_files.append((entry, data))
    try:
        entries = clean_page_files([entry for entry, _ in attached_files])
    except PageError as exc:
        raise NodeError(str(exc)) from exc
    for entry, data in attached_files:
        try:
            store_page_file(config.page_files_dir, entry["sha256"], data)
        except PageError as exc:
            raise NodeError(str(exc)) from exc
    existing = load_local_page(config.page_path)
    title = (
        args.title
        if args.title is not None
        else title_from_markdown(markdown)
    )
    version = existing["version"] + 1 if existing is not None else 1
    try:
        doc = build_page(
            identity,
            title=title,
            markdown=markdown,
            files=entries,
            version=version,
        )
    except PageError as exc:
        raise NodeError(str(exc)) from exc
    save_local_page(config.page_path, doc)
    if _json_mode(args):
        _emit(public_page(doc), True)
    else:
        print(
            f"Published page version {version}"
            + (f" — {doc['title']}" if doc["title"] else "")
            + "."
        )
        for entry in entries:
            print(f"  linked {entry['name']} ({entry['size']} bytes)")
        print("  Peers can fetch it with `tau page <your name>`.")
    return _EXIT_OK


def cmd_page_show(args: argparse.Namespace) -> int:
    """Show this node's published personal page, if one is set."""
    config = _make_config(args)
    doc = load_local_page(config.page_path)
    if doc is None:
        if _json_mode(args):
            _emit({"page": None}, True)
        else:
            print(
                "No page published yet. Publish one with "
                "`tau page publish <file.md>`."
            )
        return _EXIT_OK
    if _json_mode(args):
        _emit(public_page(doc), True)
    else:
        _print_page(doc, page_files_dir=config.page_files_dir)
    return _EXIT_OK


def cmd_page_fetch(args: argparse.Namespace) -> int:
    """Show a peer's signed personal page, fetching it when needed.

    A live fetch is attempted first (unless ``--cached``): the peer
    answers with its page only when it holds a newer version than
    this node.  When the fetch fails, the stored copy is shown
    instead and the failure is named; when nothing is stored at all,
    the fetch failure is the error.
    """
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    try:
        peer = registry.resolve(args.target)
    except PeerError as exc:
        raise NodeError(str(exc)) from exc
    if peer is None:
        raise NodeError(f"no peer matches {args.target!r}")
    store = PageStore(config.pages_path)
    fetch_error: Optional[str] = None
    fetched = False
    if not getattr(args, "cached", False):
        node = TauNode(config, name=config.name)
        node.start()
        try:
            node.request_page(peer, timeout=args.timeout)
            fetched = True
        except NodeError as exc:
            fetch_error = str(exc)
        finally:
            node.stop()
        # The node saved any newer page it fetched; re-read the
        # store rather than trusting the in-memory copy above.
        store = PageStore(config.pages_path)
    page = store.get(peer.hexhash)
    if page is None:
        if fetch_error is not None:
            raise NodeError(
                f"could not fetch a page from {peer.display_name}: "
                f"{fetch_error}"
            )
        if _json_mode(args):
            _emit(
                {
                    "peer": peer.hexhash,
                    "name": peer.display_name,
                    "page": None,
                },
                True,
            )
        else:
            print(f"{peer.display_name} has no personal page yet.")
        return _EXIT_OK
    if _json_mode(args):
        _emit(
            {
                "peer": peer.hexhash,
                "name": peer.display_name,
                "page": public_page(page),
                "fetched": fetched,
                "fetch_error": fetch_error,
            },
            True,
        )
    else:
        _print_page(page, page_files_dir=config.page_files_dir)
        if fetch_error is not None:
            print(f"  (showing the stored copy — fetch failed: {fetch_error})")
    return _EXIT_OK


def _resolve_page_file_entry(
    page: Dict[str, Any], selector: str
) -> Dict[str, Any]:
    """The linked-file entry named by *selector*: exact name, or a
    unique SHA-256 prefix.  Raises :class:`NodeError` when nothing
    matches, or when a prefix matches more than one file.
    """
    entries = page.get("files", [])
    for entry in entries:
        if entry["name"] == selector:
            return entry
    matches = [
        entry for entry in entries if entry["sha256"].startswith(selector)
    ]
    if not matches:
        raise NodeError(f"no linked file named {selector!r} on this page")
    if len(matches) > 1:
        names = ", ".join(entry["name"] for entry in matches)
        raise NodeError(
            f"{selector!r} matches more than one linked file: {names}"
        )
    return matches[0]


def cmd_page_file(args: argparse.Namespace) -> int:
    """Save one of a peer's linked page files to disk.

    The peer's page is fetched live first (unless ``--cached``), which
    also fetches its linked files; a file whose bytes are already
    stored is used as-is.  The file is written to ``--to`` (default:
    the file's name in the current directory) and never overwrites
    an existing file unless ``--force`` is given.
    """
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    try:
        peer = registry.resolve(args.target)
    except PeerError as exc:
        raise NodeError(str(exc)) from exc
    if peer is None:
        raise NodeError(f"no peer matches {args.target!r}")
    store = PageStore(config.pages_path)
    fetch_error: Optional[str] = None
    if not getattr(args, "cached", False):
        node = TauNode(config, name=config.name)
        node.start()
        try:
            node.request_page(peer, timeout=args.timeout)
        except NodeError as exc:
            fetch_error = str(exc)
        finally:
            node.stop()
        # The node saved any newer page it fetched; re-read the
        # store rather than trusting the in-memory copy above.
        store = PageStore(config.pages_path)
    page = store.get(peer.hexhash)
    if page is None:
        if fetch_error is not None:
            raise NodeError(
                f"could not fetch a page from {peer.display_name}: "
                f"{fetch_error}"
            )
        raise NodeError(f"{peer.display_name} has no personal page yet.")
    entry = _resolve_page_file_entry(page, args.selector)
    data = load_page_file(config.page_files_dir, entry["sha256"])
    if data is None and not getattr(args, "cached", False):
        node = TauNode(config, name=config.name)
        node.start()
        try:
            data = node.request_page_file(
                peer, entry, timeout=args.timeout
            )
        except NodeError as exc:
            fetch_error = str(exc)
        finally:
            node.stop()
    if data is None:
        if fetch_error is not None:
            raise NodeError(
                f"could not fetch {entry['name']} from "
                f"{peer.display_name}: {fetch_error}"
            )
        raise NodeError(
            f"{entry['name']} is listed on {peer.display_name}'s page "
            "but its bytes are not stored here; fetch the page live "
            "to download it."
        )
    destination = (
        Path(args.to).expanduser()
        if getattr(args, "to", None) is not None
        else Path.cwd() / entry["name"]
    )
    if destination.exists() and not getattr(args, "force", False):
        raise NodeError(
            f"{destination} already exists; pass --force to overwrite it"
        )
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
    except OSError as exc:
        raise NodeError(f"could not write {destination}: {exc}") from exc
    if _json_mode(args):
        _emit(
            {
                "peer": peer.hexhash,
                "name": peer.display_name,
                "file": entry,
                "path": str(destination),
            },
            True,
        )
    else:
        print(
            f"Saved {entry['name']} ({entry['size']} bytes) "
            f"from {peer.display_name} to {destination}."
        )
    return _EXIT_OK


def cmd_page_allow(args: argparse.Namespace) -> int:
    """Manage the page allow-list (who may fetch this node's page).

    NomadNet-style ``.allowed``: with no list file the page is public;
    ``tau page allow on`` enforces the list, so only listed peers may
    fetch.  A running serve applies saved changes when it restarts.
    """
    config = _make_config(args)
    json_output = _json_mode(args)
    command = getattr(args, "page_allow_command", None)
    registry = PeerRegistry(config.peers_path)
    changed = False

    if command in ("add", "remove", "on", "off"):
        # Reload inside the lock: two allow commands racing must not
        # discard each other's change to a different peer.
        with allowlist_lock(config.page_allowlist_path):
            try:
                allowlist = load_allowlist(config.page_allowlist_path)
            except AllowListError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return _EXIT_ERROR
            if command == "add":
                destination = _resolve_allowlist_peer(registry, args.peer)
                if destination not in allowlist.allow:
                    allowlist = allowlist.with_added(destination)
                    changed = True
            elif command == "remove":
                destination = _resolve_allowlist_peer(registry, args.peer)
                if destination in allowlist.allow:
                    allowlist = allowlist.with_removed(destination)
                    changed = True
            elif command == "on":
                if not allowlist.enabled:
                    allowlist = allowlist.with_enabled(True)
                    changed = True
            elif command == "off":
                if allowlist.enabled:
                    allowlist = allowlist.with_enabled(False)
                    changed = True
            if changed:
                save_allowlist(config.page_allowlist_path, allowlist)
    else:
        try:
            allowlist = load_allowlist(config.page_allowlist_path)
        except AllowListError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR

    def describe(destination: str) -> Dict[str, Any]:
        peer = registry.get(destination)
        return {
            "destination": destination,
            # The asserted name, not the local alias: the operator must
            # see who was actually allowed, and an alias could mask that.
            "name": (
                (peer.name or short_hash(peer.hexhash))
                if peer is not None
                else ""
            ),
        }

    if json_output:
        payload = allowlist.to_dict()
        payload["entries"] = [describe(value) for value in sorted(allowlist.allow)]
        payload["path"] = str(config.page_allowlist_path)
        _emit(payload, True)
    else:
        state = "enforced" if allowlist.enabled else "off"
        print(f"Page allow-list ({config.page_allowlist_path}): {state}")
        if not allowlist.allow:
            print("  no peers listed — when enforced, nobody may fetch this page")
        for entry in sorted(allowlist.allow):
            peer = registry.get(entry)
            name = (
                f" ({peer.name or short_hash(peer.hexhash)})"
                if peer is not None
                else ""
            )
            print(f"  {entry}{name}")
        if changed:
            print("  (a running serve applies saved changes when it restarts)")
    return _EXIT_OK


def cmd_peers_profile(args: argparse.Namespace) -> int:
    """Show a peer's signed public profile, fetching it when needed.

    A live fetch is attempted first (unless ``--cached``): the peer
    answers with its profile only when it holds a newer version than
    this node.  When the fetch fails, the stored copy is shown
    instead and the failure is named; when nothing is stored at all,
    the fetch failure is the error.
    """
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    try:
        peer = registry.resolve(args.target)
    except PeerError as exc:
        raise NodeError(str(exc)) from exc
    if peer is None:
        raise NodeError(f"no peer matches {args.target!r}")
    store = ProfileStore(config.profiles_path)
    fetch_error: Optional[str] = None
    fetched = False
    if not getattr(args, "cached", False):
        node = TauNode(config, name=config.name)
        node.start()
        try:
            node.request_profile(peer, timeout=args.timeout)
            fetched = True
        except NodeError as exc:
            fetch_error = str(exc)
        finally:
            node.stop()
        # The node saved any newer profile it fetched; re-read the
        # store rather than trusting the in-memory copy above.
        store = ProfileStore(config.profiles_path)
    profile = store.get(peer.hexhash)
    if profile is None:
        if fetch_error is not None:
            raise NodeError(
                f"could not fetch a profile from {peer.display_name}: "
                f"{fetch_error}"
            )
        if _json_mode(args):
            _emit(
                {
                    "peer": peer.hexhash,
                    "name": peer.display_name,
                    "profile": None,
                },
                True,
            )
        else:
            print(f"{peer.display_name} has no public profile yet.")
        return _EXIT_OK
    if _json_mode(args):
        _emit(
            {
                "peer": peer.hexhash,
                "name": peer.display_name,
                "profile": public_profile(profile),
                "fetched": fetched,
                "fetch_error": fetch_error,
            },
            True,
        )
    else:
        _print_profile(profile)
        if fetch_error is not None:
            print(f"  (showing the stored copy — fetch failed: {fetch_error})")
    return _EXIT_OK


def cmd_peers_exchange(args: argparse.Namespace, config: TauConfig) -> int:
    """Exchange peer lists with known peers and report what was learned."""
    node = TauNode(config, name=config.name)
    node.start()
    try:
        target = getattr(args, "to", None)
        targets = [node.resolve_peer(target)] if target else node.registry.all()
        results: List[Dict[str, Any]] = []
        for peer in targets:
            try:
                summary = node.exchange_peers(peer)
            except NodeError as exc:
                results.append(
                    {
                        "peer": peer.hexhash,
                        "name": peer.display_name,
                        "ok": False,
                        "error": str(exc),
                    }
                )
                continue
            results.append(
                {"peer": peer.hexhash, "name": peer.display_name, "ok": True, **summary}
            )
        totals = {
            key: sum(int(r.get(key, 0)) for r in results if r.get("ok"))
            for key in ("added", "updated", "skipped")
        }
        totals["peers"] = len(results)
        totals["failed"] = sum(1 for r in results if not r.get("ok"))
    finally:
        node.stop()

    if _json_mode(args):
        _emit({"exchanged": results, **totals}, True)
    else:
        for result in results:
            if result.get("ok"):
                print(
                    f"{result['name']} ({result['peer'][:8]}): "
                    f"{result['added']} new, {result['updated']} updated, "
                    f"{result['skipped']} skipped"
                )
            else:
                print(
                    f"{result['name']} ({result['peer'][:8]}): "
                    f"failed — {result['error']}"
                )
        print(
            f"\n{totals['added']} new peer(s), {totals['updated']} updated, "
            f"{totals['skipped']} skipped from {totals['peers']} peer(s)"
            + (f", {totals['failed']} failed" if totals["failed"] else "")
        )
    # A failed exchange is still reported per peer above, but scripts
    # checking only the exit status must not read it as success.
    return _EXIT_ERROR if totals["failed"] else _EXIT_OK


def cmd_id(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    destination = destination_hash(identity).hex()
    payload = {
        "name": config.name,
        "destination": destination,
        "identity": identity.hexhash,
        "lxmf": lxmf_delivery_hash(identity).hex(),
        # Fingerprint of this identity alone — the pairwise safety number
        # for a peer (shown by `tau peers --safety-number`) folds the
        # peer's key in too, so the two numbers are different by design.
        "safety_number": safety_number(identity.get_public_key()),
        "data_dir": str(config.data_dir),
        "rns_config": str(config.rns_config_file),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"name:          {config.name}")
        print(f"destination:   {destination}")
        print(f"identity:      {identity.hexhash}")
        print(f"lxmf:          {payload['lxmf']}")
        print(f"safety number: {payload['safety_number']}")
        print(f"data dir:      {config.data_dir}")
    return _EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    """Summarise the local node's state without starting it.

    Read-only: no RNS, no network, and the data-directory lock is only
    probed, never taken, so this keeps working while a node is serving.
    A missing Tan database reports zero posts instead of creating one.
    """
    config = _make_config(args)
    identity = load_identity(config.identity_path, harden=False)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    mailbox = Mailbox(config.mailbox_path)
    counts = mailbox.counts()
    now = time.time()
    due = sum(
        1
        for entry in mailbox.outbox_list()
        if entry.expires_at > now and entry.is_due(now)
    )
    registry = PeerRegistry(config.peers_path)
    inbox = Inbox(config.inbox_path)
    # Fail closed like the running node: an unreadable allow-list file
    # reports as enabled-but-empty rather than silently open.
    allowlist = load_allowlist_or_deny_all(config.allowlist_path)
    tan_posts = 0
    tan_following = 0
    if config.social_db_path.exists():
        # Read-only: never migrate or write a database a node may be serving.
        try:
            conn = open_db_readonly(config.social_db_path)
        except sqlite3.Error:
            conn = None
        if conn is not None:
            try:
                tan_posts = int(
                    conn.execute(
                        "SELECT COUNT(*) AS n FROM posts WHERE kind = 'post'"
                    ).fetchone()["n"]
                )
                tan_following = len(following(conn))
            except sqlite3.Error:
                # Missing or incompatible tables report zero, not an error.
                tan_posts = 0
                tan_following = 0
            finally:
                conn.close()
    serving_pid = instance_lock_holder(config.instance_lock_path)
    payload = {
        "name": config.name,
        "destination": destination_hash(identity).hex(),
        "lxmf": lxmf_delivery_hash(identity).hex(),
        "data_dir": str(config.data_dir),
        "serving": serving_pid is not None,
        "serving_pid": serving_pid,
        "peers": len(registry),
        "outbox": {
            "queued": counts["outbox"],
            "due": due,
            "held": counts["held"],
        },
        "inbox": {"messages": len(inbox)},
        "tan": {"posts": tan_posts, "following": tan_following},
        "allowlist": {
            "enabled": allowlist.enabled,
            "entries": len(allowlist.allow),
        },
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"name:        {payload['name']}")
        print(f"destination: {payload['destination']}")
        print(f"lxmf:        {payload['lxmf']}")
        print(f"data dir:    {payload['data_dir']}")
        if serving_pid is None:
            print("serving:     no")
        elif serving_pid < 0:
            print("serving:     yes (pid unknown)")
        else:
            print(f"serving:     yes (pid {serving_pid})")
        print(f"peers:       {payload['peers']}")
        print(
            f"outbox:      {payload['outbox']['queued']} queued "
            f"({payload['outbox']['due']} due), "
            f"{payload['outbox']['held']} held"
        )
        print(f"inbox:       {payload['inbox']['messages']} messages")
        print(
            f"tan:         {payload['tan']['posts']} posts, "
            f"following {payload['tan']['following']} authors"
        )
        print(
            f"allowlist:   {'enabled' if payload['allowlist']['enabled'] else 'disabled'} "
            f"({payload['allowlist']['entries']} entries)"
        )
    return _EXIT_OK


def _same_path(left: Path, right: Path) -> bool:
    """Compare two paths after expanding ``~`` and resolving symlinks."""
    try:
        return Path(left).expanduser().resolve() == Path(right).expanduser().resolve()
    except OSError:
        return False


def cmd_id_export(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    target = Path(args.to).expanduser()
    if _same_path(target, config.identity_path):
        raise NodeError(
            f"cannot export the identity to itself ({target}); "
            "choose a different backup path"
        )
    if target.is_dir():
        raise NodeError(f"backup path {target} is a directory")
    # lexists, not exists: a dangling symlink at the target counts as an
    # existing backup and must not be silently replaced without --force.
    if os.path.lexists(target) and not getattr(args, "force", False):
        raise NodeError(
            f"backup already exists at {target}; pass --force to overwrite it"
        )
    # The backup goes into a caller-selected folder: never change that
    # folder's permissions, only the backup file itself is private.
    save_identity(identity, target, restrict_parent=False)
    payload = {
        "destination": destination_hash(identity).hex(),
        "identity": identity.hexhash,
        "path": str(target),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"identity:    {payload['identity']}")
        print(f"destination: {payload['destination']}")
        print(f"backup:      {payload['path']}")
    return _EXIT_OK


def cmd_id_import(args: argparse.Namespace) -> int:
    config = _make_config(args)
    source = Path(args.from_path).expanduser()
    if not source.exists():
        raise NodeError(f"no identity backup at {source}")
    if _same_path(source, config.identity_path):
        raise NodeError(f"{source} is already this node's identity file")
    try:
        identity = load_identity(source)
    except Exception as exc:
        # A backup file is untrusted input: any parse failure is a bad backup.
        raise NodeError(f"not a valid identity backup: {exc}") from exc
    if (
        identity is None
    ):  # pragma: no cover - load_identity returns None only when missing
        raise NodeError(f"no identity backup at {source}")
    config.prepare()
    # Hold the instance lock across the whole replacement: a node that
    # starts after the probe but before the write would load the old
    # identity and then serve with a different identity than the one on
    # disk.  The identity-creation lock serialises the check-and-write
    # against a concurrent `tau init` on the same directory.  Lock order
    # (instance, then identity) matches `TauNode.start`, which takes the
    # instance lock before loading or creating its identity.
    # Note the instance lock is a no-op on a platform with neither
    # fcntl nor msvcrt, the same limitation `tau serve` itself accepts.
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        with _identity_lock(config.identity_path):
            if config.identity_path.exists() and not getattr(args, "force", False):
                raise NodeError(
                    f"an identity already exists at {config.identity_path}; "
                    "pass --force to replace it"
                )
            # Snapshot the installed identity BEFORE clearing
            # verifications, the same ordering as a redeem (see
            # #721) — a failed snapshot must be known before any
            # state is cleared.  An unreadable old identity does not
            # abort the import — it was already tolerated when
            # deciding the reset — it only disables rollback.
            try:
                previous, had_previous = _snapshot_previous_identity(config)
            except OSError as exc:
                log.warning(
                    "Could not read the existing identity (%s); the "
                    "replacement cannot be rolled back",
                    exc,
                )
                previous, had_previous = None, True
            verifications_cleared = _clear_verifications_for_identity_change(
                config, identity
            )
            # Decide the reset while the old identity is still on
            # disk, but apply it only after the new identity is
            # installed: a failed write must leave the old pairing
            # records intact (see #716).
            registry = DeviceRegistry(config.devices_path)
            reset_devices = _devices_reset_needed(config, identity)
            _install_identity_and_reset(
                config,
                identity,
                registry,
                reset_devices,
                previous=previous,
                had_previous=had_previous,
            )
    finally:
        lock.release()
    payload = {
        "destination": destination_hash(identity).hex(),
        "identity": identity.hexhash,
        "data_dir": str(config.data_dir),
        "verifications_cleared": verifications_cleared,
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"identity:    {payload['identity']}")
        print(f"destination: {payload['destination']}")
        print(f"data dir:    {payload['data_dir']}")
        if verifications_cleared:
            print(
                f"verifications cleared: {verifications_cleared} "
                "(local identity changed; re-verify peers)"
            )
    return _EXIT_OK


def _snapshot_previous_identity(
    config: TauConfig,
) -> Tuple[Optional[bytes], bool]:
    """Read the installed identity for a later rollback.

    Returns ``(previous_bytes, had_previous)``: ``(None, False)``
    when no identity is installed yet.  Raises ``OSError`` when an
    identity file exists but cannot be read — the caller decides
    whether that aborts the command (a redeem must fail before its
    token is recorded, so the bundle stays retryable) or merely
    disables rollback (an import keeps the old tolerance for
    unreadable identities, see #716).
    """
    if not config.identity_path.exists():
        return None, False
    return config.identity_path.read_bytes(), True


def _install_identity_and_reset(
    config: TauConfig,
    identity: Any,
    registry: DeviceRegistry,
    reset_devices: bool,
    redeemed_token: Optional[str] = None,
    previous: Optional[bytes] = None,
    had_previous: bool = False,
) -> None:
    """Install *identity*, then apply the pairing reset, rolling back on failure.

    If the identity write or the registry reset fails — ``OSError``,
    ``IdentityError`` from the write, or ``DevicesError`` when the
    reset reloads a corrupt registry — the previous identity file is
    restored (or removed when there was none) so the identity and
    pairing records stay consistent.  For a redeem, the token recorded
    for this bundle is dropped once the previous identity is confirmed
    restored, so the same bundle can be retried.

    ``previous``/``had_previous`` are the snapshot from
    :func:`_snapshot_previous_identity`, taken before anything is
    written (for a redeem: before the token is recorded).  When the
    previous identity was unreadable, ``had_previous`` is True with
    ``previous`` None: there is nothing to restore to, so a failure
    leaves the new identity in place and the token is kept — the
    bundle cannot be replayed — rather than deleting the only
    identity on disk.
    """
    try:
        save_identity(identity, config.identity_path)
        if reset_devices:
            registry.reset()
    except (OSError, IdentityError, DevicesError):
        restored = False
        try:
            if not had_previous:
                if config.identity_path.exists():
                    config.identity_path.unlink()
                restored = True
            elif previous is not None:
                atomic_write_bytes(
                    config.identity_path, previous, mode=IDENTITY_FILE_MODE
                )
                restored = True
            else:
                log.warning(
                    "Could not restore the previous identity at %s: it "
                    "was unreadable before the replacement",
                    config.identity_path,
                )
        except OSError as restore_exc:
            log.warning(
                "Could not restore the previous identity at %s: %s",
                config.identity_path,
                restore_exc,
            )
        if restored and redeemed_token is not None:
            try:
                registry.forget_redeemed(redeemed_token)
            except (OSError, DevicesError) as forget_exc:
                # Fail closed: a token that cannot be released stays
                # recorded, so the bundle cannot be replayed.
                log.warning(
                    "Could not release the redeemed pairing token: %s",
                    forget_exc,
                )
        raise


def _clear_verifications_for_identity_change(
    config: TauConfig, identity: Any
) -> int:
    """Clear peer safety-number verifications invalidated by a new identity.

    Pairwise safety numbers are computed with the local key, so
    verifications made under the old identity mean nothing under the
    new one.  An unreadable old identity is treated the same way: its
    comparisons cannot be trusted either.  The invalidation is
    persisted BEFORE the identity is replaced: if the registry write
    fails, abort with the old identity still in place rather than
    leaving the new identity alongside stale verified markers.
    """
    try:
        old_identity = load_identity(config.identity_path)
        old_key = (
            old_identity.get_public_key() if old_identity is not None else None
        )
    except Exception as exc:
        # The identity is about to be replaced either way, so the
        # comparisons are cleared — but say why, so a transient read
        # failure (e.g. permissions) is not silent.
        log.warning(
            "Could not read the existing identity (%s); clearing "
            "peer verifications",
            exc,
        )
        old_key = None
    if old_key == identity.get_public_key():
        return 0
    registry = PeerRegistry(config.peers_path)
    cleared = registry.clear_verifications()
    if cleared:
        try:
            saved = registry.save()
        except OSError as exc:
            raise NodeError(
                f"could not save peer registry ({exc}); identity not replaced"
            ) from exc
        if not saved:
            raise NodeError("could not save peer registry; identity not replaced")
    return cleared


def _devices_reset_needed(config: TauConfig, identity: Any) -> bool:
    """Decide whether an identity change drops the pairing records.

    Issued offers and the pairing itself belong to the identity that
    was installed when they were recorded: after an identity
    replacement they would otherwise be reported as the new
    identity's pairing.  A restore of the same identity keeps them,
    and a node with no identity yet has nothing to drop.  An
    unreadable old identity counts as different — records of unknown
    provenance must not be attributed to the replacement.

    This only decides: the caller applies ``DeviceRegistry.reset()``
    after ``save_identity`` succeeds, so a failed identity write
    leaves the old records intact (see #716).  Returns True when the
    registry must be reset.
    """
    if not config.identity_path.exists():
        return False
    try:
        old_identity = load_identity(config.identity_path)
        old_key = (
            old_identity.get_public_key() if old_identity is not None else None
        )
    except Exception:
        old_key = None
    if old_key is not None and old_key == identity.get_public_key():
        return False
    return True


def cmd_devices_pair(args: argparse.Namespace) -> int:
    """Mint a pairing bundle for a second device (slice 1 of #469, #713).

    The bundle carries the private identity, so it is written
    owner-only and the user is told to treat it like an identity
    backup: anyone holding it can adopt this identity until it
    expires.
    """
    config = _make_config(args)
    # Only the data directory (which holds the instance-lock file) is
    # needed here; preparing the Reticulum config dir would touch
    # unrelated state.
    ensure_dir(config.data_dir)
    # Hold the instance lock across loading the identity, minting
    # and publishing the bundle, and recording the offer: without
    # it, a `tau id import --force` between the bundle creation and
    # record_issued() lands the old identity's offer in the
    # replacement identity's registry (see #715).  Lock order
    # (instance, then identity) matches `tau id import`, which
    # takes the instance lock before loading or creating its
    # identity — the same discipline `tau serve` itself follows.
    # Note the instance lock is a no-op on a platform with neither
    # fcntl nor msvcrt, the same limitation `tau serve` itself accepts.
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    try:
        lock.acquire()
    except InstanceAlreadyRunning:
        # The holder need not be a serving node: a backup holds the
        # same lock while it archives, so report the conflict as
        # another tau process using the data directory rather than
        # telling the user to stop a node that may not be running.
        holder = instance_lock_holder(config.instance_lock_path)
        detail = f" (pid {holder})" if holder not in (None, -1) else ""
        raise NodeError(
            "cannot mint a pairing bundle: data directory "
            f"{config.data_dir} is in use by another tau process"
            f"{detail}; wait for it to finish or stop it first"
        ) from None
    try:
        identity = load_identity(config.identity_path)
        if identity is None:
            raise NodeError(
                f"no identity at {config.identity_path}; run `tau init` first"
            )
        target = Path(args.to).expanduser()
        if _same_path(target, config.identity_path):
            raise NodeError(
                f"cannot write a pairing bundle to the identity itself ({target})"
            )
        if target.is_dir():
            raise NodeError(f"pairing bundle path {target} is a directory")
        # lexists, not exists: a dangling symlink at the target counts as
        # an existing bundle and must not be silently replaced without
        # --force — the same rule `tau id export` applies.
        if os.path.lexists(target) and not getattr(args, "force", False):
            raise NodeError(
                f"pairing bundle already exists at {target}; pass --force to overwrite it"
            )
        bundle = build_pairing_bundle(identity, ttl_seconds=float(args.ttl))
        # Load the registry before publishing the bundle: a corrupt
        # registry must fail the command while no file exists, not leave a
        # redeemable bundle on disk that `tau devices list` cannot see.
        registry = DeviceRegistry(config.devices_path)
        # The bundle goes into a caller-selected folder: never change that
        # folder's permissions, only the bundle file itself is private —
        # the same rule `tau id export` applies.
        #
        # Stage the new bundle beside the target and only replace the
        # target once the offer is recorded.  Writing straight to the
        # target first destroys a previous bundle when --force overwrites
        # one and the registry write then fails: the unlink in the
        # failure path removes the only remaining copy (see #717).  Until
        # the replacement is finalised the old target is untouched, so a
        # failed recording removes only the staged file.
        # The staging name is random and created exclusively (mkstemp), so
        # another user cannot pre-create a path that blocks the write in a
        # shared folder.
        ensure_dir(target.parent, restrict_existing=False)
        fd, staging_name = tempfile.mkstemp(
            dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
        )
        staging = Path(staging_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(bundle, indent=2, sort_keys=True, ensure_ascii=False)
                    + "\n"
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(staging, 0o600)
            registry.record_issued(
                bundle["token"], bundle["created_at"], bundle["expires_at"]
            )
        except BaseException:
            # The command failed, so the bundle must not stay behind as
            # an untracked redeemable offer — this covers OSError and
            # DevicesError alike.  Only the staged file goes: the previous
            # bundle at the target is still the live one.
            staging.unlink(missing_ok=True)
            raise
        try:
            os.replace(staging, target)
        except BaseException:
            # The offer is recorded but the new bundle never landed: the
            # old bundle is still at the target and still redeemable, so
            # drop the staged file and report the failure rather than
            # leaving a second copy behind.  BaseException, not OSError:
            # an interrupt during the one-syscall replace must clean up
            # the staged private identity too (see #724).
            staging.unlink(missing_ok=True)
            raise
    finally:
        lock.release()
    expires = datetime.fromtimestamp(bundle["expires_at"]).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    payload = {
        "token": bundle["token"],
        "primary": bundle["primary"],
        "destination": destination_hash(identity).hex(),
        "identity": identity.hexhash,
        "expires_at": bundle["expires_at"],
        "path": str(target),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"identity:    {payload['identity']}")
        print(f"destination: {payload['destination']}")
        print(f"bundle:      {payload['path']}")
        print(f"expires:     {expires}")
        print(
            "keep this file private: anyone holding it can adopt this "
            "identity until it expires"
        )
    return _EXIT_OK


def _recover_stranded_pairing(
    config: TauConfig,
    registry: DeviceRegistry,
    identity: Any,
    bundle: Dict[str, Any],
) -> bool:
    """Complete a pairing whose status write failed (see #718).

    A redeem records the token before installing the identity and
    writes the pairing record only after the install succeeds.  When
    that second write fails, the retry was rejected as "already
    redeemed" even though the identity is installed and `devices
    list` shows no pairing.  The redeemed-token record already
    carries the pairing metadata (the primary), so the retry can
    complete the record instead of stranding the bundle.

    Returns True when the token was redeemed for the identity now
    installed but no pairing is recorded yet, and the record has
    now been written.  Returns False for a plain replay: a different
    identity is installed, or a pairing is already recorded for
    this bundle.  Raises NodeError when the record still cannot be
    written, so the retry reports the failure instead of silently
    leaving the status missing.
    """
    try:
        installed = load_identity(config.identity_path)
    except (OSError, IdentityError) as exc:
        raise NodeError(
            f"could not inspect the installed identity at {config.identity_path} "
            f"to recover the pairing ({exc})"
        ) from exc
    if installed is None:
        return False
    if installed.get_public_key() != identity.get_public_key():
        return False
    # Only a token whose pairing record is still pending qualifies.  A
    # completed pairing stays completed even if an identity swap has
    # since cleared the pairing record, so it cannot be replayed.
    entry = registry.redeemed_entry(bundle["token"])
    if entry is None or entry.get("paired", True):
        return False
    # The token must have been redeemed for this bundle's primary.
    if entry["primary"] != str(bundle["primary"]).lower():
        return False
    # A pairing recorded for THIS bundle means the pairing completed.
    # Anything else predates this redemption: a failed reset plus a
    # failed identity rollback can leave the previous identity's
    # pairing recorded alongside the replacement identity (see #718),
    # and the retry must be allowed to clear it via the pending
    # reset below instead of being rejected as already redeemed.  A
    # pairing recorded after the failed attempt is for a different
    # identity, which the key check above already rejects.
    paired = registry.paired_from
    if paired is not None and paired.get("token") == entry["token"]:
        return False
    # A different-token pairing completed at or after this redemption
    # belongs to a later successful redeem, not the stranded one: the
    # pending reset must not erase it or its issued offers.
    paired_at = paired.get("paired_at") if paired is not None else None
    redeemed_at = entry.get("redeemed_at")
    if paired_at is not None and redeemed_at is not None:
        if float(paired_at) >= float(redeemed_at):
            return False
    # The identity-change reset is complete only when the redeem that
    # needed it has cleared its pending marker (the reset clears it
    # atomically).  If the reset already ran, offers issued since must
    # not be dropped by a retry; if it never ran, complete it here
    # before recording the pairing, and fail instead of returning
    # success with stale records if it still cannot run.
    if entry.get("reset_pending"):
        try:
            registry.reset()
        except (OSError, DevicesError) as exc:
            raise NodeError(
                f"could not complete the pairing reset ({exc}); "
                "retry the same bundle once the failure clears"
            ) from exc
    try:
        registry.record_paired_from(bundle["token"], bundle["primary"])
    except (OSError, DevicesError) as exc:
        raise NodeError(
            f"could not record pairing ({exc}); identity was installed"
        ) from exc
    return True


def cmd_devices_redeem(args: argparse.Namespace) -> int:
    """Adopt the identity in a pairing bundle (slice 1 of #469, #713)."""
    config = _make_config(args)
    source = Path(args.from_path).expanduser()
    if not source.exists():
        raise NodeError(f"no pairing bundle at {source}")
    if _same_path(source, config.identity_path):
        raise NodeError(f"{source} is already this node's identity file")
    try:
        size = source.stat().st_size
    except OSError as exc:
        raise NodeError(f"not a valid pairing bundle: {exc}") from exc
    if size > MAX_PAIRING_BUNDLE_BYTES:
        raise NodeError(
            f"pairing bundle is too large ({size} bytes, "
            f"over {MAX_PAIRING_BUNDLE_BYTES})"
        )
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise NodeError(f"not a valid pairing bundle: {exc}") from exc
    identity, bundle = verify_pairing_bundle(raw)
    config.prepare()
    # Hold the instance lock across the whole replacement: a node that
    # starts after the probe but before the write would load the old
    # identity and then serve with a different identity than the one on
    # disk.  The identity-creation lock serialises the check-and-write
    # against a concurrent `tau init` on the same directory.  Lock
    # order (instance, then identity) matches `TauNode.start`, which
    # takes the instance lock before loading or creating its identity
    # — the same discipline `tau id import` follows.
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        with _identity_lock(config.identity_path):
            registry = DeviceRegistry(config.devices_path)
            if registry.is_redeemed(bundle["token"]):
                # A redeem whose pairing record failed to persist
                # leaves the token recorded and the identity installed
                # but no pairing status (see #718): the retry was
                # rejected here before it could repair the status
                # `devices list` displays.  Complete the record
                # instead when this bundle is the one installed; a
                # plain replay of a completed pairing still fails.
                if _recover_stranded_pairing(
                    config, registry, identity, bundle
                ):
                    # The identity was already installed by the
                    # first attempt, so no verifications were
                    # cleared by this retry.
                    verifications_cleared = 0
                else:
                    raise NodeError(
                        "pairing bundle was already redeemed on this node"
                    )
            else:
                if config.identity_path.exists() and not getattr(args, "force", False):
                    raise NodeError(
                        f"an identity already exists at {config.identity_path}; "
                        "pass --force to replace it"
                    )
                # Snapshot the installed identity BEFORE clearing
                # verifications and BEFORE recording the token: if the
                # snapshot read fails, the command aborts while the bundle
                # is still unredeemed and retryable — and with the old
                # identity's peer verifications intact (see #721).
                previous, had_previous = _snapshot_previous_identity(config)
                verifications_cleared = _clear_verifications_for_identity_change(
                    config, identity
                )
                # Decide the reset while the old identity is still on
                # disk, but apply it only after the new identity is
                # installed: a failed write must leave the old pairing
                # records intact (see #716).  The reset keeps redeemed
                # tokens, so the token recorded just below survives it.
                reset_devices = _devices_reset_needed(config, identity)
                # Record the token BEFORE installing the identity: if the
                # record fails, the old identity stays in place and the
                # bundle can be minted fresh — the reverse order would
                # leave an installed identity with no redeemed-token
                # record, so a later --force redeem would accept the same
                # bundle again (see #714).  The pairing itself is only
                # recorded after the install succeeds, so a failed install
                # never claims a pairing that did not happen.
                registry.record_redeemed(
                    bundle["token"],
                    bundle["primary"],
                    bundle["expires_at"],
                    reset_pending=reset_devices,
                )
                _install_identity_and_reset(
                    config,
                    identity,
                    registry,
                    reset_devices,
                    redeemed_token=bundle["token"],
                    previous=previous,
                    had_previous=had_previous,
                )
                try:
                    registry.record_paired_from(bundle["token"], bundle["primary"])
                except OSError as exc:
                    # The identity is already installed; the pairing
                    # record is bookkeeping, so say so rather than
                    # reporting the redeem as failed.
                    raise NodeError(
                        f"could not record pairing ({exc}); identity was installed — "
                        "retry the same bundle to record the pairing"
                    ) from exc
    finally:
        lock.release()
    payload = {
        "destination": destination_hash(identity).hex(),
        "identity": identity.hexhash,
        "paired_from": bundle["primary"],
        "data_dir": str(config.data_dir),
        "verifications_cleared": verifications_cleared,
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"identity:    {payload['identity']}")
        print(f"destination: {payload['destination']}")
        print(f"paired from: {payload['paired_from']}")
        print(f"data dir:    {payload['data_dir']}")
        if verifications_cleared:
            print(
                f"verifications cleared: {verifications_cleared} "
                "(local identity changed; re-verify peers)"
            )
    return _EXIT_OK


def cmd_devices_list(args: argparse.Namespace) -> int:
    """Show this device and the pairing offers it issued (#713)."""
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    registry = DeviceRegistry(config.devices_path)
    offers = registry.issued_offers()
    paired_from = registry.paired_from
    payload = {
        "identity": identity.hexhash if identity is not None else None,
        "destination": (
            destination_hash(identity).hex() if identity is not None else None
        ),
        "paired_from": paired_from,
        "issued_offers": offers,
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        if identity is None:
            print(
                "this device: no identity yet "
                "(run `tau init`, or redeem a pairing bundle)"
            )
        else:
            print(f"this device: {identity.hexhash}")
            print(f"destination: {destination_hash(identity).hex()}")
            if paired_from is not None:
                paired_at = datetime.fromtimestamp(
                    paired_from["paired_at"]
                ).strftime("%Y-%m-%d %H:%M:%S")
                print(
                    f"paired from: {paired_from['primary']} "
                    f"(token {paired_from['token'][:8]}…, {paired_at})"
                )
        if offers:
            print(f"pairing offers issued: {len(offers)}")
            for offer in offers:
                expires = datetime.fromtimestamp(offer["expires_at"]).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                print(f"  token {offer['token'][:8]}… expires {expires}")
        else:
            print("no pairing offers issued")
    return _EXIT_OK


def _resolve_send_reply_to(
    node: TauNode, target: str, ref: str, discover_timeout: Optional[float]
) -> Dict[str, Any]:
    """Resolve ``--reply-to`` to an ``in_reply_to`` wire reference on *node*.

    Derive the conversation hash without network discovery, so a reply
    to a queued message still reaches the outbox when the recipient is
    offline; discovery is only a fallback.
    """
    target_hash = node._target_hash(target)
    if target_hash is None:
        peer = node.resolve_peer(
            target,
            timeout=float(
                discover_timeout if discover_timeout is not None else 10.0
            ),
        )
        target_hash = peer.destination_hash
    return _resolve_reply_target(
        Inbox(node.config.inbox_path),
        node.mailbox,
        ref,
        target_hash.hex(),
    )


def _finalize_send_result(
    response: Dict[str, Any],
    *,
    target: str,
    expires_at: Optional[float],
    queued_before: set,
) -> Dict[str, Any]:
    """Massage a raw send response into the result ``tau send`` reports.

    The peer response is untrusted JSON: keep the local recipient label
    authoritative, so a peer cannot relabel this result. A "queued"
    claim only counts with a new id (see below); a direct delivery keeps
    no "queued" key at all, preserving the established single-recipient
    JSON shape.
    """
    result = dict(response)
    result["target"] = target
    if expires_at is not None:
        result["expires_at"] = expires_at
    if response.get("queued"):
        result["queued"] = (
            isinstance(response.get("id"), str)
            and response["id"] not in queued_before
        )
        if result["queued"] and "to" not in response:
            result["to"] = target
    return result


def _reconcile_queued_sends(
    node: TauNode, results: List[Dict[str, Any]], expires_at: Optional[float]
) -> None:
    """Flip queued results to delivered when a flush moved them.

    After a queued send, give reachable peers a brief chance to take a
    sealed copy (or the message itself), then report as delivered any
    message that left the outbox during the flush.
    """
    if not any(result.get("queued") for result in results):
        return
    node.flush_outbox(timeout=3.0, max_peers=5)
    for result in results:
        if not result.get("queued"):
            continue
        message_id = result["id"]
        # Leaving the outbox is not proof of delivery: an entry can also
        # expire or be cancelled. Only a recorded delivery counts.
        sent = node.mailbox.sent_record(message_id)
        if node.mailbox.get_outbox(message_id) is None and (
            sent is not None and sent.get("delivered_at") is not None
        ):
            to_hex = result["to"]
            target = result["target"]
            # The node reports the effective expiry (explicit or the
            # per-conversation default); prefer it over the caller's.
            effective_expires = result.get("expires_at", expires_at)
            # Keep the private "_" fields (e.g. the resolved reply id).
            private = {k: v for k, v in result.items() if k.startswith("_")}
            result.clear()
            result.update(
                {
                    "target": target,
                    "ok": True,
                    "delivered": True,
                    "id": message_id,
                    "to": to_hex,
                    **(
                        {"expires_at": effective_expires}
                        if effective_expires is not None
                        else {}
                    ),
                    **private,
                }
            )


def _wait_for_queued_sends(
    node: TauNode, results: List[Dict[str, Any]], wait: float
) -> None:
    """Wait for the queued results in turn, sharing one deadline.

    The ``--wait`` handling from :func:`cmd_send`, shared by the local
    send path and the send server so both routes report the same
    result: ``--wait`` replaces the one-shot flush, a direct delivery
    records ``waited: 0.0``, and a queued message still queued at the
    deadline — or one that left the queue without a delivery
    confirmation from this node — is recorded as not delivered.  All
    results share a single ``wait`` budget, so a group send with many
    offline members cannot block for ``wait`` seconds per leg.
    """
    deadline = time.monotonic() + max(0.0, wait)
    for result in results:
        if not result.get("queued"):
            if result.get("ok", True):
                result["delivered"] = True
                result["waited"] = 0.0
            continue
        started = time.monotonic()
        remaining = max(0.0, deadline - started)
        delivered = node.wait_for_delivery(result["id"], timeout=remaining)
        waited = time.monotonic() - started
        if delivered:
            result.pop("queued", None)
            result["delivered"] = True
            result["waited"] = round(waited, 1)
        elif node.mailbox.get_outbox(result["id"]) is None:
            # The entry left the queue without a delivery confirmation
            # from this node: cancelled by another process, or delivered
            # by one.  Either way it must not be reported as delivered.
            result["queued"] = False
            result["delivered"] = False
            result["waited"] = round(waited, 1)
        else:
            result["delivered"] = False
            result["waited"] = round(waited, 1)


def _beacon_start_local(
    node: TauNode,
    *,
    target: str,
    group: bool = False,
    location: Dict[str, Any],
    location_file: Optional[str] = None,
    coarse: bool = False,
    duration: float = BEACON_DEFAULT_DURATION,
    interval: float = BEACON_UPDATE_INTERVAL,
) -> Dict[str, Any]:
    """Start a beacon on the running node, for the local send server.

    Starts the beacon and sends its first update at once, so the
    peer or group hears from the sender immediately instead of at
    the next mail-loop round.  Later updates go out from the mail
    loop, which re-reads the beacons file each round.
    """
    beacon = node.start_beacon(
        target,
        location,
        group=bool(group),
        coarse=bool(coarse),
        duration=float(duration),
        interval=float(interval),
        location_file=location_file,
    )
    update = node.send_beacon_update(beacon)
    return {"ok": True, "beacon": _beacon_summary(beacon), "update": update}


def _send_local_message(
    node: TauNode,
    *,
    target: str,
    text: str,
    timeout: float = 20.0,
    discover_timeout: Optional[float] = None,
    queue: bool = True,
    expires_at: Optional[float] = None,
    reply_to: Optional[str] = None,
    kind: Optional[str] = None,
    location: Optional[Dict[str, Any]] = None,
    wait: float = 0.0,
    send_after: Optional[float] = None,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Send one DM on the running node, for the local send server.

    Mirrors the one-shot path in :func:`cmd_send` (same queued-before
    snapshot, same queued-to-delivered flush reconciliation) so a send
    routed through the serving node reports the same result. The
    resolved reply id travels back under ``_reply_id`` for the caller's
    human-readable output; callers strip it before printing.  With
    ``--wait`` the wait replaces the flush reconciliation (see
    :func:`_wait_for_queued_sends`).
    """
    queued_before = {entry.id for entry in node.mailbox.outbox_list()}
    in_reply_to = None
    if reply_to is not None:
        in_reply_to = _resolve_send_reply_to(
            node, target, reply_to, discover_timeout
        )
    response = node.send_message(
        target,
        text,
        timeout=float(timeout),
        discover_timeout=discover_timeout,
        queue=bool(queue),
        in_reply_to=in_reply_to,
        expires_at=expires_at,
        kind=kind,
        location=location,
        send_after=send_after,
        poll_options=poll_options,
        poll_vote=poll_vote,
        poll_close=poll_close,
    )
    result = _finalize_send_result(
        response, target=target, expires_at=expires_at, queued_before=queued_before
    )
    if in_reply_to is not None:
        result["_reply_id"] = in_reply_to["id"]
    if wait > 0:
        _wait_for_queued_sends(node, [result], wait)
    else:
        _reconcile_queued_sends(node, [result], expires_at)
    return result


def _send_group_local_message(
    node: TauNode,
    *,
    group: str,
    text: str,
    timeout: float = 20.0,
    leg_timeout: Optional[float] = None,
    discover_timeout: Optional[float] = None,
    queue: bool = True,
    in_reply_to: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
    expires_at: Optional[float] = None,
    wait: float = 0.0,
    send_after: Optional[float] = None,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Fan one group message out on the running node, for the local send server.

    Mirrors the one-shot path in :func:`cmd_group_send` (same
    ``--wait`` handling, same queued-to-delivered flush
    reconciliation) so a group send routed through the serving node
    reports the same result.  The flush reconciliation reads the
    sent record instead of the delivery callback the one-shot path
    installs at construction: the serving node is already running
    without one, and the flush records a confirmed delivery on the
    sent record before the entry leaves the outbox.  ``leg_timeout``
    is the per-leg bound forwarded by the client; see
    :func:`_retract_group_local_message` for why it is not the reply
    wait.
    """
    node.groups.list_groups()  # reload: a group made by another process
    record = node.groups.find(group)
    if record is None:
        raise ValueError(f"unknown group: {group!r}")
    response = node.send_group_message(
        record,
        text,
        timeout=float(leg_timeout if leg_timeout is not None else timeout),
        discover_timeout=discover_timeout,
        queue=bool(queue),
        in_reply_to=in_reply_to,
        expires_at=expires_at,
        send_after=send_after,
        location=location,
        poll_options=poll_options,
        poll_vote=poll_vote,
        poll_close=poll_close,
    )
    if wait > 0:
        _wait_for_queued_sends(
            node,
            [result for result in response["results"] if result.get("queued")],
            wait,
        )
    elif any(result.get("queued") for result in response["results"]):
        # One-shot send: give reachable members a brief chance to take
        # a queued leg before this process exits.
        node.flush_outbox(timeout=3.0, max_peers=5)
        for result in response["results"]:
            if not result.get("queued"):
                continue
            # Only a recorded delivery counts as delivered; an entry
            # that merely vanished was cancelled, expired, or taken by
            # another process.
            sent = node.mailbox.sent_record(result["id"])
            if sent is not None and sent.get("delivered_at") is not None:
                result["queued"] = False
                result["delivered"] = True
            elif node.mailbox.get_outbox(result["id"]) is None:
                result["queued"] = False
                result["ok"] = False
                result["error"] = "left the outbox without a delivery confirmation"
    return response


def _retract_local_message(
    node: TauNode,
    *,
    target: str,
    message_id: str,
    timeout: float = 20.0,
    discover_timeout: Optional[float] = None,
    queue: bool = True,
) -> Dict[str, Any]:
    """Retract one DM on the running node, for the local send server."""
    return node.send_retraction(
        target,
        message_id,
        timeout=float(timeout),
        discover_timeout=discover_timeout,
        queue=bool(queue),
    )


def _retract_group_local_message(
    node: TauNode,
    *,
    group: str,
    message_id: str,
    target_sent_at: float,
    timeout: float = 20.0,
    leg_timeout: Optional[float] = None,
    discover_timeout: Optional[float] = None,
    target_author: Optional[str] = None,
    queue: bool = True,
) -> Dict[str, Any]:
    """Retract a group message on the running node, for the local send server.

    ``leg_timeout`` is the per-leg bound the client forwards in the
    request (see :func:`_retract_group_via_running_node`): the reply
    wait on the socket is unbounded for a fan-out, so the operation
    timeout cannot travel as ``timeout`` — send_running swallows that
    keyword as the reply wait.  An older client sends no leg timeout
    and the default bound applies.  ``target_author`` names the
    message's author for a moderator retract; an author retract (and
    an older client) omits it and the fan-out stamps this node.
    ``queue`` queues unreachable legs like the CLI does; an older
    client omits it and the default applies.
    """
    return node.send_group_retraction(
        group,
        message_id,
        timeout=float(leg_timeout if leg_timeout is not None else timeout),
        discover_timeout=discover_timeout,
        target_sent_at=target_sent_at,
        target_author=target_author,
        queue=bool(queue),
    )


def _edit_local_message(
    node: TauNode,
    *,
    target: str,
    message_id: str,
    text: str,
    timeout: float = 20.0,
    discover_timeout: Optional[float] = None,
    queue: bool = True,
) -> Dict[str, Any]:
    """Edit one DM on the running node, for the local send server."""
    return node.send_edit(
        target,
        message_id,
        text,
        timeout=float(timeout),
        discover_timeout=discover_timeout,
        queue=bool(queue),
    )


def _edit_group_local_message(
    node: TauNode,
    *,
    group: str,
    message_id: str,
    text: str,
    target_sent_at: float,
    timeout: float = 20.0,
    leg_timeout: Optional[float] = None,
    discover_timeout: Optional[float] = None,
    queue: bool = True,
) -> Dict[str, Any]:
    """Edit one group message on the running node, for the local send server.

    ``leg_timeout`` is the per-leg bound the client forwards in the
    request (see :func:`_edit_group_via_running_node`): the reply
    wait on the socket is unbounded for a fan-out, so the operation
    timeout cannot travel as ``timeout`` — send_running swallows that
    keyword as the reply wait.  An older client sends no leg timeout
    and the default bound applies.
    """
    return node.send_group_edit(
        group,
        message_id,
        text,
        timeout=float(leg_timeout if leg_timeout is not None else timeout),
        discover_timeout=discover_timeout,
        target_sent_at=float(target_sent_at),
        queue=bool(queue),
    )


def _typing_local_message(
    node: TauNode,
    *,
    target: str,
    state: str,
    timeout: float = 20.0,
    discover_timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Send one DM typing event on the running node, for the local send server."""
    response = node.send_typing(
        target,
        state,
        timeout=float(timeout),
        discover_timeout=discover_timeout,
    )
    peer = node.registry.get(str(response.get("to") or ""))
    return dict(response, to_name=peer.display_name if peer is not None else "")


def _typing_group_local_message(
    node: TauNode,
    *,
    group: str,
    state: str,
    timeout: float = 20.0,
    leg_timeout: Optional[float] = None,
    discover_timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Fan a group typing event out on the running node, for the local send server.

    ``leg_timeout`` is the per-leg bound forwarded by the client; see
    :func:`_edit_group_local_message` for why it is not the reply wait.
    """
    node.groups.list_groups()  # reload: a group made by another process
    record = node.groups.find(group)
    if record is None:
        raise ValueError(f"unknown group: {group!r}")
    return node.send_group_typing(
        record,
        state,
        timeout=float(leg_timeout if leg_timeout is not None else timeout),
        discover_timeout=discover_timeout,
    )


def _dispatch_local_send(
    node: TauNode, *, bridge: Any = None, **request: Any
) -> Dict[str, Any]:
    """Serve one local send-socket request: a DM send, a group send, a
    retraction, a group retraction, an edit, a group edit, or a
    console-copy drop.

    The socket carries them all, distinguished by ``op``: ``"send"``
    (the default, so older one-shot clients keep working) runs
    :func:`_send_local_message`, ``"group_send"`` runs
    :func:`_send_group_local_message`, while ``"retract"``,
    ``"retract_group"``, ``"edit"`` and ``"edit_group"`` run the
    retraction, the group retraction, the edit and the group edit on
    the serving node, so ``tau retract`` and ``tau edit`` work while
    ``tau serve`` holds the data directory.

    ``"drop_console_copy"`` and ``"drop_console_group"`` drop the
    serving console's copy of a retracted sent message.  The CLI sends
    them only after its own inbox delete succeeds, so the console
    never loses a copy while the inbox still holds one — a retract
    used to drop the console copy inside the ``"retract"`` op, before
    the CLI deleted its inbox copy, so a failing inbox delete left
    the console copy gone while the inbox record was restored.  The
    drop is best-effort and never fails the retract itself: the
    retraction already succeeded on the wire, so a console failure
    only logs — the entry stays, exactly as before this change.
    """
    op = request.pop("op", "send")
    if op == "retract":
        return _retract_local_message(node, **request)
    if op == "retract_group":
        return _retract_group_local_message(node, **request)
    if op == "drop_console_copy":
        # Sent by the CLI only after its inbox delete succeeded.
        _drop_sent_console_copy(bridge, request.get("message_id"))
        return {"ok": True}
    if op == "drop_console_group":
        _drop_sent_console_group(
            bridge,
            request.get("group"),
            request.get("target_sent_at"),
        )
        return {"ok": True}
    if op == "edit":
        return _edit_local_message(node, **request)
    if op == "edit_group":
        return _edit_group_local_message(node, **request)
    if op == "typing":
        return _typing_local_message(node, **request)
    if op == "typing_group":
        return _typing_group_local_message(node, **request)
    if op == "beacon_start":
        return _beacon_start_local(node, **request)
    if op == "send":
        return _send_local_message(node, **request)
    if op == "group_send":
        return _send_group_local_message(node, **request)
    raise NodeError(f"unknown local send operation: {op!r}")


def _drop_retracted_console_copy(bridge: Any, record: Any) -> None:
    """Drop the serving console's entry for one retracted DM.

    The node already deleted the inbox record; this drops the
    console's own copy so a retracted DM leaves the feed and never
    comes back after a restart.  A record this node sent goes
    through the sent side — ``delete_by_wire_id`` only matches
    inbound entries, so a queued retract's outbound bubble would
    otherwise stay until the next restart.  Best-effort: a console
    that cannot drop its copy leaves the retract itself untouched.
    """
    if bridge is None:
        return
    wire_id = str(record.get("id") or "") if isinstance(record, dict) else ""
    if not wire_id:
        return
    try:
        if record.get("direction") == "out":
            bridge.delete_sent_by_wire_id(wire_id)
        else:
            bridge.delete_by_wire_id(wire_id)
    except Exception:  # pragma: no cover - console callback
        log.warning("could not drop retracted message from console", exc_info=True)


def _drop_sent_console_copy(bridge: Any, message_id: Any) -> None:
    """Drop the serving console's outbound entry for one retracted DM.

    Best-effort: a console that cannot drop its copy (or has no entry
    for it) leaves the retract itself untouched — the CLI still deletes
    the local inbox copy, and the entry stays as it did before.
    """
    if bridge is None:
        return
    if not isinstance(message_id, str) or not message_id:
        return
    try:
        bridge.delete_sent_by_wire_id(message_id)
    except Exception:  # pragma: no cover - console callback
        log.warning("could not drop retracted message from console", exc_info=True)


def _drop_sent_console_group(
    bridge: Any, group: Any, target_sent_at: Any
) -> None:
    """Drop the serving console's outbound legs for one retracted group send.

    Best-effort, like :func:`_drop_sent_console_copy`: the fan-out
    already reached every member, so a console failure only logs.
    """
    if bridge is None:
        return
    if not isinstance(group, str) or not group:
        return
    if (
        not isinstance(target_sent_at, (int, float))
        or isinstance(target_sent_at, bool)
    ):
        return
    try:
        bridge.delete_sent_group(group, float(target_sent_at))
    except Exception:  # pragma: no cover - console callback
        log.warning(
            "could not drop retracted group message from console",
            exc_info=True,
        )


def _send_via_local_node(
    args: argparse.Namespace,
    config: TauConfig,
    text: str,
    targets: List[str],
    expires_at: Optional[float],
    reply_ids: Dict[str, str],
    kind: Optional[str] = None,
    wait: float = 0.0,
    send_after: Optional[float] = None,
    location: Optional[Dict[str, Any]] = None,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
    node: Optional[TauNode] = None,
) -> List[Dict[str, Any]]:
    """One-shot send on a node this process starts and stops itself.

    Pass an already-started *node* to send on it instead — the caller
    owns starting and stopping it, so several sends can share one
    Reticulum instance in a process (a second ``TauNode.start()``
    fails: Reticulum cannot be reinitialised in the same process).
    """
    timeout = float(getattr(args, "timeout", 20.0))
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    owned_node = node is None
    if owned_node:
        log.info("starting a local one-shot node (data dir %s)", config.data_dir)
        node = TauNode(config, name=config.name)
        node.start()
    results: List[Dict[str, Any]] = []
    try:
        for target in targets:
            # Outbox ids present before this call: only this node's own
            # queue path creates an entry for the message it just queued,
            # so a "queued" reply is only genuine when its id is new
            # since this snapshot. A peer claiming queued with a
            # pre-existing id (e.g. one it holds a sealed copy of) is
            # treated as delivered — the link request succeeded.
            queued_before = {entry.id for entry in node.mailbox.outbox_list()}
            started = time.monotonic()
            try:
                reply_to = None
                if getattr(args, "reply_to", None):
                    reply_to = _resolve_send_reply_to(
                        node,
                        target,
                        args.reply_to,
                        discover_timeout,
                    )
                    reply_ids[target] = reply_to["id"]
                log.debug(
                    "sending to %s via the local node "
                    "(timeout %.0fs, discover %.0fs)",
                    target,
                    timeout,
                    discover_timeout,
                )
                response = node.send_message(
                    target,
                    text,
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                    queue=not getattr(args, "no_queue", False),
                    in_reply_to=reply_to,
                    expires_at=expires_at,
                    kind=kind,
                    location=location,
                    send_after=send_after,
                    poll_options=poll_options,
                    poll_vote=poll_vote,
                    poll_close=poll_close,
                )
            except (NodeError, PeerError, MailboxError) as exc:
                log.debug(
                    "send to %s failed after %.2fs: %s",
                    target,
                    time.monotonic() - started,
                    exc,
                )
                if len(targets) == 1:
                    # Single recipient keeps the historical behaviour:
                    # the error reaches main() and exits non-zero.
                    raise
                # One recipient failing must not stop the rest; the
                # failure is reported per recipient below. This also
                # covers a full outbox: enqueue raises MailboxFull
                # inside send_message, and that must not abort the
                # remaining recipients.
                results.append({"target": target, "ok": False, "error": str(exc)})
                continue
            result = _finalize_send_result(
                response,
                target=target,
                expires_at=expires_at,
                queued_before=queued_before,
            )
            if result.get("queued"):
                outcome = f"queued (id {short_hash(str(result.get('id', '')))})"
            else:
                outcome = "delivered"
            log.debug(
                "send to %s finished in %.2fs: %s",
                target,
                time.monotonic() - started,
                outcome,
            )
            results.append(result)
        if wait > 0:
            queued = sum(1 for result in results if result.get("queued"))
            log.debug(
                "waiting up to %.1fs for %d queued send(s)", wait, queued
            )
            _wait_for_queued_sends(node, results, wait)
        else:
            _reconcile_queued_sends(node, results, expires_at)
    finally:
        if owned_node:
            node.stop()
    return results


def _send_via_running_node(
    args: argparse.Namespace,
    config: TauConfig,
    text: str,
    targets: List[str],
    expires_at: Optional[float],
    reply_ids: Dict[str, str],
    kind: Optional[str] = None,
    wait: float = 0.0,
    send_after: Optional[float] = None,
    location: Optional[Dict[str, Any]] = None,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Send through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    timeout = float(getattr(args, "timeout", 20.0))
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    results: List[Dict[str, Any]] = []
    for target in targets:
        started = time.monotonic()
        try:
            log.debug(
                "sending to %s via the running node's send socket "
                "(timeout %.0fs, discover %.0fs)",
                target,
                timeout,
                discover_timeout,
            )
            result = send_running(
                config.data_dir,
                target=target,
                text=text,
                timeout=timeout,
                discover_timeout=discover_timeout,
                queue=not getattr(args, "no_queue", False),
                expires_at=expires_at,
                reply_to=getattr(args, "reply_to", None),
                kind=kind,
                location=location,
                wait=wait,
                send_after=send_after,
                poll_options=poll_options,
                poll_vote=poll_vote,
                poll_close=poll_close,
            )
        except LocalIPCUnavailable as exc:
            raise NodeError(
                "the running node stopped answering mid-send; retry the send"
            ) from exc
        except (NodeError, PeerError, MailboxError) as exc:
            log.debug(
                "send to %s failed after %.2fs: %s",
                target,
                time.monotonic() - started,
                exc,
            )
            if len(targets) == 1:
                raise
            results.append({"target": target, "ok": False, "error": str(exc)})
            continue
        reply_id = result.pop("_reply_id", None)
        if reply_id is not None:
            reply_ids[target] = reply_id
        if result.get("queued"):
            outcome = f"queued (id {short_hash(str(result.get('id', '')))})"
        elif result.get("delivered") is False:
            outcome = "sent (no delivery confirmation)"
        else:
            outcome = "delivered"
        log.debug(
            "send to %s finished in %.2fs: %s",
            target,
            time.monotonic() - started,
            outcome,
        )
        results.append(result)
    return results


def _retract_via_running_node(
    config: TauConfig,
    recipient: str,
    message_id: str,
    *,
    timeout: float,
    discover_timeout: Optional[float],
    queue: bool = True,
) -> Dict[str, Any]:
    """Retract through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    try:
        return send_running(
            config.data_dir,
            op="retract",
            target=recipient,
            message_id=message_id,
            timeout=timeout,
            discover_timeout=discover_timeout,
            queue=queue,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "the running node stopped answering mid-retract; retry the retract"
        ) from exc


def _retract_group_via_running_node(
    config: TauConfig,
    group_id: str,
    message_id: str,
    target_sent_at: float,
    *,
    timeout: float,
    discover_timeout: Optional[float],
    target_author: Optional[str] = None,
    queue: bool = True,
) -> Dict[str, Any]:
    """Retract a group message through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    try:
        # The per-leg timeout travels in the request; the reply wait stays
        # unbounded because a fan-out can outlast any single leg's bound.
        return send_running(
            config.data_dir,
            op="retract_group",
            group=group_id,
            message_id=message_id,
            target_sent_at=target_sent_at,
            target_author=target_author,
            leg_timeout=timeout,
            discover_timeout=discover_timeout,
            queue=queue,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "the running node stopped answering mid-retract; retry the retract"
        ) from exc


def _edit_group_via_running_node(
    config: TauConfig,
    group_id: str,
    message_id: str,
    text: str,
    target_sent_at: float,
    *,
    timeout: float,
    discover_timeout: Optional[float],
    queue: bool = True,
) -> Dict[str, Any]:
    """Edit a group message through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    try:
        # The per-leg timeout travels in the request; the reply wait stays
        # unbounded because a fan-out can outlast any single leg's bound.
        return send_running(
            config.data_dir,
            op="edit_group",
            group=group_id,
            message_id=message_id,
            text=text,
            target_sent_at=target_sent_at,
            leg_timeout=timeout,
            discover_timeout=discover_timeout,
            queue=queue,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "the running node stopped answering mid-edit; retry the edit"
        ) from exc


#: How long the best-effort console-copy drop waits for the serving
#: node before giving up.  The durable retraction marker is recorded
#: before the notification is sent, so a slow drop heals at the next
#: serve start instead of hanging the retract.
_DROP_CONSOLE_COPY_TIMEOUT_SECONDS = 10.0


def _drop_console_copy_via_running_node(
    config: TauConfig,
    *,
    message_id: Optional[str] = None,
    group: Optional[str] = None,
    target_sent_at: Optional[float] = None,
) -> None:
    """Ask the serving node to drop its console copy of a retracted send.

    Called only after the local inbox delete succeeded and the
    durable retraction marker is recorded.  Best-effort: the retract
    already succeeded on the wire, so a failure — including a serving
    node too slow to answer within the bound — only logs and never
    fails the retract.  The marker heals a missed drop at the next
    serve start.
    """
    from .local_send import send_running

    try:
        if group is None:
            send_running(
                config.data_dir,
                reply_timeout=_DROP_CONSOLE_COPY_TIMEOUT_SECONDS,
                op="drop_console_copy",
                message_id=message_id,
            )
        else:
            send_running(
                config.data_dir,
                reply_timeout=_DROP_CONSOLE_COPY_TIMEOUT_SECONDS,
                op="drop_console_group",
                group=group,
                target_sent_at=target_sent_at,
            )
    except Exception:
        log.warning(
            "could not drop retracted message from the serving console",
            exc_info=True,
        )


def _edit_via_running_node(
    config: TauConfig,
    recipient: str,
    message_id: str,
    text: str,
    *,
    timeout: float,
    discover_timeout: Optional[float],
    queue: bool = True,
) -> Dict[str, Any]:
    """Edit through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    try:
        return send_running(
            config.data_dir,
            op="edit",
            target=recipient,
            message_id=message_id,
            text=text,
            timeout=timeout,
            discover_timeout=discover_timeout,
            queue=queue,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "the running node stopped answering mid-edit; retry the edit"
        ) from exc


def _typing_via_running_node(
    config: TauConfig,
    target: str,
    state: str,
    *,
    timeout: float,
    discover_timeout: Optional[float],
) -> Dict[str, Any]:
    """Send a DM typing event through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    try:
        return send_running(
            config.data_dir,
            op="typing",
            target=target,
            state=state,
            timeout=timeout,
            discover_timeout=discover_timeout,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "the running node stopped answering mid-typing; retry shortly"
        ) from exc


def _typing_group_via_running_node(
    config: TauConfig,
    group_id: str,
    state: str,
    *,
    timeout: float,
    discover_timeout: Optional[float],
) -> Dict[str, Any]:
    """Fan a group typing event out through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    try:
        # The per-leg timeout travels in the request; the reply wait stays
        # unbounded because a fan-out can outlast any single leg's bound.
        return send_running(
            config.data_dir,
            op="typing_group",
            group=group_id,
            state=state,
            leg_timeout=timeout,
            discover_timeout=discover_timeout,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "the running node stopped answering mid-typing; retry shortly"
        ) from exc


#: How long a one-shot send waits for a serving node's socket to appear
#: (or a transient lock holder to finish) before failing closed.
_SEND_ROUTE_WAIT_SECONDS = 10.0


def _instance_lock_free(config: TauConfig) -> bool:
    """True when no live process holds the data-directory lock.

    The probe is the ground truth: taking the lock (and releasing it)
    is the only way to know no holder owns the directory.  A PID read
    from the lock file is never trusted on its own — a holder records
    its PID just after taking the lock, so the file can still name a
    dead predecessor while a new holder is starting up, and a dead
    PID then reads as "free" while a serve is mid-startup.  The probe
    holds the lock only for microseconds; a `tau serve` starting in
    that exact window fails fast with "already serving" and can be
    retried, which beats a false "free" starting a second node on the
    same identity.
    """
    probe = InstanceLock(config.instance_lock_path, config.data_dir)
    try:
        probe.acquire()
    except InstanceAlreadyRunning:
        return False
    probe.release()
    return True


def _send_routes_via_running_node(config: TauConfig) -> bool:
    """True when a serving node should carry the send; False for a local node.

    A serving node holds the data-directory lock for its whole life but
    answers on its send socket only between startup and shutdown. A
    socket that answers wins. Otherwise the lock decides: when it is free
    no node owns the directory (a stale socket is harmless), so a local
    one-shot node may start. When it is held, the serve may still be
    starting, so poll briefly for the socket; the holder may also be
    transient — a standalone `tau tan post` holds the lock without
    running a send server — so the poll also watches for the lock to
    free, and a local send proceeds the moment it does. Only a lock
    still held after the wait fails closed instead of starting a second
    node on the same identity.
    """
    from .local_send import send_server_available

    has_socket = hasattr(socket, "AF_UNIX")
    if has_socket and send_server_available(config.data_dir):
        log.debug("send socket answers; routing via the running node")
        return True
    config.prepare()
    if _instance_lock_free(config):
        log.debug(
            "no serving node holds the data directory; "
            "starting a local one-shot node"
        )
        return False
    log.debug(
        "data directory is locked; waiting for the serving node's send socket"
    )
    deadline = time.monotonic() + _SEND_ROUTE_WAIT_SECONDS
    while True:
        if has_socket and send_server_available(config.data_dir):
            log.debug("send socket answered while waiting; routing via the running node")
            return True
        if _instance_lock_free(config):
            log.debug(
                "data-directory lock freed while waiting; "
                "starting a local one-shot node"
            )
            return False
        if time.monotonic() >= deadline:
            break
        time.sleep(0.1)
    raise NodeError(
        "a tau node holds this data directory but is not accepting "
        "sends; retry shortly"
    ) from None


def cmd_backup(args: argparse.Namespace) -> int:
    config = _make_config(args)
    target, manifest = create_backup(
        config, args.file, force=getattr(args, "force", False)
    )
    identity = load_identity(config.identity_path)
    payload = {
        "backup": str(target),
        "node": manifest.get("node"),
        "destination": destination_hash(identity).hex() if identity else None,
        "files": manifest.get("files"),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"node:        {payload['node']}")
        print(f"destination: {payload['destination']}")
        print(f"backup:      {payload['backup']}")
        print(f"files:       {len(payload['files'] or [])}")
    return _EXIT_OK


def cmd_restore(args: argparse.Namespace) -> int:
    config = _make_config(args)
    manifest, files = restore_backup(
        config, args.file, force=getattr(args, "force", False)
    )
    identity = load_identity(config.identity_path)
    payload = {
        "data_dir": str(config.data_dir),
        "node": manifest.get("node"),
        "destination": destination_hash(identity).hex() if identity else None,
        "files": files,
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"node:        {payload['node']}")
        print(f"destination: {payload['destination']}")
        print(f"data dir:    {payload['data_dir']}")
        print(f"files:       {len(payload['files'])}")
    return _EXIT_OK


def cmd_id_rename(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    raw_name = getattr(args, "new_name", None)
    if not isinstance(raw_name, str):
        raise NodeError("new name must not be empty")
    # Empty fallback: a name that sanitises to nothing (blank, or control
    # characters only) is rejected instead of silently becoming "anonymous".
    new_name = sanitize_name(raw_name, fallback="")
    if not new_name:
        raise NodeError("new name must not be empty")
    # Read the stored name, not the resolved one: a global --name flag
    # would override the resolved name and misreport the rename.
    old_name = load_node_name(config.data_dir) or config.name
    payload = {
        "name": new_name,
        "previous_name": old_name,
        "changed": new_name != old_name,
        "destination": destination_hash(identity).hex(),
    }
    if new_name == old_name:
        if _json_mode(args):
            _emit(payload, True)
        else:
            print(f'Node is already named "{old_name}".')
        return _EXIT_OK
    # A running node announces with its in-memory name, so a rename under
    # it would sit ignored on disk until restart. Probe the instance lock
    # (same pattern as `tau serve`) and refuse while the node is serving.
    # Hold the lock through the write so a serve starting now cannot read
    # the old name and then start announcing it after the write lands.
    probe = InstanceLock(config.instance_lock_path, config.data_dir)
    probe.acquire()
    try:
        write_node_metadata(config.data_dir, new_name)
    finally:
        probe.release()
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f'Renamed node "{old_name}" to "{new_name}".')
    return _EXIT_OK


def _send_results_json(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """The JSON payload ``cmd_send`` emits for *results*.

    A single successful recipient keeps the historical bare-response
    shape; anything else reports the per-recipient results with
    delivered / queued / failed counts.
    """
    failed = [result for result in results if not result.get("ok", True)]
    if len(results) == 1 and not failed:
        return {k: v for k, v in results[0].items() if k != "target"}
    return {
        "results": results,
        "delivered": sum(
            1
            for r in results
            if r.get("ok", True)
            and not r.get("queued")
            and r.get("delivered") is not False
        ),
        "queued": sum(1 for r in results if r.get("queued")),
        "failed": len(failed),
    }


def _report_send_results(
    args: argparse.Namespace,
    results: List[Dict[str, Any]],
    reply_ids: Dict[str, str],
    expires_at: Optional[float],
) -> int:
    """Report *results* the way ``cmd_send`` does and return its exit code.

    JSON mode emits one document; human mode prints one line per
    recipient plus the queued hint.  A forward reuses this for its
    attribution text, so the two paths report a send the same way.
    """
    failed = [result for result in results if not result.get("ok", True)]
    # --wait outcomes are recorded on the results by the send path
    # (see _wait_for_queued_sends): a waited result that is not
    # delivered failed the wait, whether it is still queued or left the
    # queue without a delivery confirmation.
    wait_failed = any(
        result.get("waited") is not None and result.get("delivered") is False
        for result in results
    )
    if _json_mode(args):
        _emit(_send_results_json(results), True)
    else:
        for result in results:
            target = result["target"]
            if not result.get("ok", True):
                print(f"failed to send to {target}: {result['error']}", file=sys.stderr)
            elif (
                result.get("waited") is not None
                and not result.get("delivered")
                and not result.get("queued")
            ):
                # The entry left the queue while waiting without a delivery
                # confirmation from this node: cancelled, or delivered by
                # another process.  Say so instead of claiming delivery.
                print(f"no longer queued for {target} after {result['waited']:.1f}s")
                print("  delivery was not confirmed — it may have been cancelled,")
                print("  or delivered by another process sharing this data directory")
            elif result.get("queued"):
                waited = result.get("waited")
                if waited is not None:
                    print(f"still queued for {target} after {waited:.1f}s")
                elif result.get("send_after"):
                    print(
                        f"scheduled for {target} — sends in "
                        f"{_ago(result['send_after'] - time.time())}"
                    )
                else:
                    print(f"queued for {target} — they are unreachable right now")
                print(f"  id:      {result['id']}")
                if reply_ids.get(target):
                    print(f"  in reply to {short_hash(reply_ids[target])}")
                if expires_at is not None:
                    print(f"  disappears in {_ago(expires_at - time.time())}")
            elif result.get("delivered"):
                waited = result.get("waited")
                if (
                    waited is not None
                    and "_rtt" not in result
                    and "echo" not in result
                ):
                    print(f"delivered to {target} after {waited:.1f}s")
                else:
                    print(f"delivered to {target}")
                if reply_ids.get(target):
                    print(f"  in reply to {short_hash(reply_ids[target])}")
                if expires_at is not None:
                    print(f"  disappears in {_ago(expires_at - time.time())}")
            else:
                print(f"delivered to {target}")
                if reply_ids.get(target):
                    print(f"  in reply to {short_hash(reply_ids[target])}")
                echo = result.get("echo")
                if echo is not None:
                    print(f"echo: {echo}")
                rtt = result.get("_rtt")
                if isinstance(rtt, (int, float)):
                    print(f"round trip: {rtt * 1000:.0f} ms")
                if result.get("expired"):
                    print("  the receiver dropped it — past its expiry")
                if expires_at is not None:
                    print(f"  disappears in {_ago(expires_at - time.time())}")
        queued = [result for result in results if result.get("queued")]
        if queued:
            if all(result.get("send_after") for result in queued):
                print("  it will be delivered at its scheduled time;")
            else:
                print(
                    "  it will be delivered automatically when they are heard from;"
                )
            print("  check `tau outbox` for the queue, or just leave a node serving")
    return _EXIT_ERROR if (failed or wait_failed) else _EXIT_OK


def cmd_send(args: argparse.Namespace) -> int:
    # Validate flags before reading the message, so an invalid command
    # fails at once instead of blocking on stdin.
    wait_given = getattr(args, "wait", None) is not None
    wait = float(getattr(args, "wait", None) or 0.0)
    if not math.isfinite(wait):
        print("error: --wait must be a finite number of seconds", file=sys.stderr)
        return 2
    if wait < 0:
        print("error: --wait must not be negative", file=sys.stderr)
        return 2
    if wait_given and getattr(args, "no_queue", False):
        print("error: --wait cannot be combined with --no-queue", file=sys.stderr)
        return 2
    poll_options = None
    if getattr(args, "poll_options", None):
        try:
            poll_options = clean_poll_options(list(args.poll_options))
        except PostError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    # Validate the pin before reading the message, so an invalid
    # --location fails at once instead of blocking on stdin.
    location = _location_from_args(args)
    text = _message_text_with_location(args, location)
    config = _make_config(args)
    try:
        # No --kind means no kind: the node tags a pin ``location``
        # and defaults anything else to ``chat``.
        kind = (
            normalize_message_kind(getattr(args, "kind", None))
            if getattr(args, "kind", None) is not None
            else None
        )
    except ProtocolError as exc:
        raise NodeError(str(exc)) from exc
    expires_at = None
    expire_text = getattr(args, "expire", None)
    if expire_text is not None:
        expires_at = time.time() + _parse_duration(expire_text)
    send_after = None
    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        if getattr(args, "no_queue", False):
            print(
                "error: --delay cannot be combined with --no-queue",
                file=sys.stderr,
            )
            return 2
        delay = _parse_duration(delay_text, flag="--delay")
        if delay > MAX_MAIL_TTL - 3600.0:
            print(
                "error: --delay must be at most 29d23h (the 30d mail TTL "
                "less an hour of delivery slack)",
                file=sys.stderr,
            )
            return 2
        send_after = time.time() + delay
        if expires_at is not None and send_after >= expires_at:
            print(
                "error: --delay must be shorter than --expire — the "
                "message would expire before it is sent",
                file=sys.stderr,
            )
            return 2
    targets = args.to if isinstance(args.to, list) else [args.to]
    reply_ids: Dict[str, str] = {}
    log.debug(
        "send: %d target(s), kind=%s, wait=%.1fs, expires_at=%s, send_after=%s",
        len(targets),
        kind,
        wait,
        expires_at,
        send_after,
    )
    # A serving node owns the data directory: send through it instead of
    # starting a second Reticulum instance on the same identity.
    if _send_routes_via_running_node(config):
        log.info("sending via the running node")
        results = _send_via_running_node(
            args, config, text, targets, expires_at, reply_ids, kind=kind, wait=wait,
            send_after=send_after, location=location,
            poll_options=poll_options,
        )
    else:
        log.info("sending via a local one-shot node")
        results = _send_via_local_node(
            args, config, text, targets, expires_at, reply_ids, kind=kind, wait=wait,
            send_after=send_after, location=location,
            poll_options=poll_options,
        )

    return _report_send_results(args, results, reply_ids, expires_at)


def cmd_forward(args: argparse.Namespace) -> int:
    """Forward a stored message to another peer, into a group, into a
    channel, or to every member of a broadcast list, attributed to its
    sender.

    The forwarded text is the original prefixed with one attribution
    line, so the recipient sees who said it on any transport with no
    protocol change.  Delivery reuses :func:`cmd_send` wholesale for a
    peer target — :func:`cmd_group_send` for a group target, fanning
    out one leg per member, or :func:`cmd_broadcast_send` for a
    broadcast target, one DM per member, or :func:`cmd_channel_send`
    for a channel target.  Either way the forward is a new message, so
    it carries no expiry, kind, or reply chain from the original.
    """
    config = _make_config(args)
    # `--timeout` defaults to None: a text forward uses the send
    # default (20s) while a file forward uses the send-file default
    # (300s) — a file transfer outlasts a text send.  An explicit
    # --timeout applies to both legs of a text forward, and to the
    # file leg of a file forward.
    file_timeout = getattr(args, "timeout", None)
    if file_timeout is None:
        args.timeout = 20.0
        file_timeout = 300.0
    else:
        file_timeout = float(file_timeout)
    inbox = Inbox(config.inbox_path)
    try:
        record = inbox.resolve(args.message)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if record.get("kind") == "group":
        # Group messages live in the same inbox but belong to the group:
        # forwarding one would leak the group's text as a DM to any
        # --to recipient, including a nonmember.
        raise NodeError("cannot forward a group message: forward a DM instead")
    # Messages hidden by the block-list are hidden from `tau messages`
    # and must not be forwarded either.  An unreadable block-list fails
    # closed, matching the listing.  Sent messages carry the *recipient*
    # in `destination`, so the check only applies to received ones —
    # forwarding your own message must not trip on its old recipient.
    if record.get("direction") != "out":
        blocked = load_blocked_set(config.blocklist_path)
        if blocked is None:
            raise NodeError("cannot forward: the block-list could not be read")
        if record.get("destination") in blocked:
            raise NodeError("cannot forward a message from a blocked peer")
    text = record.get("text")
    if record.get("direction") == "out":
        # The message is one this node sent; the recipient already knows
        # the forwarder, so the attribution names the forwarder as "you".
        label = "you"
    else:
        # Inbox records already store the sender's display name as known
        # when the message arrived; re-resolve it so a newer alias wins,
        # falling back to the stored label (a name, or the short hash).
        sender_hash = record.get("destination")
        stored_name = record.get("from")
        if not stored_name or stored_name == "unknown":
            # No real name was ever recorded: attribute by short hash.
            label = (
                short_hash(sender_hash)
                if isinstance(sender_hash, str) and sender_hash
                else "unknown"
            )
        else:
            label = str(stored_name)
        peer = (
            PeerRegistry(config.peers_path).get(sender_hash)
            if isinstance(sender_hash, str) and sender_hash
            else None
        )
        if peer is not None:
            label = peer.display_name
    if record.get("kind") == "file":
        # A file record carries no text: forward the stored bytes
        # instead, with the original sender named in the file offer
        # itself.  A record that arrived already forwarded keeps its
        # original sender; otherwise the sender of this record is
        # the original.
        forwarded_from = clean_forwarded_from_or_none(
            record.get("forwarded_from")
        )
        if forwarded_from is None:
            sender_hash = record.get("destination")
            if isinstance(sender_hash, str) and sender_hash:
                forwarded_from = {"hash": sender_hash, "name": label}
        return _forward_file(
            args,
            config,
            record,
            file_timeout=file_timeout,
            forwarded_from=forwarded_from,
        )
    if not text:
        raise NodeError("cannot forward a message with no text to send")
    forwarded = f"Forwarded from {label}:\n{text}"
    if len(forwarded) > MAX_MESSAGE_CHARS:
        raise NodeError(
            f"message is too long to forward "
            f"({len(forwarded)} > {MAX_MESSAGE_CHARS} characters)"
        )
    # `cmd_send` reads its text from `args.message`; hand it the
    # attributed text and let it own delivery, queueing, and output.
    # A group target takes the group-send path instead: the same
    # attributed text, fanned out one leg per member.  A broadcast
    # target takes the broadcast-send path the same way: one DM per
    # member, each with the same store-and-forward queueing as a
    # one-to-one DM, so every delivery flag keeps its meaning.  A
    # channel target takes the channel-send path the same way.
    args.message = forwarded
    if getattr(args, "group", None):
        return cmd_group_send(args)
    if getattr(args, "broadcast", None):
        return cmd_broadcast_send(args)
    if getattr(args, "channel", None):
        # `--no-queue`, `--wait`, and `--discover-timeout` only mean
        # something for peer/group delivery; the channel-send path
        # never reads them, so refuse them here instead of silently
        # ignoring them.
        if getattr(args, "no_queue", False):
            raise NodeError("--no-queue does not apply to a channel forward")
        if getattr(args, "wait", None) is not None:
            raise NodeError("--wait does not apply to a channel forward")
        if getattr(args, "discover_timeout", None) is not None:
            raise NodeError(
                "--discover-timeout does not apply to a channel forward"
            )
        # `cmd_channel_send` reads `args.to` as a publish restriction
        # ("publish only to these peers"); the forward's own `--to`
        # is a peer target, unset on this branch by the parser, but
        # clear it explicitly so the two meanings can never mix.
        args.to = None
        return cmd_channel_send(args)
    return cmd_send(args)


def _forward_file(
    args: argparse.Namespace,
    config: Any,
    record: Dict[str, Any],
    *,
    file_timeout: float = 300.0,
    forwarded_from: Optional[Dict[str, Any]] = None,
) -> int:
    """Forward a stored received file to a peer or into a group.

    The file itself carries the attribution: the offer names the
    original sender in ``forwarded_from``, so the recipient's file
    record shows who sent it first — no separate text leg that could
    arrive out of order or not at all.  The stored bytes are verified
    against the record before anything is sent, so a missing or
    altered file fails cleanly up front.  A failed leg is reported
    per recipient, like ``tau group send-file``.  Scheduling is not
    supported in v1: a delayed file forward would need the staged
    bytes scheduled alongside the offer.

    The file leg starts the first local node, like ``tau send-file``:
    with no text leg left, there is nothing to route through a
    serving node.  The verified bytes are staged once under their own
    name and the file legs send from there, never from the mutable
    stored path, so a change to the stored file between verification
    and delivery cannot swap in different bytes.  ``--json`` emits
    the file result document.

    A file forward only targets a peer or a group: the broadcast and
    channel targets take their text through the broadcast / channel
    send paths, which have no file leg — a file record sent there
    would go to a ``None`` peer.  Refuse those targets with a clear
    error instead of an unrelated send failure.
    """
    if getattr(args, "broadcast", None) or getattr(args, "channel", None):
        print(
            "error: a file cannot be forwarded to a broadcast list or "
            "a channel yet — forward it to a peer or a group instead",
            file=sys.stderr,
        )
        return 2
    if getattr(args, "delay", None) is not None:
        print(
            "error: --delay cannot be used when forwarding a file — "
            "schedule the file itself with `tau send-file --delay`",
            file=sys.stderr,
        )
        return 2
    if getattr(args, "wait", None) is not None:
        # --wait bounded the old attribution text leg; a file leg is
        # a direct transfer with no queue to wait on, so there is
        # nothing left for it to apply to.
        print(
            "error: --wait cannot be used when forwarding a file",
            file=sys.stderr,
        )
        return 2
    if forwarded_from is None:
        print(
            "error: cannot forward a file: the sender is unknown",
            file=sys.stderr,
        )
        return 2
    _path, data = resolve_received_file(record, config.files_dir)
    name = str(record["name"])
    size = len(data)
    digest = hashlib.sha256(data).hexdigest()
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    targets = args.to if isinstance(args.to, list) else [args.to]
    # Stage the verified bytes once: the file legs send from here,
    # never from the mutable stored path.
    with tempfile.TemporaryDirectory(prefix="tau-forward-file-") as staging:
        staged = Path(staging) / name
        staged.write_bytes(data)
        if getattr(args, "group", None):
            return _forward_file_to_group(
                args,
                config,
                staged,
                name,
                size,
                digest,
                forwarded_from,
                file_timeout=file_timeout,
                discover_timeout=discover_timeout,
            )
        return _forward_file_to_peers(
            args,
            config,
            staged,
            name,
            size,
            digest,
            forwarded_from,
            targets,
            file_timeout=file_timeout,
            discover_timeout=discover_timeout,
        )


def _record_forwarded_file(
    node: Any,
    *,
    name: str,
    size: int,
    forwarded_from: Dict[str, Any],
    results: List[Dict[str, Any]],
    group: Optional[Dict[str, Any]] = None,
) -> None:
    """Leave a local trace of a file forward in the forwarder's history.

    A file leg stores nothing locally, so without this a CLI file
    forward vanishes from ``tau messages``: the old attribution text
    leg used to leave the trace.  The record is local only — nothing
    is sent — and mirrors how text sends record: one record per
    delivered peer leg, one record for a whole group fan-out.
    """
    delivered = [r for r in results if r.get("delivered")]
    if not delivered:
        return
    origin = forwarded_from["name"]
    text = f"Forwarded {name} ({size} bytes) — originally from {origin}"
    if group is not None:
        node._record_sent(
            {
                "from": node.name,
                "destination": "",
                "text": text,
                "kind": "group",
                "group": group["id"],
                "group_name": group["name"],
                "sent_at": time.time(),
                "received_at": time.time(),
                "id": delivered[0].get("id"),
                "leg_ids": [
                    r.get("id") for r in delivered if isinstance(r.get("id"), str)
                ],
                "direction": "out",
            }
        )
        return
    for result in delivered:
        destination = result.get("to")
        if not isinstance(destination, str) or not destination:
            continue
        node._record_sent(
            {
                "from": node.name,
                "destination": destination,
                "text": text,
                "kind": "chat",
                "sent_at": time.time(),
                "received_at": time.time(),
                "id": result.get("id"),
                "direction": "out",
            }
        )


def _forward_file_to_peers(
    args: argparse.Namespace,
    config: Any,
    staged: Path,
    name: str,
    size: int,
    digest: str,
    forwarded_from: Dict[str, Any],
    targets: List[str],
    *,
    file_timeout: float,
    discover_timeout: float,
) -> int:
    """Forward a stored file to peers: one file leg each.

    The file leg starts the first local node in the process, like
    ``tau send-file`` — there is no text leg left to route through a
    serving node.  Each leg carries the original sender in its offer.
    """
    node = TauNode(config, name=config.name)
    node.start()
    try:
        file_results = _forward_file_send_peers(
            node,
            targets,
            staged,
            file_timeout,
            discover_timeout,
            forwarded_from=forwarded_from,
        )
        _record_forwarded_file(
            node,
            name=name,
            size=size,
            forwarded_from=forwarded_from,
            results=file_results,
        )
    finally:
        node.stop()
    file_payload = {
        "name": name,
        "size": size,
        "sha256": digest,
        "results": file_results,
    }
    failed_files = [
        result for result in file_results if not result.get("delivered")
    ]
    if _json_mode(args):
        _emit({"file": file_payload}, True)
        return _EXIT_ERROR if failed_files else _EXIT_OK
    for result in file_results:
        if result.get("delivered"):
            print(
                f"forwarded {result['name']} ({result['size']} bytes) "
                f"to {result['target']}"
            )
        else:
            print(
                f"failed to forward {name} to {result['target']}: "
                f"{result.get('error') or 'unknown error'}"
            )
    return _EXIT_ERROR if failed_files else _EXIT_OK


def _forward_file_send_peers(
    node: Any,
    targets: List[str],
    staged: Path,
    file_timeout: float,
    discover_timeout: float,
    *,
    forwarded_from: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Send the staged file to each peer, reporting per-recipient results."""
    results: List[Dict[str, Any]] = []
    for target in targets:
        try:
            sent = node.send_file(
                target,
                staged,
                timeout=file_timeout,
                discover_timeout=discover_timeout,
                forwarded_from=forwarded_from,
            )
        except (NodeError, PeerError, ValueError, OSError) as exc:
            results.append({"target": target, "ok": False, "error": str(exc)})
            continue
        results.append({"target": target, "delivered": True, **sent})
    return results


def _forward_file_to_group(
    args: argparse.Namespace,
    config: Any,
    staged: Path,
    name: str,
    size: int,
    digest: str,
    forwarded_from: Dict[str, Any],
    *,
    file_timeout: float,
    discover_timeout: float,
) -> int:
    """Forward a stored file into a group: one file leg per member.

    The file leg starts the first local node in the process, like
    ``tau send-file`` — there is no text leg left to route through a
    serving node.  Each leg carries the original sender in its offer.
    """
    node = TauNode(config, name=config.name)
    node.start()
    try:
        group = node.groups.find(args.group)
        file_response = node.send_group_file(
            group,
            staged,
            timeout=file_timeout,
            discover_timeout=discover_timeout,
            forwarded_from=forwarded_from,
        )
        _record_forwarded_file(
            node,
            name=name,
            size=size,
            forwarded_from=forwarded_from,
            results=file_response.get("results") or [],
            group=group,
        )
    finally:
        node.stop()
    failed_legs = [
        result for result in file_response["results"] if not result.get("delivered")
    ]
    if _json_mode(args):
        _emit({"file": file_response}, True)
        return _EXIT_ERROR if failed_legs else _EXIT_OK
    if not file_response["results"]:
        print(
            f"group {file_response['name']} has no other members"
            " — nothing to send to."
        )
        return _EXIT_OK
    for result in file_response["results"]:
        member_name = result.get("to_name") or result["to"][:8]
        if result.get("delivered"):
            print(
                f"forwarded {result['name']} "
                f"({result['size']} bytes) to {member_name}"
            )
        else:
            print(
                f"failed to forward {name} to {member_name}: "
                f"{result.get('error') or 'unknown error'}"
            )
    return _EXIT_ERROR if failed_legs else _EXIT_OK


def _queue_file_for_later(
    config: Any,
    *,
    target: str,
    kind: str,
    path: Path,
    send_after: float,
    group_id: Optional[str] = None,
    group_members: Tuple[str, ...] = (),
    timeout: float = 300.0,
    discover_timeout: float = 10.0,
) -> ScheduledFile:
    """Stage *path* and queue it for *target* at *send_after*.

    Reads and validates the file first (same 1..10 MiB limit and safe
    name an immediate send enforces), stages the bytes under a fresh
    queue id, then records the queue entry.  Staging and the queue
    commit happen under the scheduled-files lock, so a crash or a
    backup cannot slip between them and leave orphaned staged bytes
    no queue entry owns.  A queue write that fails after staging
    removes the staged bytes again under the same lock hold, so a
    failed schedule never leaves an orphaned staged file behind — a
    removal that itself fails is logged, not hidden behind the
    original error.
    """
    try:
        data, name, digest = read_file_payload(path)
    except ScheduledFileError as exc:
        raise NodeError(str(exc)) from exc
    entry_id = new_scheduled_file_id()
    with scheduled_files_lock(config.scheduled_files_path):
        stage_file(config.scheduled_files_dir, entry_id, name, data)
        try:
            return _schedule_file_locked(
                config.scheduled_files_path,
                ScheduledFile(
                    id=entry_id,
                    target=target,
                    kind=kind,
                    file={"sha256": digest, "size": len(data), "name": name},
                    publish_at=send_after,
                    created_at=time.time(),
                    group_id=group_id,
                    group_members=group_members,
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                ),
            )
        except ScheduledFileError as exc:
            try:
                remove_staged_file(config.scheduled_files_dir, entry_id)
            except OSError as cleanup_exc:
                log.warning(
                    "Scheduled file %s could not be queued (%s) and its "
                    "staged bytes could not be removed (%s)",
                    entry_id,
                    exc,
                    cleanup_exc,
                )
            raise NodeError(str(exc)) from exc
        except Exception:
            try:
                remove_staged_file(config.scheduled_files_dir, entry_id)
            except OSError as cleanup_exc:  # pragma: no cover - best effort
                log.warning(
                    "Scheduled file %s could not be queued and its staged "
                    "bytes could not be removed (%s)",
                    entry_id,
                    cleanup_exc,
                )
            raise


def _print_scheduled_file(entry: ScheduledFile, *, delay: float) -> None:
    if entry.kind == "group":
        destination = f"group {entry.target}"
    elif entry.kind == "broadcast":
        destination = f"broadcast list {entry.target}"
    else:
        destination = entry.target
    print(
        f"scheduled {entry.file['name']} ({entry.file['size']} bytes) "
        f"for {destination} — sends in {_ago(delay)}"
    )
    print(f"  id:      {entry.id}")
    print("  a node serving will send it at its scheduled time;")
    print(
        "  list it with `tau scheduled-files`, cancel it with "
        "`tau scheduled-files cancel <id>`"
    )


def _emit_scheduled_file(entry: ScheduledFile) -> None:
    _emit(
        {
            "scheduled": True,
            "id": entry.id,
            "to": entry.target,
            "kind": entry.kind,
            "name": entry.file["name"],
            "size": entry.file["size"],
            "sha256": entry.file["sha256"],
            "send_after": entry.publish_at,
        },
        True,
    )


def cmd_send_file(args: argparse.Namespace) -> int:
    config = _make_config(args)
    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        delay = _parse_duration(delay_text, flag="--delay")
        if delay > MAX_MAIL_TTL - 3600.0:
            print(
                "error: --delay must be at most 29d23h (the 30d mail TTL "
                "less an hour of delivery slack)",
                file=sys.stderr,
            )
            return 2
        entry = _queue_file_for_later(
            config,
            target=args.to,
            kind="dm",
            path=args.path,
            send_after=time.time() + delay,
            timeout=float(getattr(args, "timeout", 300.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
        if _json_mode(args):
            _emit_scheduled_file(entry)
        else:
            _print_scheduled_file(entry, delay=delay)
        return _EXIT_OK
    node = TauNode(config, name=config.name)
    node.start()
    last_percent = -1

    def on_progress(progress: float) -> None:
        nonlocal last_percent
        percent = int(progress * 100)
        if _json_mode(args) or percent == last_percent:
            return
        last_percent = percent
        print(f"\r  sending… {percent}%", end="", flush=True)

    try:
        response = node.send_file(
            args.to,
            args.path,
            timeout=float(getattr(args, "timeout", 300.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
            progress_callback=on_progress,
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(response, True)
        return _EXIT_OK
    if last_percent >= 0:
        print()
    print(
        f"sent {response['name']} ({response['size']} bytes) to {args.to}"
    )
    print(f"  id:     {response['id']}")
    print(f"  sha256: {response['sha256']}")
    return _EXIT_OK


def cmd_scheduled_files(args: argparse.Namespace) -> int:
    config = _make_config(args)
    try:
        queue = load_scheduled_files(config.scheduled_files_path)
    except ScheduledFileError as exc:
        raise NodeError(str(exc)) from exc
    entries = queue.files
    if _json_mode(args):
        _emit({"scheduled": [entry.to_dict() for entry in entries]}, True)
        return _EXIT_OK
    if not entries:
        print("No files scheduled.")
        return _EXIT_OK
    now = time.time()
    for entry in entries:
        when = _stamp(entry.publish_at)
        if entry.publish_at <= now:
            when += " (due — sends on the next serve pass)"
        else:
            when += f" (in {_ago(entry.publish_at - now)})"
        if entry.kind == "group":
            destination = f"group {entry.target}"
        elif entry.kind == "broadcast":
            destination = f"broadcast list {entry.target}"
        else:
            destination = entry.target
        print(
            f"{entry.id}  {destination}  {entry.file['name']} "
            f"({entry.file['size']} bytes)  {when}"
        )
        if entry.attempts:
            if entry.next_attempt_at > now:
                print(
                    f"  {entry.attempts} failed attempt(s); "
                    f"next try in {_ago(entry.next_attempt_at - now)}"
                )
            else:
                print(f"  {entry.attempts} failed attempt(s)")
    return _EXIT_OK


def cmd_cancel_scheduled_file(args: argparse.Namespace) -> int:
    config = _make_config(args)

    def _delete_staged(entry: ScheduledFile) -> None:
        # Dropping only the queue entry would leave the staged bytes
        # behind; a later schedule reuses the id space, never the
        # bytes, so delete them with the entry.
        remove_staged_file(config.scheduled_files_dir, entry.id)

    try:
        entry = cancel_scheduled_file(
            config.scheduled_files_path,
            args.target,
            on_cancel=_delete_staged,
        )
    except ScheduledFileError as exc:
        raise NodeError(str(exc)) from exc
    if _json_mode(args):
        _emit({"cancelled": entry.to_dict()}, True)
    else:
        print(f"cancelled {entry.id} — {entry.file['name']} will not be sent")
    return _EXIT_OK




def cmd_outbox(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    entries = mailbox.outbox_list()
    counts = mailbox.counts()

    def describe(entry: Any) -> Dict[str, Any]:
        peer = registry.get(entry.to)
        payload = entry.to_dict()
        payload["to_name"] = peer.display_name if peer is not None else entry.to_name
        payload["held_by"] = sorted(entry.held_by)
        # Still queued, so never read — but the field keeps the shape
        # identical to the sent history, where read_at is meaningful.
        payload["read_at"] = mailbox.read_at(entry.id)
        return payload

    if _json_mode(args):
        _emit(
            {
                "outbox": [describe(entry) for entry in entries],
                "count": len(entries),
                "held_for_others": counts["held"],
                "receipts_queued": counts["receipts"],
            },
            True,
        )
    else:
        if not entries:
            print("Outbox is empty — nothing waiting on offline peers.")
        else:
            now = time.time()
            header = ("ID", "TO", "NAME", "KIND", "AGE", "TRIES", "NEXT TRY", "EXPIRES IN")
            rows: List[tuple] = [header]
            for entry in entries:
                peer = registry.get(entry.to)
                name = peer.display_name if peer is not None else entry.to_name
                next_try = _next_try_at(entry)
                if next_try is None or next_try <= now:
                    next_try_text = "now"
                else:
                    next_try_text = f"in {_ago(next_try - now)}"
                rows.append(
                    (
                        entry.id[:8],
                        entry.to_hex,
                        name or "-",
                        getattr(entry, "kind", None) or DEFAULT_MESSAGE_KIND,
                        _ago(now - entry.created_at),
                        str(entry.attempts),
                        next_try_text,
                        _ago(entry.expires_at - now),
                    )
                )
            widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
            for row in rows:
                print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
            print(f"\n{len(entries)} message(s) waiting for offline peers.")
        if counts["held"]:
            print(
                f"Also holding {counts['held']} sealed message(s) for other peers"
                " — see `tau outbox held`."
            )
        if counts["receipts"]:
            print(
                f"Also waiting to send {counts['receipts']} read receipt(s)"
                " to offline senders."
            )
    return _EXIT_OK


def _is_hex32(value: str) -> bool:
    return len(value) == 32 and all(char in "0123456789abcdefABCDEF" for char in value)


def _format_message_time(timestamp: float) -> str:
    """One inbox entry's received time; never fails the whole listing."""
    try:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
    except (OverflowError, OSError, ValueError):
        return "unknown time"


def _format_optional_time(timestamp: Any) -> str:
    """A stored timestamp for the show view, or ``"unknown"`` when absent.

    A zero send time means the sender's clock was unusable (the inbox
    normalizes a non-finite ``sent_at`` to 0.0 instead of dropping the
    record), so it reads as unknown rather than 1970-01-01 — the
    epoch is never a real send time here.
    """
    try:
        value = float(timestamp)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return "unknown"
    if value <= 0:
        return "unknown"
    return _format_message_time(value)


def _resolve_inbox_peer(
    inbox: Inbox,
    registry: PeerRegistry,
    target: str,
    direction: Optional[str] = "in",
    *,
    extra_hashes: Optional[Iterable[str]] = None,
) -> str:
    """Resolve ``--peer`` to a sender hash for inbox filtering.

    A full destination hash filters directly, and an exact name match
    wins over any prefix.  Aliases resolve before names, matching
    ``PeerRegistry.resolve`` and the message-send path: when one peer's
    name equals another peer's alias, ``--peer`` must pick the same
    peer a send would.  For a hash prefix, registered peers and
    senders stored in the inbox are combined before selecting: a
    prefix shared by two distinct senders is ambiguous, even when one
    of them is registered and the other is not (pruned, or never
    discovered — a direct message needs no prior announce).
    ``direction`` picks which inbox records are searched: ``"in"`` for
    received messages (the default), ``None`` for both directions.
    ``extra_hashes`` adds further hash candidates to the prefix
    search — ``unsave`` passes the starred peer scopes, so a star
    survives its peer disappearing from the registry and the inbox.
    """
    target = target.strip()
    if _is_hex32(target):
        return target.lower()
    peers = registry.all()
    by_alias = [
        peer
        for peer in peers
        if peer.alias and peer.alias.casefold() == target.casefold()
    ]
    if len(by_alias) == 1:
        return by_alias[0].hexhash
    if len(by_alias) > 1:
        raise NodeError(
            f"{len(by_alias)} peers share the alias {target!r}; "
            "use a destination hash"
        )
    by_name = [peer for peer in peers if peer.name.casefold() == target.casefold()]
    if len(by_name) == 1:
        return by_name[0].hexhash
    if len(by_name) > 1:
        raise NodeError(
            f"{len(by_name)} peers are named {target!r}; use a destination hash"
        )
    if len(target) >= 4:
        prefix = target.casefold()
        senders = {peer.hexhash for peer in peers if peer.hexhash.startswith(prefix)}
        senders |= {
            str(entry.get("destination") or "").casefold()
            for entry in inbox.list_messages(limit=None, direction=direction)
            if str(entry.get("destination") or "").casefold().startswith(prefix)
        }
        if extra_hashes is not None:
            senders |= {
                str(candidate).casefold()
                for candidate in extra_hashes
                if str(candidate).casefold().startswith(prefix)
            }
        if len(senders) == 1:
            return senders.pop()
        if len(senders) > 1:
            raise NodeError(f"{len(senders)} senders match the hash prefix {target!r}")
    raise NodeError(f"unknown peer: {target!r}")


def _send_displayed_receipts(
    config: TauConfig, inbox: Inbox, entries: List[Dict[str, Any]]
) -> Dict[str, int]:
    """Receipt the displayed messages, directly or via the serving node.

    Listing a DM is reading it, so displayed messages are stamped
    ``read_at`` in the inbox first — that stamp is the durable
    handoff: when a serving node already holds this data directory's
    TCP listen port, a second Reticulum instance cannot start here
    (Reticulum exits the process on the bind failure), so the serving
    node's mail sweep delivers those receipts instead.  When no node
    is serving, this command sends them directly over a one-shot node.
    Only messages never receipted before are covered, so listing the
    same inbox twice does not re-send.  Best-effort: a receipt failure
    never breaks the listing.  Returns how many peers got a live
    receipt and how many had one queued for store-and-forward.
    """
    unreceipted = [
        entry
        for entry in entries
        if isinstance(entry.get("id"), str)
        and entry.get("id")
        and entry.get("receipt_sent_at") is None
        # Notes are local-only: no peer is owed a receipt for one, and
        # receipting our own destination would make the node try to
        # discover itself.
        and entry.get("kind") != NOTE_KIND
    ]
    if not unreceipted:
        return {"sent": 0, "queued": 0}
    if not send_read_receipts_enabled(config.data_dir):
        return {"sent": 0, "queued": 0}
    try:
        inbox.mark_read([str(entry["id"]) for entry in unreceipted])
    except Exception:  # pragma: no cover - defensive
        pass
    if not config.identity_path.exists():
        # No identity yet: starting a node here would create one as a
        # side effect of merely listing messages.  The read stamp above
        # is the handoff for when a node next serves this directory.
        return {"sent": 0, "queued": 0}
    try:
        check_tcp_listen_ports(config)
    except NodeError:
        # A serving node already holds the listen port; its mail sweep
        # delivers the receipts from the read stamps above.
        return {"sent": 0, "queued": 0}
    by_peer: Dict[str, List[str]] = {}
    for entry in unreceipted:
        peer_hash = str(entry.get("destination") or "")
        if peer_hash:
            by_peer.setdefault(peer_hash, []).append(str(entry["id"]))
    if not by_peer:
        return {"sent": 0, "queued": 0}
    # No mail loop or announce: this node only sends the explicit receipts
    # below, so a background sweep cannot send the same ids concurrently.
    node = TauNode(config, name=config.name)
    try:
        node.start(mail_loop=False, announce=False)
    except Exception:
        return {"sent": 0, "queued": 0}
    sent = queued = 0
    stamped: List[str] = []
    try:
        for peer_hash, ids in by_peer.items():
            try:
                result = node.send_read_receipts(peer_hash, ids)
            except Exception:
                continue
            handled = result.get("ids") or []
            if not handled:
                continue
            if result.get("queued"):
                queued += 1
            elif result.get("ok"):
                sent += 1
            stamped.extend(handled)
    finally:
        node.stop()
    if stamped:
        try:
            inbox.mark_receipt_sent(stamped)
        except Exception:  # pragma: no cover - defensive
            pass
    return {"sent": sent, "queued": queued}


def _resolve_sent_peer(registry: PeerRegistry, target: str) -> str:
    """Resolve ``--peer`` to a recipient hash for the sent view.

    A full destination hash filters directly even when the peer has
    been pruned from the registry; names and prefixes resolve through
    it.
    """
    target = target.strip()
    if _is_hex32(target):
        return target.lower()
    try:
        peer = registry.resolve(target)
    except PeerError as exc:
        raise NodeError(str(exc)) from exc
    if peer is None:
        raise NodeError(f"unknown peer: {target!r}")
    return peer.hexhash
def _resolve_reply_target(
    inbox: Inbox, mailbox: Mailbox, ref: str, peer_hash: str
) -> Dict[str, Any]:
    """Resolve ``--reply-to`` to an ``in_reply_to`` wire reference.

    The quoted message must be in the same conversation: the inbox is
    searched first (received messages and the node's own sent ones),
    then the outbox for a still-queued message.  A reference to
    another peer's message, or to a message this node has never seen,
    is refused instead of sent dangling — the receiver could only
    render it as "original not available" anyway.
    """
    ref = ref.strip()
    if not ref:
        raise NodeError("--reply-to must not be empty")
    # Gather candidates from both stores before choosing, so a prefix that
    # matches distinct messages across the inbox and the outbox is rejected
    # rather than silently quoting whichever store is searched first.
    candidates: Dict[str, Dict[str, Any]] = {}
    for item in inbox.find_prefix(ref, peer=peer_hash):
        candidates.setdefault(str(item["id"]), item)
    value = ref.lower()
    for entry in mailbox.outbox_list(to=bytes.fromhex(peer_hash)):
        # A queued group leg to this peer is not a DM: its id belongs
        # to the group send, so it must not resolve as a direct reply.
        if entry.group_id is not None:
            continue
        # A queued edit or reaction is not a message: its id names
        # an edit or a reaction, so it must not resolve as a reply
        # target either.
        if getattr(entry, "kind", None) in ("edit", "reaction"):
            continue
        if entry.id.lower().startswith(value):
            candidates.setdefault(
                entry.id, {"id": entry.id, "text": entry.text}
            )
    if len(candidates) > 1:
        raise NodeError(
            f"{len(candidates)} messages in this conversation match {ref!r};"
            " use a longer prefix"
        )
    if not candidates:
        raise NodeError(
            f"no message {ref!r} in this conversation to reply to;"
            " replies quote a message from this peer's history"
        )
    quoted = next(iter(candidates.values()))
    return {"id": str(quoted["id"]), "excerpt": excerpt_hash(str(quoted["text"]))}


def _resolve_group_reply_target(
    inbox: Inbox, mailbox: Mailbox, ref: str, group_id: str
) -> Dict[str, Any]:
    """Resolve ``tau group send --reply-to`` to an ``in_reply_to`` wire reference.

    Same rules as :func:`_resolve_reply_target`, scoped to the group
    instead of a peer: the quoted message must be in this group's
    history — a received message, one of this node's own sent legs,
    or a still-queued leg — and a prefix matching more than one is
    refused.
    """
    ref = ref.strip()
    if not ref:
        raise NodeError("--reply-to must not be empty")
    candidates: Dict[str, Dict[str, Any]] = {}
    # A group send is one inbox record with every leg id, and a queued
    # leg is also an outbox entry: the same leg seen in both stores is
    # one candidate, not two — otherwise quoting a queued leg's id is
    # always refused as ambiguous.  A candidate is keyed by the leg id
    # that matched the prefix, so the reference names the leg quoted;
    # two legs of one send matching the same prefix still resolve to
    # that one send.
    covered_ids: Set[str] = set()
    value = ref.lower()
    for item in inbox.find_prefix(ref, group=group_id):
        matched: Optional[str] = None
        if str(item["id"]).lower().startswith(value):
            matched = str(item["id"])
        else:
            for leg in item.get("leg_ids") or []:
                if isinstance(leg, str) and leg.lower().startswith(value):
                    matched = leg
                    break
        if matched is None:
            continue
        candidates.setdefault(
            matched, {"id": matched, "text": str(item["text"])}
        )
        covered_ids.add(str(item["id"]))
        for leg in item.get("leg_ids") or []:
            if isinstance(leg, str):
                covered_ids.add(leg)
    for entry in mailbox.outbox_list():
        if entry.group_id != group_id:
            continue
        if entry.id in covered_ids:
            continue
        # A queued edit or reaction is not a message: its id names
        # an edit or a reaction, so it must not resolve as a reply
        # target.
        if getattr(entry, "kind", None) in ("edit", "reaction"):
            continue
        if entry.id.lower().startswith(value):
            candidates.setdefault(
                entry.id, {"id": entry.id, "text": entry.text}
            )
    if len(candidates) > 1:
        raise NodeError(
            f"{len(candidates)} messages in this group match {ref!r};"
            " use a longer prefix"
        )
    if not candidates:
        raise NodeError(
            f"no message {ref!r} in this group to reply to;"
            " replies quote a message from this group's history"
        )
    quoted = next(iter(candidates.values()))
    return {"id": str(quoted["id"]), "excerpt": excerpt_hash(str(quoted["text"]))}


class AmbiguousMessageRefError(NodeError):
    """A message reference matches more than one stored message.

    Distinct from a plain "not found" :class:`NodeError` so callers
    that fall back to a starred entry on a missing message — ``unsave``
    — can refuse an ambiguous reference instead of silently picking
    the one star that happens to match it.
    """


def _resolve_group_pin_target(
    inbox: Inbox, group_ids: Sequence[str], ref: str
) -> Dict[str, Any]:
    """Resolve ``tau group pin`` / ``unpin`` to one stored group message.

    Same scoping as :func:`_resolve_group_reply_target` but inbox-only:
    a pin attaches to a stored record, so a still-queued leg has nothing
    to pin and is never a candidate.  ``group_ids`` spans renames, so a
    message stored under a predecessor id still resolves.  An exact id
    wins over a longer id that merely starts with it, as in
    :meth:`Inbox.pin` (via :func:`_match_ref`).  A prefix matching more
    than one record, or nothing at all, is refused.

    A sent group message pins like any sent record — ``tau pin`` pins
    sent DMs regardless of delivery — so there is deliberately no
    refusal for sends whose legs are still queued: the stored record
    is what the pin attaches to, not the delivery state.
    """
    ref = ref.strip()
    if not ref:
        raise NodeError("message must not be empty")
    matches: List[Dict[str, Any]] = []
    for group_id in group_ids:
        matches.extend(inbox.find_prefix(ref, group=group_id))
    exact = [m for m in matches if str(m["id"]) == ref]
    if not exact:
        folded = ref.lower()
        exact = [m for m in matches if str(m["id"]).lower() == folded]
    if exact:
        matches = exact
    if len(matches) > 1:
        raise AmbiguousMessageRefError(
            f"{len(matches)} messages in this group match {ref!r};"
            " use a longer prefix"
        )
    if not matches:
        raise NodeError(
            f"no message {ref!r} in this group to pin;"
            " pins attach to a stored message from this group's history"
        )
    return matches[0]


def _resolve_group_poll(
    inbox: Inbox, ref: str, group_id: str
) -> Dict[str, Any]:
    """Resolve ``tau group vote`` / ``close-poll`` to one stored group poll.

    Same scoping as :func:`_resolve_group_pin_target` but restricted
    to polls: the reference (a message id or unique leg prefix, as
    shown by ``tau messages --group``) must match exactly one stored
    poll record in this group.  A vote or close record never resolves
    — only the poll itself can be voted on or closed.
    """
    ref = ref.strip()
    if not ref:
        raise NodeError("poll must not be empty")
    matches = [
        item
        for item in inbox.find_prefix(ref, group=group_id)
        if is_group_poll(item)
    ]
    exact = [m for m in matches if str(m["id"]) == ref]
    if not exact:
        folded = ref.lower()
        exact = [m for m in matches if str(m["id"]).lower() == folded]
    if exact:
        matches = exact
    if len(matches) > 1:
        raise NodeError(
            f"{len(matches)} polls in this group match {ref!r};"
            " use a longer prefix"
        )
    if not matches:
        raise NodeError(
            f"no poll {ref!r} in this group;"
            " votes name a poll from this group's history"
        )
    return matches[0]


def _attach_group_poll_tallies(
    inbox: Inbox,
    entries: List[Dict[str, Any]],
    own_hash: Optional[str],
) -> None:
    """Attach a ``poll`` tally to each group-poll record in *entries*.

    The tally derives from the group's full stored history — votes a
    filter left out of *entries* (read votes under ``--unread``, say)
    still count — so the history is re-read per group involved.
    Entries are the copies :meth:`Inbox.list_messages` returns, so
    attaching here never touches the stored records.
    """
    group_ids = {
        str(entry.get("group"))
        for entry in entries
        if entry.get("kind") == "group" and entry.get("group")
    }
    if not group_ids:
        return
    history = inbox.list_messages(
        limit=None, group=sorted(group_ids), direction=None
    )
    attach_group_poll_tallies(history, own_hash=own_hash)
    by_id = {
        (str(item.get("group")), str(item.get("id"))): item for item in history
    }
    for entry in entries:
        if not is_group_poll(entry):
            continue
        match = by_id.get(
            (str(entry.get("group")), str(entry.get("id")))
        )
        if match is not None and "poll" in match:
            entry["poll"] = match["poll"]


def _attach_channel_poll_tallies(
    app: ChannelApp,
    entries: List[Dict[str, Any]],
    own_hash: Optional[str],
    *,
    blocked: Optional[AbstractSet[str]] = None,
) -> None:
    """Attach a ``poll`` tally to each channel-poll record in *entries*.

    The tally derives from the channel's visible history — votes a
    displayed page left out still count, but a vote history hides
    (expired or retracted) never does — so the history is re-read
    for the channel involved.  Blocked authors are left out of the
    tally history, so their votes never count.  When the list cannot
    be read (*blocked* is None), only this node's records count, so
    a hidden sender's vote never leaks into a retained own poll's
    tally.  Entries are the copies :meth:`ChannelApp.history` returns,
    so attaching here never touches the stored records.
    """
    channels = {
        str(entry.get("channel"))
        for entry in entries
        if entry.get("channel")
    }
    if not channels:
        return
    for channel in sorted(channels):
        history = app.visible_history(channel)
        if blocked is None:
            # The list cannot be read: tallies count only this
            # node's records, so a hidden sender's vote never
            # affects the counts or leaks voter metadata — the
            # same fail-closed rule the retained hits follow.
            history = [
                record
                for record in history
                if str(record.get("author", "")) == (own_hash or "")
            ]
        elif blocked:
            history = [
                record
                for record in history
                if str(record.get("author", "")) not in blocked
            ]
        attach_channel_poll_tallies(history, own_hash=own_hash)
        by_hash = {
            str(item.get("hash")): item for item in history
        }
        for entry in entries:
            if not is_channel_poll(entry):
                continue
            match = by_hash.get(str(entry.get("hash")))
            if match is not None and "poll" in match:
                entry["poll"] = match["poll"]


def _attach_dm_poll_tallies(
    inbox: Inbox,
    entries: List[Dict[str, Any]],
    own_hash: Optional[str],
) -> None:
    """Attach a ``poll`` tally to each DM-poll record in *entries*.

    The tally derives from the conversation's full stored history —
    votes a filter left out of *entries* (read votes under
    ``--unread``, say) still count — so the history is re-read per
    peer involved.  Entries are the copies
    :meth:`Inbox.list_messages` returns, so attaching here never
    touches the stored records.
    """
    peer_hashes = {
        str(entry.get("destination"))
        for entry in entries
        if entry.get("kind") != "group" and entry.get("destination")
    }
    if not peer_hashes:
        return
    history: List[Dict[str, Any]] = []
    for peer_hash in sorted(peer_hashes):
        history.extend(
            inbox.list_messages(limit=None, peer=peer_hash, direction=None)
        )
    attach_dm_poll_tallies(history, own_hash=own_hash)
    by_id = {
        (str(item.get("destination")), str(item.get("id"))): item
        for item in history
    }
    for entry in entries:
        if not is_dm_poll(entry):
            continue
        match = by_id.get(
            (str(entry.get("destination")), str(entry.get("id")))
        )
        if match is not None and "poll" in match:
            entry["poll"] = match["poll"]


def _own_destination_hash(config: TauConfig) -> Optional[str]:
    """This node's destination hash, or ``None`` without an identity."""
    identity = load_identity(config.identity_path)
    return destination_hash(identity).hex() if identity is not None else None


def _is_group_creator(config: TauConfig, target: Dict[str, Any]) -> bool:
    """True when this node created the group *target* belongs to.

    The creator is read from the pinned roster for the message's
    group — the same record the receive path verifies a moderator
    retract against — so a stale local copy cannot grant moderation
    this node does not have.
    """
    group_id = target.get("group") or ""
    if not group_id:
        return False
    own_hash = _own_destination_hash(config)
    if own_hash is None:
        return False
    pinned = pinned_group(GroupStore(config.groups_path), str(group_id))
    return pinned is not None and (pinned.get("creator") or "") == own_hash


def _quote_line(
    inbox: Inbox,
    registry: PeerRegistry,
    entry: Dict[str, Any],
    *,
    blocked: Optional[Any],
    own_hash: Optional[str],
) -> Optional[str]:
    """Render one message's quote, or ``None`` when it has none.

    The quoted text is pulled from the local inbox, in the same
    conversation only; when the original is not there — or its text
    no longer matches the excerpt hash the sender quoted — the line
    says so instead of trusting anything on the wire.  A group quote
    is scoped to the group, not the replier: the quoted message may
    be from any member.  Group legs also carry per-member ids, so a
    member may hold the quoted text under a different leg id — when
    the id is not held, the excerpt hash matches the same text
    within the group instead.
    """
    reply_to = entry.get("in_reply_to")
    if not isinstance(reply_to, dict):
        return None
    ref_id = reply_to.get("id")
    if not isinstance(ref_id, str) or not ref_id:
        return None
    group_id = entry.get("group")
    excerpt = reply_to.get("excerpt")
    if isinstance(group_id, str) and group_id:
        quoted = None
        # A reply that carries the logical reference names the quoted
        # send exactly, even when this node holds a different leg id
        # for it — or the same text under another author, which the
        # excerpt fallback below cannot attribute.  The reference is
        # only trusted inside its own group: a group that does not
        # match this message's is a sender error, not a quote.
        if (
            reply_to.get("group") == group_id
            and isinstance(reply_to.get("author"), str)
            and isinstance(reply_to.get("sent_at"), (int, float))
            and not isinstance(reply_to.get("sent_at"), bool)
        ):
            quoted = inbox.find_group_message(
                group_id, reply_to["author"], reply_to["sent_at"]
            )
            # This node's own group sends are stored as outbound records
            # with no member destination, so the inbound lookup above
            # never sees them: a reference naming this node as author
            # resolves against the outbound record instead.
            if (
                quoted is None
                and own_hash is not None
                and reply_to["author"] == own_hash
            ):
                quoted = inbox.find_sent_group_message(
                    group_id, reply_to["sent_at"]
                )
        if quoted is None:
            quoted = inbox.find(ref_id, group=group_id)
        if quoted is None and isinstance(excerpt, str) and excerpt:
            quoted = inbox.find_by_excerpt(excerpt, group=group_id)
    else:
        quoted = inbox.find(ref_id, peer=str(entry.get("destination") or ""))
    # A quote never shows a received message from a blocked author, even
    # when it resolved through the logical reference or the excerpt.
    if quoted is not None and quoted.get("direction") != "out" and (
        blocked is None or str(quoted.get("destination") or "") in blocked
    ):
        quoted = None
    if quoted is None or (
        isinstance(excerpt, str)
        and excerpt
        and excerpt_hash(str(quoted.get("text") or "")) != excerpt.lower()
    ):
        return "↩ original not available"
    if quoted.get("direction") == "out":
        name = "you"
    else:
        peer = registry.get(str(quoted.get("destination") or ""))
        name = peer.display_name if peer is not None else quoted.get("from") or "unknown"
    quoted_text = str(quoted.get("text") or "")
    first_line = quoted_text.splitlines()[0][:80] if quoted_text else ""
    return f"↩ {name}: {first_line}"


def _message_peer_label(registry: PeerRegistry, entry: Dict[str, Any]) -> str:
    """Who a stored message is from — or, for one this node sent, to."""
    destination = str(entry.get("destination") or "")
    peer = registry.get(destination)
    if entry.get("direction") == "out":
        name = peer.display_name if peer is not None else "unknown"
        return f"to {name}"
    name = peer.display_name if peer is not None else entry.get("from") or "unknown"
    return f"from {name}"


def _print_entry_reactions(registry: PeerRegistry, entry: Dict[str, Any]) -> None:
    """Print one line per reaction on a stored message, reactor named."""
    for reaction in entry.get("reactions") or []:
        reactor = registry.get(str(reaction.get("destination") or ""))
        reactor_name = (
            reactor.display_name
            if reactor is not None
            else str(reaction.get("from") or "unknown")
        )
        print(f"  {reaction.get('reaction')} {reactor_name}")


def _print_message_entry(
    inbox: Inbox,
    registry: PeerRegistry,
    entry: Dict[str, Any],
    *,
    blocked: Optional[Any],
    own_hash: Optional[str],
    full_id: bool = False,
) -> None:
    """Print one stored message the way `tau messages` does.

    Shared by `tau messages` and `tau pinned` so a pinned message
    reads the same in both listings.  A message this node sent is
    shown as from you, to its recipient; a received one as from its
    sender.  Pinned entries carry a 📌 marker, like the ● unread one.
    With *full_id* the header names the whole id instead of the
    8-character prefix the listing uses — the show view, where the
    id is the thing being inspected.
    """
    destination = str(entry.get("destination") or "")
    peer = registry.get(destination)
    sender = short_hash(destination)
    if entry.get("direction") == "out" and entry.get("kind") == "group":
        # A group send has no single destination: name the group instead.
        who = f"you → {entry.get('group_name') or 'group'}"
    elif entry.get("direction") == "out":
        name = peer.display_name if peer is not None else "unknown"
        who = f"you → {name} ({sender})"
    else:
        name = peer.display_name if peer is not None else entry.get("from") or "unknown"
        who = f"{name} ({sender})"
    when = _format_message_time(float(entry.get("received_at", 0)))
    # Unread is a received-message state; a sent message is never unread.
    marker = "● " if entry.get("unread") and entry.get("direction") != "out" else ""
    pin_marker = "📌 " if entry.get("pinned") else ""
    # The short id is what `tau react` takes: without it the
    # listing gives the user no way to name a message to react to.
    # The show view names the whole id instead — the id is the thing
    # being inspected there, and a prefix is ambiguous to copy.
    message_id = entry.get("id")
    if isinstance(message_id, str) and message_id:
        shown_id = message_id if full_id else _safe_id_prefix(message_id)
        id_suffix = f" #{shown_id}"
    else:
        id_suffix = ""
    group_suffix = ""
    if entry.get("kind") == "group":
        group_suffix = f" in {entry.get('group_name') or 'group'}"
    entry_kind = entry.get("kind") or DEFAULT_MESSAGE_KIND
    kind_tag = (
        f" [{entry_kind}]"
        if entry_kind not in (DEFAULT_MESSAGE_KIND, "group", "file")
        else ""
    )
    edited_suffix = " (edited)" if entry.get("edited_at") else ""
    print(f"{marker}{pin_marker}[{when}] {who}{id_suffix}{kind_tag}{group_suffix}{edited_suffix}")
    if entry.get("kind") == "file":
        forwarded_from = clean_forwarded_from_or_none(
            entry.get("forwarded_from")
        )
        if forwarded_from is not None:
            print(f"  forwarded from {forwarded_from['name']}")
        sha = str(entry.get("sha256") or "")
        # The show view names the whole stored checksum for an integrity
        # comparison; a listing keeps the abbreviated one.
        shown_sha = sha if full_id else short_hash(sha, 12)
        print(
            f"  file: {entry.get('name')} "
            f"({entry.get('size')} bytes, "
            f"sha256 {shown_sha})"
        )
        print(f"  saved to {entry.get('path')}")
        _print_entry_reactions(registry, entry)
        return
    quote = _quote_line(inbox, registry, entry, blocked=blocked, own_hash=own_hash)
    if quote is not None:
        print(f"  {quote}")
    for line in str(entry.get("text", "")).splitlines() or [""]:
        print(f"  {line}")
    location = clean_location(entry.get("location"))
    if location is not None:
        # A static pin prints under the text with its map link, so a
        # location message reads in the terminal the way it renders
        # in the console; a hand-edited record with a bad pin just
        # shows its text.
        name = location.get("name")
        label = name if isinstance(name, str) and name else "Location"
        print(f"  📍 {label} — {location['lat']}, {location['lon']}")
        print(f"  {location_maps_url(location)}")
        beacon = clean_beacon(entry.get("beacon"))
        if beacon is not None:
            # A live update names its sequence and expiry, so the
            # reader can tell it from a one-off pin and see how long
            # the beacon has left; an expired beacon still reads as
            # the pin it was.
            remaining = beacon["expires_at"] - time.time()
            if remaining > 0:
                print(
                    f"  live beacon · update {beacon['seq']} · "
                    f"expires in {_ago(remaining)}"
                )
            else:
                print(f"  live beacon · update {beacon['seq']} · expired")
    poll = entry.get("poll")
    if isinstance(poll, dict) and poll.get("options"):
        # A group or DM poll renders its tally under the question, like a
        # Tan poll: one line per option with its vote count, plus the
        # total and the closed state.  Votes and closes themselves are
        # never printed as messages (see cmd_messages).
        options = poll["options"]
        counts = poll.get("counts") or [0] * len(options)
        for position, option in enumerate(options):
            count = counts[position] if position < len(counts) else 0
            mine = " ← your vote" if poll.get("my_vote") == position else ""
            print(f"  🗳️ {position + 1}. {option} — {count} vote(s){mine}")
        total = poll.get("total", 0)
        closed = " (closed)" if poll.get("closed") else ""
        print(f"  {total} voter(s) total{closed}")
    _print_entry_reactions(registry, entry)
    entry_expires_at = entry.get("expires_at")
    if isinstance(entry_expires_at, (int, float)) and float(entry_expires_at) > time.time():
        print(f"  disappears in {_ago(float(entry_expires_at) - time.time())}")


def cmd_messages(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    # A corrupt mutes file fails open here: the conversations count
    # as unmuted (with a warning), never breaking the listing.
    muted_peers = muted_peers_for(config.muted_path)
    muted_groups = expand_muted_groups(
        muted_groups_for(config.muted_path),
        GroupStore(config.groups_path).list_groups(),
    )
    # Quiet hours silence every conversation at once, like a mute on
    # the whole node: the count and listing paths below read it
    # through `quiet`, so the badge and the markers agree.
    quiet = quiet_hours_active(config.quiet_hours_path)
    peer_hash = None
    if getattr(args, "peer", None):
        peer_hash = _resolve_inbox_peer(inbox, registry, args.peer)
    group_id = None
    group_ids = None
    if getattr(args, "group", None):
        store = GroupStore(config.groups_path)
        group = store.find(args.group)
        group_id = group["id"]
        # History spans renames: messages stored under a predecessor
        # id still belong to this group.  The store only keeps
        # predecessors whose record shares this group's creator.
        group_ids = store.lineage(group["id"])
    if getattr(args, "count", False):
        # A count is for scripting: it never advances the read cursor.
        # Muted conversations never count, so the badge and the
        # scripting count agree.
        counts = inbox.unread_counts(
            exclude_dm_from=muted_peers,
            exclude_group=muted_groups,
            quiet=quiet,
        )
        per_peer = counts["per_peer"]
        per_group = counts["per_group"]
        if peer_hash is not None and group_ids is not None:
            # Both filters list the group posts this sender made in the
            # group, so count that intersection as the listing shows
            # it: blocked senders' posts are hidden from the listing,
            # so they never count.  An unreadable block-list hides
            # everything, exactly as the listing does.
            blocked = load_blocked_set(config.blocklist_path)
            if blocked is None:
                here = 0
            else:
                here = sum(
                    1
                    for entry in inbox.list_messages(
                        peer=peer_hash, group=group_ids, quiet=quiet
                    )
                    if entry.get("unread")
                    and entry.get("destination") not in blocked
                )
            per_peer = {}
            per_group = {group_id: here} if here else {}
        elif peer_hash is not None:
            # A peer filter is a DM filter: group posts are not that
            # conversation's mail.
            per_peer = {
                destination: count
                for destination, count in per_peer.items()
                if destination == peer_hash
            }
            per_group = {}
        elif group_ids is not None:
            # A renamed group's history spans its lineage: fold the
            # predecessors' counts into the current id's line.
            here = sum(per_group.get(gid, 0) for gid in group_ids)
            per_group = {group_id: here} if here else {}
            per_peer = {}
        total = sum(per_peer.values()) + sum(per_group.values())
        if _json_mode(args):
            _emit(
                {"unread": per_peer, "unread_groups": per_group, "total": total},
                True,
            )
            return _EXIT_OK
        if not per_peer and not per_group:
            print("No unread messages.")
            return _EXIT_OK
        for destination, count in sorted(per_peer.items()):
            peer = registry.get(destination)
            name = peer.display_name if peer is not None else "unknown"
            print(f"{name} ({short_hash(destination)}): {count} unread")
        groups = GroupStore(config.groups_path)
        for group, count in sorted(per_group.items()):
            # A group can be gone while its posts stay stored: fall
            # back to a bare label instead of failing the count.
            try:
                found = groups.find(group)
            except ValueError:
                found = None
            name = found["name"] if found else "group"
            print(f"{name} ({short_hash(group)}): {count} unread")
        print(f"Total: {total} unread")
        return _EXIT_OK
    limit = getattr(args, "limit", 30)
    if limit is not None and limit < 0:
        raise NodeError("--limit must not be negative")
    kind = getattr(args, "kind", None)
    if kind is not None:
        # ``note`` is a reserved record kind, not a wire kind, so the
        # normalizer would refuse it — but it is exactly the filter a
        # user needs to list their own notes.
        if isinstance(kind, str) and kind.strip().lower() == NOTE_KIND:
            kind = NOTE_KIND
        else:
            try:
                kind = normalize_message_kind(kind)
            except ProtocolError as exc:
                raise NodeError(str(exc)) from exc
    query = getattr(args, "search", None)
    if query is not None and not query.strip():
        raise NodeError("--search must not be empty")
    archived = load_archived_set(config.archived_path)
    show_archived = getattr(args, "archived", False)
    if (
        peer_hash is not None
        and not show_archived
        and query is None
        and group_id is None
        and not getattr(args, "sent", False)
        and peer_hash in archived
    ):
        # An archived peer's thread is hidden from the default
        # listing; say so instead of showing an empty list.
        peer = registry.get(peer_hash)
        name = peer.display_name if peer is not None else "unknown"
        unread = inbox.unread_counts(
            exclude_dm_from=muted_peers,
            exclude_group=muted_groups,
            quiet=quiet,
        )["per_peer"].get(peer_hash, 0)
        if _json_mode(args):
            _emit(
                {
                    "messages": [],
                    "count": 0,
                    "more_unread": {},
                    "archived_unread": unread,
                    "archived": True,
                },
                True,
            )
            return _EXIT_OK
        print(
            f"Conversation with {name} ({short_hash(peer_hash)}) is archived — "
            f"see it with `tau messages --archived --peer {args.peer}` "
            f"or restore it with `tau unarchive {args.peer}`."
        )
        return _EXIT_OK
    if getattr(args, "sent", False):
        return _cmd_messages_sent(args, config, registry, limit)
    # Blocked senders' history stays stored but is hidden locally;
    # unblocking restores the view.  Filter before applying the limit so
    # hidden messages do not use up the requested count.  A block-list
    # that cannot be read hides everything, matching the node, which
    # blocks all senders on the same condition.
    entries = inbox.list_messages(
        limit=None,
        peer=peer_hash,
        group=group_ids,
        query=query,
        kind=kind,
        exclude_dm_from=muted_peers,
        exclude_group=muted_groups,
        quiet=quiet,
    )
    # A stored reaction from a non-member must never be displayed on
    # a group message, even when the authorization recheck or its
    # cleanup failed and left it in the inbox: filter the listed
    # reactions against each group's pinned roster before anything
    # prints or is emitted as JSON.  With no pinned roster there is
    # nothing to check against.
    rosters = pinned_rosters(
        GroupStore(config.groups_path),
        {str(entry.get("group")) for entry in entries if entry.get("group")},
    )
    for entry in entries:
        group = entry.get("group")
        if group and entry.get("reactions"):
            entry["reactions"] = visible_reactions(
                entry["reactions"], rosters.get(str(group))
            )
    blocked = load_blocked_set(config.blocklist_path)
    if blocked is None:
        entries = []
    elif blocked:
        entries = [
            entry for entry in entries if entry.get("destination") not in blocked
        ]
    # Ignoring a group is a display preference only: its posts stay
    # stored and keep arriving, and an explicit view (--group, --search,
    # --ignored) still shows them.  Archive membership is likewise a
    # direct-message concern only: a group post from an archived sender
    # is still that group's mail, and groups keep their current listing.
    # --archived is an explicit archived-only view, so it applies even
    # to searches and group listings.  The default hiding (and its unread
    # count) stays limited to plain listings, where a search is an
    # explicit lookup that still finds ignored-group or archived mail.
    ignored = load_ignored_group_ids(config.ignored_groups_path)
    show_ignored = getattr(args, "ignored", False)
    ignored_unread = 0
    archived_unread = 0
    if show_ignored and show_archived:
        entries = [
            entry
            for entry in entries
            if (entry.get("kind") == "group"
                and str(entry.get("group") or "") in ignored)
            or (entry.get("kind") != "group"
                and entry.get("destination") in archived)
        ]
    elif show_ignored:
        entries = [
            entry
            for entry in entries
            if entry.get("kind") == "group"
            and str(entry.get("group") or "") in ignored
        ]
    elif show_archived:
        entries = [
            entry
            for entry in entries
            if entry.get("kind") != "group"
            and entry.get("destination") in archived
        ]
    elif query is None and group_id is None:
        if ignored:
            hidden = [
                entry
                for entry in entries
                if entry.get("kind") == "group"
                and str(entry.get("group") or "") in ignored
            ]
            ignored_unread = sum(1 for entry in hidden if entry.get("unread"))
        if archived:
            hidden = [
                entry
                for entry in entries
                if entry.get("kind") != "group"
                and entry.get("destination") in archived
            ]
            archived_unread = sum(1 for entry in hidden if entry.get("unread"))
        entries = [
            entry
            for entry in entries
            if (entry.get("kind") != "group"
                or str(entry.get("group") or "") not in ignored)
            and (entry.get("kind") == "group"
                or entry.get("destination") not in archived)
        ]
    # Filter before limiting, so --unread finds older unread mail even
    # when newer messages from the same listing are already read.
    unread_only = getattr(args, "unread", False)
    if unread_only:
        entries = [entry for entry in entries if entry.get("unread")]
    # Group polls carry their tally on the poll record itself, derived
    # from the group's full history (see _attach_group_poll_tallies).
    # Votes and closes are not chat: they stay in the listing for
    # read-cursor purposes but are never printed as messages.
    _attach_group_poll_tallies(
        inbox, entries, _own_destination_hash(config)
    )
    _attach_dm_poll_tallies(
        inbox, entries, _own_destination_hash(config)
    )
    shown = entries if limit is None else entries[:limit]
    omitted = entries[len(shown):]
    # Unread mail the window omitted: reported, never silently cleared.
    more_unread: Dict[str, int] = {}
    for entry in omitted:
        if entry.get("unread"):
            destination = str(entry.get("destination") or "")
            more_unread[destination] = more_unread.get(destination, 0) + 1
    if _json_mode(args):
        _emit(
            {
                "messages": shown,
                "count": len(shown),
                "more_unread": more_unread,
                "ignored_unread": ignored_unread,
                "archived_unread": archived_unread,
            },
            True,
        )
    elif not shown:
        if query is not None:
            print(f"No messages matching {query!r}.")
        elif more_unread:
            _print_more_unread(registry, more_unread)
        else:
            print(
                "No unread messages." if unread_only else "No messages received yet."
            )
        if ignored_unread:
            _print_ignored_unread(ignored_unread)
        if archived_unread:
            _print_archived_unread(archived_unread)
    else:
        own_hash = _own_destination_hash(config)
        printable = [
            entry
            for entry in shown
            if not is_group_poll_vote(entry)
            and not is_group_poll_close(entry)
            and not is_dm_poll_vote(entry)
            and not is_dm_poll_close(entry)
        ]
        for entry in printable:
            _print_message_entry(
                inbox, registry, entry, blocked=blocked, own_hash=own_hash
            )
        if not printable and shown:
            print("No chat messages to show — only poll votes.")
        if more_unread:
            _print_more_unread(registry, more_unread)
        if ignored_unread:
            _print_ignored_unread(ignored_unread)
        if archived_unread:
            _print_archived_unread(archived_unread)
    # Reading a conversation advances its cursor to the newest message
    # just shown, so the next listing only flags what arrived since.  A
    # conversation with unread mail omitted by --limit is left alone:
    # a cursor past the newest shown message would silently clear it.
    # A search is a read-only lookup: messages the filter excluded
    # were never shown, so nothing is marked read and no receipts go
    # out — otherwise an older unread message that did not match would
    # silently lose its unread state.
    if query is None:
        # Read cursors are per-conversation, so a DM and a group post
        # from the same sender advance independently — but a listing
        # that only shows part of one conversation must still not
        # advance past mail it did not show.  Track the newest shown
        # sequence per conversation, skipping a conversation entirely
        # when the limit cut off one of its unread messages.
        partially_listed = {
            _conversation_key(entry) for entry in omitted if entry.get("unread")
        }
        newest_shown: Dict[str, int] = {}
        for entry in shown:
            key = _conversation_key(entry)
            if key in partially_listed:
                continue
            newest_shown[key] = max(newest_shown.get(key, 0), int(entry["seq"]))
        # Cap each conversation's advance just before its oldest
        # unread message that this listing did not show — ignored-group
        # mail, archived mail, or mail a filtered view (group,
        # --ignored, --archived) left out.  The capped message stays
        # unread until the owner displays it; the next listing that
        # does advances past it.
        shown_seqs = {int(entry["seq"]) for entry in shown}
        hidden_floor: Dict[str, int] = {}
        for entry in inbox.list_messages(limit=None):
            key = _conversation_key(entry)
            if key not in newest_shown:
                continue
            if not entry.get("unread"):
                continue
            if int(entry["seq"]) in shown_seqs:
                continue
            seq = int(entry["seq"])
            if key not in hidden_floor or seq < hidden_floor[key]:
                hidden_floor[key] = seq
        for key, up_to_seq in newest_shown.items():
            floor = hidden_floor.get(key)
            if floor is not None:
                up_to_seq = min(up_to_seq, floor - 1)
            if key.startswith("group:"):
                inbox.mark_read(group=key[len("group:"):], up_to_seq=up_to_seq)
            else:
                inbox.mark_read(
                    peer=key[len("dm:"):], up_to_seq=up_to_seq
                )
        # Listing a DM is reading it: send the sender a receipt, unless
        # this node opted out.  Best-effort — failures stay silent.
        outcome = _send_displayed_receipts(config, inbox, shown)
        if not _json_mode(args) and (outcome["sent"] or outcome["queued"]):
            parts = []
            if outcome["sent"]:
                parts.append(f"{outcome['sent']} peer(s) told their message was read")
            if outcome["queued"]:
                parts.append(
                    f"{outcome['queued']} peer(s) unreachable — receipt queued"
                )
            print(f"  ({'; '.join(parts)})")
    return _EXIT_OK


def cmd_messages_delete(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        removed = inbox.delete(args.target)
    except InboxError as exc:
        raise NodeError(str(exc)) from exc
    peer = registry.get(removed.get("destination") or "")
    name = peer.display_name if peer is not None else removed.get("from") or "unknown"
    if _json_mode(args):
        _emit({"deleted": removed}, True)
    else:
        short = _safe_id_prefix(str(removed.get("id") or ""))
        when = _format_message_time(float(removed.get("received_at", 0)))
        print(f"deleted [{short}] from {name} ({when})")
        print(
            "note: only this node's copy was deleted; the sender keeps"
            " theirs, and sealed copies already held by relays are"
            " unaffected."
        )
    return _EXIT_OK


def cmd_messages_show(args: argparse.Namespace) -> int:
    """Show one stored message's full details: sender, times, text, reactions, poll, state.

    Read-only: resolving the message never advances the read cursor
    and never sends read receipts, like `tau messages --search` and
    `tau pinned`.  A retracted message is already gone from the inbox
    — the retract path deletes the stored record — so it reads as
    unknown here, never as a surviving message.
    """
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        entry = inbox.resolve(args.target)
    except (ValueError, InboxError) as exc:
        raise NodeError(str(exc)) from exc
    # Blocked senders' stored messages stay hidden here too, exactly as
    # the listing hides them; an unreadable block-list hides every
    # received message.  Sent messages are never filtered by sender.
    blocked = load_blocked_set(config.blocklist_path)
    if entry.get("direction") != "out":
        sender = str(entry.get("destination") or "")
        if blocked is None:
            raise NodeError(
                "cannot show a received message while the blocklist"
                " cannot be read"
            )
        if sender in blocked:
            raise NodeError("cannot show a message from a blocked peer")
    # Conversation state the listing applies before it derives unread
    # flags: a corrupt mutes file fails open (unmuted, with a warning),
    # and quiet hours silence every conversation at once.
    muted_peers = muted_peers_for(config.muted_path)
    muted_groups = expand_muted_groups(
        muted_groups_for(config.muted_path),
        GroupStore(config.groups_path).list_groups(),
    )
    quiet = quiet_hours_active(config.quiet_hours_path)
    # The listing's derived view carries the unread flag and the
    # reaction list; the raw record does not.  Resolve first so a
    # prefix or leg id names the message, then take that record's
    # derived view — a stored record always lists, so a missing view
    # falls back to the raw record with reactions read directly.
    view = next(
        (
            item
            for item in inbox.list_messages(
                limit=None,
                direction=None,
                exclude_dm_from=muted_peers,
                exclude_group=muted_groups,
                quiet=quiet,
            )
            if item.get("id") == entry.get("id")
        ),
        None,
    )
    shown = dict(view) if view is not None else dict(entry)
    if not shown.get("reactions"):
        shown["reactions"] = inbox.reactions_for(str(entry.get("id") or ""))
    if shown.get("kind") == "group" and shown.get("reactions"):
        # A stored reaction from a non-member must never be displayed,
        # even when cleanup left it in the inbox — the same filter the
        # listing applies before anything prints.
        rosters = pinned_rosters(
            GroupStore(config.groups_path),
            {str(shown.get("group"))},
        )
        shown["reactions"] = visible_reactions(
            shown["reactions"], rosters.get(str(shown.get("group")))
        )
    own_hash = _own_destination_hash(config)
    if is_group_poll(shown):
        _attach_group_poll_tallies(inbox, [shown], own_hash)
    elif is_dm_poll(shown):
        _attach_dm_poll_tallies(inbox, [shown], own_hash)
    saved_kind = "group" if shown.get("kind") == "group" else "peer"
    saved_scope = (
        str(shown.get("group") or "")
        if saved_kind == "group"
        else str(shown.get("destination") or "")
    )
    try:
        saved = load_saved_messages(config.saved_messages_path).is_saved(
            saved_kind, saved_scope, shown.get("id")
        )
    except SavedMessagesError as exc:
        # A corrupt star list must not fail the show: the message
        # reads as unsaved, the same fail-open rule the ignored-groups
        # list follows, and the warning surfaces the corruption.
        log.warning("Starred-message list is unusable (%s); showing unsaved", exc)
        saved = False
    is_group = shown.get("kind") == "group"
    if is_group:
        group_id = str(shown.get("group") or "")
        conversation_muted = group_id in muted_groups
        ignored = group_id in load_ignored_group_ids(config.ignored_groups_path)
        archived = False
    else:
        destination = str(shown.get("destination") or "")
        conversation_muted = destination in muted_peers
        ignored = False
        archived = destination in load_archived_set(config.archived_path)
    if _json_mode(args):
        payload = dict(shown)
        payload["unread"] = bool(shown.get("unread")) and shown.get("direction") != "out"
        payload["saved"] = saved
        payload["muted"] = conversation_muted
        if is_group:
            payload["ignored"] = ignored
        else:
            payload["archived"] = archived
        _emit({"message": payload}, True)
        return _EXIT_OK
    _print_message_entry(
        inbox, registry, shown, blocked=blocked, own_hash=own_hash, full_id=True
    )
    print(f"  sent: {_format_optional_time(shown.get('sent_at'))}")
    if shown.get("read_at") is not None:
        print(f"  read: {_format_optional_time(shown.get('read_at'))}")
    if shown.get("receipt_sent_at") is not None:
        print(f"  receipt sent: {_format_optional_time(shown.get('receipt_sent_at'))}")
    if shown.get("edited_at") is not None:
        print(f"  edited: {_format_optional_time(shown.get('edited_at'))}")
    state: List[str] = []
    if shown.get("unread") and shown.get("direction") != "out":
        state.append("unread")
    if shown.get("pinned"):
        state.append("pinned")
    if saved:
        state.append("saved")
    if shown.get("relayed"):
        state.append("relayed")
    if conversation_muted:
        state.append("muted")
    if archived:
        state.append("archived")
    if ignored:
        state.append("ignored")
    expires_at = shown.get("expires_at")
    if (
        isinstance(expires_at, (int, float))
        and not isinstance(expires_at, bool)
        and float(expires_at) <= time.time()
    ):
        state.append("expired")
    print(f"  state: {', '.join(state) if state else 'none'}")
    return _EXIT_OK


def cmd_messages_unread(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    if getattr(args, "group", None):
        if getattr(args, "peer", None):
            raise NodeError("pass a peer or --group, not both")
        store = GroupStore(config.groups_path)
        group = store.find(args.group)
        # History spans renames: the mark covers the current id and
        # every predecessor, like the --group listing does.
        group_ids = _group_ids_spanning_renames(store, group)
        counts = inbox.mark_unread(group=group_ids, all=getattr(args, "all", False))
        if counts is None:
            raise NodeError(f"no stored messages in group {args.group!r}")
        unread = sum(counts["per_group"].get(gid, 0) for gid in group_ids)
        noun = "message" if unread == 1 else "messages"
        if _json_mode(args):
            _emit(
                {"group": group["id"], "unread": unread, "total": counts["total"]},
                True,
            )
        else:
            print(f"marked {unread} {noun} in group {group['name']} unread")
        return _EXIT_OK
    if not getattr(args, "peer", None):
        raise NodeError("pass a peer or --group")
    peer_hash = _resolve_inbox_peer(inbox, registry, args.peer)
    counts = inbox.mark_unread(peer_hash, all=getattr(args, "all", False))
    if counts is None:
        raise NodeError(f"no stored messages from {args.peer!r}")
    peer = registry.get(peer_hash)
    name = peer.display_name if peer is not None else short_hash(peer_hash)
    unread = counts["per_peer"].get(peer_hash, 0)
    if _json_mode(args):
        _emit({"peer": peer_hash, "unread": unread, "total": counts["total"]}, True)
    else:
        noun = "message" if unread == 1 else "messages"
        print(f"marked {unread} {noun} from {name} unread")
    return _EXIT_OK


def cmd_pin(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        entry, changed = inbox.pin(args.message)
    except InboxError as exc:
        raise NodeError(str(exc)) from exc
    if _json_mode(args):
        _emit({"pinned": entry, "already_pinned": not changed}, True)
        return _EXIT_OK
    short = _safe_id_prefix(str(entry.get("id") or ""))
    when = _format_message_time(float(entry.get("received_at", 0)))
    state = "pinned" if changed else "already pinned"
    print(f"{state} [{short}] {_message_peer_label(registry, entry)} ({when})")
    if changed:
        print(
            "note: pins are local-only; the peer's copy is unaffected"
            " and the pin is never sent."
        )
    return _EXIT_OK


def cmd_unpin(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        entry, changed = inbox.unpin(args.message)
    except InboxError as exc:
        raise NodeError(str(exc)) from exc
    if _json_mode(args):
        _emit({"unpinned": entry, "was_pinned": changed}, True)
        return _EXIT_OK
    short = _safe_id_prefix(str(entry.get("id") or ""))
    when = _format_message_time(float(entry.get("received_at", 0)))
    state = "unpinned" if changed else "was not pinned"
    print(f"{state} [{short}] {_message_peer_label(registry, entry)} ({when})")
    return _EXIT_OK


def cmd_pinned(args: argparse.Namespace) -> int:
    """List pinned messages, received and sent.

    A read-only lookup, like `tau messages --search`: it never
    advances the read cursor and never sends read receipts.
    """
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    peer_hash = None
    if getattr(args, "peer", None):
        # The listing covers sent pins too, so the lookup must search
        # recipients as well as senders.
        peer_hash = _resolve_inbox_peer(
            inbox, registry, args.peer, direction=None
        )
    limit = getattr(args, "limit", 30)
    if limit is not None and limit < 0:
        raise NodeError("--limit must not be negative")
    entries = inbox.list_messages(
        limit=None, peer=peer_hash, pinned_only=True, direction=None
    )
    # Blocked senders' history stays stored but is hidden locally,
    # like `tau messages`; unblocking restores the view.  The filter
    # applies to received pins only: sent pins are this node's own
    # messages, not a blocked sender's, so blocking the recipient
    # never hides them — and an unreadable list hides received pins
    # (fail closed) while leaving sent ones visible.
    blocked = load_blocked_set(config.blocklist_path)
    if blocked is None:
        entries = [entry for entry in entries if entry.get("direction") == "out"]
    elif blocked:
        entries = [
            entry
            for entry in entries
            if entry.get("direction") == "out"
            or entry.get("destination") not in blocked
        ]
    shown = entries if limit is None else entries[:limit]
    if _json_mode(args):
        _emit({"messages": shown, "count": len(shown)}, True)
    elif not shown:
        print("No pinned messages.")
    else:
        own_hash = _own_destination_hash(config)
        _attach_group_poll_tallies(inbox, shown, own_hash)
        _attach_dm_poll_tallies(inbox, shown, own_hash)
        for entry in shown:
            _print_message_entry(
                inbox, registry, entry, blocked=blocked, own_hash=own_hash
            )
    return _EXIT_OK


def cmd_messages_export(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    peer_hash = None
    scope = "all senders"
    if getattr(args, "peer", None):
        registry = PeerRegistry(config.peers_path)
        peer_hash = _resolve_inbox_peer(inbox, registry, args.peer)
        scope = f"sender {short_hash(peer_hash)}"
    document = inbox.export_document(peer=peer_hash)
    count = len(document["messages"])
    text = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True)
    if getattr(args, "output", None):
        atomic_write_text(args.output, text + "\n")
        print(f"Exported {count} messages ({scope}) to {args.output}")
    else:
        print(text)
    return _EXIT_OK


def cmd_messages_import(args: argparse.Namespace) -> int:
    config = _make_config(args)
    if str(args.file) == "-":
        raw = sys.stdin.read()
    else:
        raw = args.file.read_text(encoding="utf-8")
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise NodeError(f"export file is not valid JSON: {exc}") from exc
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)
    # Hold the mailbox transaction across the import: it reloads the
    # markers from disk, so a retract persisted by a running `tau serve`
    # is seen, and it cannot land between the check and the insert.
    with mailbox.transaction():
        result = inbox.import_messages(
            document,
            is_retracted=mailbox.is_retracted,
            is_group_retracted=mailbox.is_group_retracted,
            pending_group_edit=mailbox.pending_group_edit,
        )
    if _json_mode(args):
        _emit(result, True)
    else:
        print(
            f"Imported {result['imported']} messages "
            f"({result['present']} already present, "
            f"{result['skipped']} skipped)"
        )
        for error in result["errors"]:
            ref = error["id"] if error["id"] else "?"
            print(f"  skipped {ref}: {error['error']}")
    return _EXIT_OK if result["skipped"] == 0 else _EXIT_ERROR


def cmd_messages_prune(args: argparse.Namespace) -> int:
    config = _make_config(args)
    dry_run = bool(getattr(args, "dry_run", False))
    # A dry run must not persist an expiry purge: open the inbox read-only.
    inbox = Inbox(config.inbox_path, read_only=dry_run)
    registry = PeerRegistry(config.peers_path)
    older_than = _parse_duration(
        args.older_than,
        max_seconds=_PRUNE_MAX_SECONDS,
        label="--older-than",
    )
    peer_hash = None
    group_id = None
    scope_parts = []
    if getattr(args, "peer", None):
        peer_hash = _resolve_inbox_peer(inbox, registry, args.peer)
        peer = registry.get(peer_hash)
        scope_parts.append(
            "with "
            + (peer.display_name if peer is not None else short_hash(peer_hash))
        )
    if getattr(args, "group", None):
        group = GroupStore(config.groups_path).find(args.group)
        group_id = group["id"]
        scope_parts.append(f"in group {group['name']}")
    result = inbox.prune(
        older_than,
        peer=peer_hash,
        group=group_id,
        dry_run=dry_run,
    )
    scope = " ".join(scope_parts) if scope_parts else "from all senders"
    if _json_mode(args):
        _emit(result, True)
    else:
        verb = "Would prune" if result["dry_run"] else "Pruned"
        print(
            f"{verb} {result['pruned']} messages older than {args.older_than} ({scope})"
        )
        if result["dry_run"]:
            print("dry run — nothing was deleted.")
        else:
            print(
                "note: only this node's stored inbox copies were deleted"
                " (received and sent); the other parties keep theirs, and"
                " sealed copies already held by relays are unaffected."
                " Sent history shown by `tau messages --sent` and messages"
                " still queued in the outbox remain stored locally."
            )
    return _EXIT_OK


def _request_sender_label(registry: PeerRegistry, request_sender: str) -> str:
    """Display name for a request's sender, or their short hash."""
    peer = registry.get(request_sender)
    if peer is not None:
        return peer.display_name
    return short_hash(request_sender)


def _request_preview(text: str, limit: int = 80) -> str:
    first = text.splitlines()[0] if text else ""
    if len(first) <= limit:
        return first
    return first[: limit - 1] + "…"


def cmd_messages_requests(args: argparse.Namespace) -> int:
    """List DMs held for review from peers outside the allow-list.

    A read-only lookup: listing requests never marks anything read
    and never sends read receipts — the messages have not been
    delivered, so there is nothing to receipt.
    """
    config = _make_config(args)
    try:
        requests = list_dm_requests(config.dm_requests_path)
    except DmRequestsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_ERROR
    registry = PeerRegistry(config.peers_path)
    if _json_mode(args):
        _emit({"requests": [request.to_dict() for request in requests]}, True)
        return _EXIT_OK
    if not requests:
        print("no DM requests — messages from peers outside the allow-list")
        print("are held here for review instead of being dropped.")
        return _EXIT_OK
    for request in requests:
        name = _request_sender_label(registry, request.sender)
        when = _format_message_time(request.received_at)
        print(f"[{_safe_id_prefix(request.id)}] from {name} ({when})")
        print(f"    {_request_preview(request.text)}")
    print(
        "accept one with `tau messages accept-request <ID>`;"
        " decline with `tau messages decline-request <ID>`."
    )
    return _EXIT_OK


def cmd_messages_accept_request(args: argparse.Namespace) -> int:
    """Allow-list a request's sender and deliver the held message.

    The acceptance itself lives in
    :func:`tau.dm_requests.accept_dm_request`, shared with the web
    console — this command only resolves the sender's display name
    and reports the outcome.
    """
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    try:
        acceptance = accept_dm_request(
            dm_requests_path=config.dm_requests_path,
            allowlist_path=config.allowlist_path,
            blocklist_path=config.blocklist_path,
            inbox=Inbox(config.inbox_path),
            mailbox=Mailbox(config.mailbox_path),
            # Resolved under the request-store lock, from the request
            # actually accepted, so the label cannot name another one.
            sender_name=lambda sender: _request_sender_label(registry, sender),
            ref=args.target,
        )
    except DmRequestsError as exc:
        raise NodeError(str(exc)) from exc
    request = acceptance.request
    delivered = acceptance.delivered
    allowlisted = acceptance.allowlisted
    skipped = acceptance.skipped
    name = _request_sender_label(registry, request.sender)
    if _json_mode(args):
        _emit(acceptance.to_dict(), True)
        return _EXIT_OK
    short = _safe_id_prefix(request.id)
    when = _format_message_time(request.received_at)
    if delivered:
        state = "delivered to the inbox"
    else:
        state = f"not delivered ({skipped})"
    if allowlisted:
        print(f"accepted [{short}] from {name} ({when}) — sender allow-listed, message {state}")
        print("  (a running serve applies the allow-list change when it restarts)")
    else:
        print(f"accepted [{short}] from {name} ({when}) — sender already allow-listed, message {state}")
    return _EXIT_OK


def cmd_messages_decline_request(args: argparse.Namespace) -> int:
    """Drop a held DM request without allow-listing its sender.

    The decline itself lives in
    :func:`tau.dm_requests.decline_dm_request`, shared with the web
    console — this command only resolves the sender's display name
    and reports the outcome.
    """
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    block = bool(getattr(args, "block", False))
    try:
        decline = decline_dm_request(
            dm_requests_path=config.dm_requests_path,
            blocklist_path=config.blocklist_path,
            ref=args.target,
            block=block,
        )
    except DmRequestsError as exc:
        raise NodeError(str(exc)) from exc
    request = decline.request
    name = _request_sender_label(registry, request.sender)
    if _json_mode(args):
        _emit(decline.to_dict(), True)
        return _EXIT_OK
    short = _safe_id_prefix(request.id)
    when = _format_message_time(request.received_at)
    print(f"declined [{short}] from {name} ({when})")
    if decline.blocked:
        print("  blocked the sender — their later messages are dropped, not held.")
    elif block:
        print("  the sender was already blocked.")
    else:
        print("  the sender stays outside the allow-list; use `tau block` to block them.")
    return _EXIT_OK


def cmd_archive(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    destination = _resolve_inbox_peer(inbox, registry, args.peer)
    peer = registry.get(destination)
    name = peer.display_name if peer is not None else "unknown"
    # Reload inside the lock: two archive commands racing must not
    # discard each other's change to a different peer.
    with archive_lock(config.archived_path):
        try:
            archive = load_archive(config.archived_path)
        except ArchiveError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        already = destination in archive.archived
        if not already:
            archive = archive.with_added(destination)
            save_archive(config.archived_path, archive)
    if _json_mode(args):
        _emit(
            {
                "destination": destination,
                "name": name,
                "archived": True,
                "changed": not already,
            },
            True,
        )
        return _EXIT_OK
    if already:
        print(
            f"Conversation with {name} ({short_hash(destination)}) "
            "is already archived."
        )
    else:
        print(
            f"Archived conversation with {name} ({short_hash(destination)}). "
            "Messages keep arriving; `tau unarchive` restores the view."
        )
    return _EXIT_OK


def cmd_unarchive(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    destination = _resolve_inbox_peer(inbox, registry, args.peer)
    peer = registry.get(destination)
    name = peer.display_name if peer is not None else "unknown"
    # Reload inside the lock: two archive commands racing must not
    # discard each other's change to a different peer.
    with archive_lock(config.archived_path):
        try:
            archive = load_archive(config.archived_path)
        except ArchiveError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        was_archived = destination in archive.archived
        if was_archived:
            archive = archive.with_removed(destination)
            save_archive(config.archived_path, archive)
    if _json_mode(args):
        _emit(
            {
                "destination": destination,
                "name": name,
                "archived": False,
                "changed": was_archived,
            },
            True,
        )
        return _EXIT_OK
    if was_archived:
        print(
            f"Restored conversation with {name} ({short_hash(destination)}) "
            "to `tau messages`."
        )
    else:
        print(
            f"Conversation with {name} ({short_hash(destination)}) "
            "was not archived."
        )
    return _EXIT_OK


def cmd_disappearing(args: argparse.Namespace) -> int:
    config = _make_config(args)
    if getattr(args, "off", False) and args.duration is not None:
        print("error: --off and a duration cannot be combined", file=sys.stderr)
        return _EXIT_ERROR
    if args.group is not None:
        store = GroupStore(config.groups_path)
        group = store.find(args.group)
        key = group["id"]
        label = f"group {group['name']} ({short_hash(key)})"
        is_group = True
        is_channel = False
    elif getattr(args, "channel", None) is not None:
        try:
            key = clean_channel_name(args.channel)
        except ChannelError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        label = f"channel #{key}"
        is_group = False
        is_channel = True
    else:
        inbox = Inbox(config.inbox_path)
        registry = PeerRegistry(config.peers_path)
        key = _resolve_inbox_peer(inbox, registry, args.peer)
        peer = registry.get(key)
        name = peer.display_name if peer is not None else "unknown"
        label = f"conversation with {name} ({short_hash(key)})"
        is_group = False
        is_channel = False
    seconds = None
    if args.duration is not None:
        try:
            seconds = _parse_duration(
                args.duration,
                label="duration",
                max_seconds=MAX_DISAPPEARING_SECONDS,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
    # Reload inside the lock: two disappearing commands racing must not
    # discard each other's change to a different conversation.
    with disappearing_lock(config.disappearing_path):
        try:
            defaults = load_disappearing_defaults(config.disappearing_path)
        except DisappearingError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        if is_group:
            previous = defaults.group_default(key)
            if getattr(args, "off", False):
                defaults = defaults.without_group(key)
            elif seconds is not None:
                defaults = defaults.with_group(key, seconds)
        elif is_channel:
            previous = defaults.channel_default(key)
            if getattr(args, "off", False):
                defaults = defaults.without_channel(key)
            elif seconds is not None:
                defaults = defaults.with_channel(key, seconds)
        else:
            previous = defaults.peer_default(key)
            if getattr(args, "off", False):
                defaults = defaults.without_peer(key)
            elif seconds is not None:
                defaults = defaults.with_peer(key, seconds)
        changed = (previous != seconds) if seconds is not None or getattr(
            args, "off", False
        ) else False
        if seconds is not None or getattr(args, "off", False):
            save_disappearing_defaults(config.disappearing_path, defaults)
    current = seconds if seconds is not None else (
        None if getattr(args, "off", False) else previous
    )
    if _json_mode(args):
        _emit(
            {
                "target": key,
                "group": is_group,
                "channel": is_channel,
                "disappearing_seconds": current,
                "changed": changed,
            },
            True,
        )
        return _EXIT_OK
    if seconds is not None:
        print(
            f"Messages to {label} now disappear after "
            f"{_describe_duration(seconds)} unless a send passes its own --expire."
        )
    elif getattr(args, "off", False):
        if previous is None:
            print(f"Messages to {label} already had no disappearing default.")
        else:
            print(f"Cleared the disappearing default for {label}.")
    elif previous is None:
        print(
            f"Messages to {label} have no disappearing default; "
            "pass a duration like 1h to set one."
        )
    else:
        print(
            f"Messages to {label} disappear after "
            f"{_describe_duration(previous)} unless a send passes its own --expire."
        )
    return _EXIT_OK


def _resolve_pin_target(
    config: TauConfig, args: argparse.Namespace
) -> Tuple[str, List[str], str]:
    """Resolve a pin command's --peer/--group/--channel to (kind, keys, label).

    A group resolves to its whole rename lineage, like a mute: the
    pin follows the conversation across a rename.  A channel must be
    joined — pinning a channel this node never joined is a typo, not
    a pin.  Unpinning a channel needs no membership: a pin for a
    channel left since stays removable.
    """
    if getattr(args, "group", None):
        try:
            group = GroupStore(config.groups_path).find(args.group)
        except ValueError:
            raise NodeError(f"unknown group: {args.group!r}") from None
        lineage = sorted(group_lineage(group))
        label = f"group {group['name']} ({short_hash(group['id'])})"
        return "group", lineage, label
    if getattr(args, "channel", None):
        try:
            name = clean_channel_name(args.channel)
        except ChannelError as exc:
            raise NodeError(str(exc)) from exc
        return "channel", [name], f"channel #{name}"
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    destination = _resolve_inbox_peer(inbox, registry, args.peer)
    peer = registry.get(destination)
    name = peer.display_name if peer is not None else "unknown"
    return (
        "peer",
        [destination],
        f"conversation with {name} ({short_hash(destination)})",
    )


def _edit_pinned(
    config: TauConfig,
    kind: str,
    keys: List[str],
    *,
    pin: bool,
) -> Tuple[bool, PinnedList]:
    """Pin or unpin every key in *keys*, reloading inside the lock.

    Returns (changed, new list): pinning an already-pinned
    conversation — or unpinning an unpinned one — is a no-op, not an
    error, mirroring the archive commands.
    """
    with pinned_lock(config.pinned_path):
        try:
            pinned = load_pinned(config.pinned_path)
        except PinnedError as exc:
            raise NodeError(str(exc)) from exc
        updated = pinned
        changed = False
        for key in keys:
            already = updated.is_pinned(kind, key)
            if pin and not already:
                updated = updated.with_pinned(kind, key)
                changed = True
            elif not pin and already:
                updated = updated.without(kind, key)
                changed = True
        if changed:
            save_pinned(config.pinned_path, updated)
    return changed, updated


def cmd_pin_conversation(args: argparse.Namespace) -> int:
    config = _make_config(args)
    kind, keys, label = _resolve_pin_target(config, args)
    if kind == "channel":
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        conn = app.connect()
        try:
            if not is_channel_member(conn, keys[0]):
                raise NodeError(
                    f"not a member of channel {keys[0]!r}; join it first"
                )
        finally:
            conn.close()
    try:
        changed, _ = _edit_pinned(config, kind, keys, pin=True)
    except PinnedError as exc:
        raise NodeError(str(exc)) from exc
    if _json_mode(args):
        _emit(
            {"kind": kind, "target": keys[0], "pinned": True, "changed": changed},
            True,
        )
        return _EXIT_OK
    if changed:
        print(f"Pinned {label} — it now stays at the top of the list.")
    else:
        print(f"{label} is already pinned.")
    return _EXIT_OK


def cmd_unpin_conversation(args: argparse.Namespace) -> int:
    config = _make_config(args)
    kind, keys, label = _resolve_pin_target(config, args)
    changed, _ = _edit_pinned(config, kind, keys, pin=False)
    if _json_mode(args):
        _emit(
            {"kind": kind, "target": keys[0], "pinned": False, "changed": changed},
            True,
        )
        return _EXIT_OK
    if changed:
        print(f"Unpinned {label}.")
    else:
        print(f"{label} was not pinned.")
    return _EXIT_OK


def cmd_pinned_conversations(args: argparse.Namespace) -> int:
    """List pinned conversations, newest pin first."""
    config = _make_config(args)
    try:
        pinned = load_pinned(config.pinned_path)
    except PinnedError as exc:
        raise NodeError(str(exc)) from exc
    registry = PeerRegistry(config.peers_path)
    store = GroupStore(config.groups_path)

    def describe(entry) -> Dict[str, Any]:
        if entry.kind == "peer":
            peer = registry.get(entry.target)
            name = peer.display_name if peer is not None else "unknown"
            label = f"{name} ({short_hash(entry.target)})"
        elif entry.kind == "group":
            try:
                group = store.find(entry.target)
                label = f"group {group['name']} ({short_hash(entry.target)})"
            except ValueError:
                label = f"group {short_hash(entry.target)}"
        else:
            label = f"channel #{entry.target}"
        return {
            "kind": entry.kind,
            "target": entry.target,
            "label": label,
            "pinned_at": entry.pinned_at,
        }

    entries = [describe(entry) for entry in pinned.entries]
    if _json_mode(args):
        _emit({"pinned": entries, "count": len(entries)}, True)
        return _EXIT_OK
    if not entries:
        print("No pinned conversations — pin one with `tau pin-conversation`.")
        return _EXIT_OK
    for entry in entries:
        age = _ago(time.time() - float(entry["pinned_at"]))
        print(f"{entry['label']} — pinned {age} ago")
    return _EXIT_OK


def _resolve_dm_saved_target(
    inbox: Inbox, peer_hash: str, ref: str
) -> Dict[str, Any]:
    """Resolve ``tau save`` / ``unsave --peer`` to one stored DM.

    Same rules as :func:`_resolve_group_pin_target` but peer-scoped:
    a star attaches to a stored record, so a still-queued message has
    nothing to star and is never a candidate.  An exact id wins over a
    longer id that merely starts with it; a prefix matching more than
    one record, or nothing at all, is refused.
    """
    ref = ref.strip()
    if not ref:
        raise NodeError("message must not be empty")
    matches = inbox.find_prefix(ref, peer=peer_hash)
    exact = [m for m in matches if str(m["id"]) == ref]
    if not exact:
        folded = ref.lower()
        exact = [m for m in matches if str(m["id"]).lower() == folded]
    if exact:
        matches = exact
    if len(matches) > 1:
        raise AmbiguousMessageRefError(
            f"{len(matches)} messages from this peer match {ref!r};"
            " use a longer prefix"
        )
    if not matches:
        raise NodeError(
            f"no message {ref!r} from this peer to save;"
            " stars attach to a stored message from this peer's history"
        )
    return matches[0]


def _saved_entry_for_ref(
    config: TauConfig, kind: str, scopes: Sequence[str], ref: str
) -> Any:
    """The one starred entry of *kind* in *scopes* that *ref* names, or ``None``.

    Used by ``unsave`` when the stored message is gone: a star outlives
    its message, so the star itself is what gets removed.  An exact id
    wins over a longer id that merely starts with it; a prefix matching
    more than one star is refused.
    """
    try:
        saved = load_saved_messages(config.saved_messages_path)
    except SavedMessagesError as exc:
        raise NodeError(str(exc)) from exc
    folded = ref.lower()
    candidates = [
        entry
        for entry in saved.entries
        if entry.kind == kind
        and entry.scope in scopes
        and entry.message_id.lower().startswith(folded)
    ]
    exact = [entry for entry in candidates if entry.message_id.lower() == folded]
    if exact:
        candidates = exact
    if len(candidates) > 1:
        raise NodeError(
            f"{len(candidates)} starred messages match {ref!r};"
            " use a longer prefix"
        )
    return candidates[0] if candidates else None


def _saved_scopes(config: TauConfig, kind: str) -> List[str]:
    """Starred scopes of *kind*, for resolving a deleted peer.

    A corrupt list yields no scopes here — the starred-entry lookup
    below raises the corruption error if the flow reaches it — so a
    bad file cannot turn an ``unsave`` into a misleading
    "unknown peer".
    """
    try:
        saved = load_saved_messages(config.saved_messages_path)
    except SavedMessagesError:
        return []
    return [entry.scope for entry in saved.entries if entry.kind == kind]


def _valid_channel_digest_ref(ref: str) -> bool:
    """Whether *ref* is a channel digest reference the live resolver accepts.

    A digest prefix must be at least 4 hex characters and no longer
    than a full digest; anything else is refused by
    :func:`resolve_message_digest` before any message is looked up, so
    it must not fall through to the starred-entry search either.
    """
    text = ref.strip().lower()
    return (
        4 <= len(text) <= HASH_HEX_LENGTH
        and all(ch in "0123456789abcdef" for ch in text)
    )


def _resolve_saved_message(
    config: TauConfig, args: argparse.Namespace, *, missing_ok: bool = False
) -> Tuple[str, str, str, Optional[Dict[str, Any]]]:
    """Resolve ``tau save`` / ``unsave`` scope + ref to a stored message.

    Returns ``(kind, scope, message_id, record)``: the record is the
    stored inbox record for a DM or group message, or the stored
    channel message for a channel.  A star attaches to a stored
    record, like a group pin, so a still-queued leg has nothing to
    star.  For a group the scope is the stored record's group id, so a
    message stored before a rename keeps its star under the id it was
    stored under.

    With *missing_ok* (``unsave``), a message that is no longer stored
    resolves to the matching star instead, with a ``None`` record, so
    the star can still be cleared.  A channel star attaches to a
    message channel history shows: an expired or retracted row cannot
    be starred, though it can still be unstarred through the fallback.
    """
    ref = str(getattr(args, "message", "") or "").strip()
    if not ref:
        raise NodeError("message must not be empty")
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    if getattr(args, "peer", None) is not None:
        extra_hashes = _saved_scopes(config, "peer") if missing_ok else None
        destination = _resolve_inbox_peer(
            inbox, registry, args.peer, direction=None, extra_hashes=extra_hashes
        )
        try:
            entry = _resolve_dm_saved_target(inbox, destination, ref)
        except AmbiguousMessageRefError:
            raise
        except NodeError:
            found = (
                _saved_entry_for_ref(config, "peer", [destination], ref)
                if missing_ok
                else None
            )
            if found is None:
                raise
            return "peer", found.scope, found.message_id, None
        return "peer", destination, str(entry["id"]), entry
    if getattr(args, "group", None) is not None:
        store = GroupStore(config.groups_path)
        group = store.find(args.group)
        group_ids = _group_ids_spanning_renames(store, group)
        try:
            entry = _resolve_group_pin_target(inbox, group_ids, ref)
        except AmbiguousMessageRefError:
            raise
        except NodeError:
            found = (
                _saved_entry_for_ref(config, "group", group_ids, ref)
                if missing_ok
                else None
            )
            if found is None:
                raise
            return "group", found.scope, found.message_id, None
        scope = str(entry.get("group") or group["id"])
        return "group", scope, str(entry["id"]), entry
    try:
        name = clean_channel_name(args.channel)
    except ChannelError as exc:
        raise NodeError(str(exc)) from exc
    app = ChannelApp(config, _tan_identity(config), name=config.name)
    conn = app.connect()
    try:
        try:
            digest = resolve_message_digest(conn, name, ref)
            # A star attaches to a message channel history shows:
            # an expired or retracted row is hidden there, so it
            # cannot be starred — the listing would render it as a
            # missing-message stub the moment it was saved.
            record = get_visible_channel_message(conn, digest)
            if record is None and not missing_ok:
                if get_channel_message(conn, digest) is not None:
                    raise NodeError(
                        f"message {ref!r} in #{name} is expired or retracted;"
                        " stars attach to visible messages"
                    )
        except ChannelError as exc:
            if not missing_ok:
                raise NodeError(str(exc)) from exc
            # Only a valid reference for a message that is gone may
            # fall through to the star: an ambiguous prefix, or a
            # reference the live resolver refuses (too short, too
            # long, not hex), stays refused instead of unsaving
            # whatever star the prefix happens to match.
            if isinstance(exc, AmbiguousDigestError) or not _valid_channel_digest_ref(
                ref
            ):
                raise NodeError(str(exc)) from exc
            found = _saved_entry_for_ref(config, "channel", [name], ref)
            if found is None:
                raise NodeError(str(exc)) from exc
            return "channel", name, found.message_id, None
    finally:
        conn.close()
    if record is None:
        if missing_ok:
            found = _saved_entry_for_ref(config, "channel", [name], ref)
            if found is not None:
                return "channel", name, found.message_id, None
        raise NodeError(f"no stored message {ref!r} in #{name}")
    return "channel", name, digest, record


def _edit_saved_messages(
    config: TauConfig,
    kind: str,
    scope: str,
    message_id: str,
    *,
    save: bool,
    sender: Optional[str] = None,
    direction: Optional[str] = None,
) -> Tuple[bool, SavedMessages]:
    """Star or unstar one message, reloading inside the lock.

    Returns ``(changed, new list)``: starring an already-starred
    message — or unstarring an unstarred one — is a no-op, not an
    error, mirroring the pinned-conversation commands.  A corrupt
    list is refused, not overwritten, so a bad file cannot silently
    drop the owner's other stars.  ``sender``/``direction`` are
    recorded with a new star so the blocklist still applies once the
    message itself is gone.
    """
    with saved_messages_lock(config.saved_messages_path):
        try:
            saved = load_saved_messages(config.saved_messages_path)
            if save:
                updated, already = saved.with_saved(
                    kind, scope, message_id, sender=sender, direction=direction
                )
                changed = not already
            else:
                updated, changed = saved.without(kind, scope, message_id)
        except SavedMessagesError as exc:
            raise NodeError(str(exc)) from exc
        if changed:
            save_saved_messages(config.saved_messages_path, updated)
    return changed, updated


def _saved_star_attribution(
    kind: str, record: Dict[str, Any], config: TauConfig
) -> Tuple[Optional[str], Optional[str]]:
    """A star's sender and direction, from the record being starred.

    The sender is the hash the blocklist is checked against: the
    peer for a DM, the message sender for a group, the author for a
    channel.  Direction is the record's own ``"in"``/``"out"`` for
    inbox records; for a channel message it is ``"out"`` when the
    author is this node, ``"in"`` otherwise.
    """
    if kind == "channel":
        sender = str(record.get("author") or "").strip() or None
        if sender is None:
            return None, None
        own = _own_destination_hash(config)
        return sender, "out" if own is not None and sender == own else "in"
    sender = str(record.get("destination") or "").strip() or None
    direction = record.get("direction")
    return sender, direction if direction in ("in", "out") else None


def _saved_stub_hidden(entry: Any, blocked: Optional[Any]) -> bool:
    """Whether a star whose message is gone must stay out of view.

    The same block rules as a stored record, from the sender and
    direction recorded when the star was created: a message this node
    sent stays visible, a blocked sender stays hidden, and an
    unreadable blocklist fails closed.  A stub whose sender was
    never recorded cannot be checked against the list, so it stays
    hidden too — showing it would expose a blocked sender's star
    metadata.
    """
    if entry.direction == "out":
        return False
    if blocked is None:
        return True
    if not entry.sender:
        return True
    return entry.sender in blocked


def _saved_scope_label(
    registry: PeerRegistry, store: GroupStore, kind: str, scope: str
) -> str:
    """One saved message's conversation, as a reader-facing label."""
    if kind == "peer":
        peer = registry.get(scope)
        name = peer.display_name if peer is not None else "unknown"
        return f"DM with {name} ({short_hash(scope)})"
    if kind == "group":
        try:
            group = store.find(scope)
            return f"group \"{group['name']}\""
        except ValueError:
            return f"group {short_hash(scope)}"
    return f"#{scope}"


def cmd_save_message(args: argparse.Namespace) -> int:
    config = _make_config(args)
    kind, scope, message_id, record = _resolve_saved_message(config, args)
    sender, direction = _saved_star_attribution(kind, record, config)
    changed, _ = _edit_saved_messages(
        config, kind, scope, message_id, save=True, sender=sender, direction=direction
    )
    if _json_mode(args):
        _emit(
            {
                "kind": kind,
                "scope": scope,
                "message_id": message_id,
                "saved": True,
                "changed": changed,
            },
            True,
        )
        return _EXIT_OK
    registry = PeerRegistry(config.peers_path)
    store = GroupStore(config.groups_path)
    label = _saved_scope_label(registry, store, kind, scope)
    if kind == "channel":
        who = str(record.get("name") or "anonymous")
        when = _format_message_time(float(record.get("created_at", 0)))
    else:
        who = _message_peer_label(registry, record)
        when = _format_message_time(float(record.get("received_at", 0)))
    short = _safe_id_prefix(message_id)
    state = "saved" if changed else "already saved"
    print(f"{state} [{short}] {who} ({when}) — {label}")
    if changed:
        print(
            "note: stars are local-only; the message stays where it is"
            " and the star is never sent."
        )
    return _EXIT_OK


def cmd_unsave_message(args: argparse.Namespace) -> int:
    config = _make_config(args)
    kind, scope, message_id, record = _resolve_saved_message(
        config, args, missing_ok=True
    )
    changed, _ = _edit_saved_messages(config, kind, scope, message_id, save=False)
    if _json_mode(args):
        _emit(
            {
                "kind": kind,
                "scope": scope,
                "message_id": message_id,
                "saved": False,
                "changed": changed,
            },
            True,
        )
        return _EXIT_OK
    registry = PeerRegistry(config.peers_path)
    store = GroupStore(config.groups_path)
    label = _saved_scope_label(registry, store, kind, scope)
    short = _safe_id_prefix(message_id)
    state = "unsaved" if changed else "was not saved"
    if record is None:
        print(f"{state} [{short}] (message no longer stored) — {label}")
        return _EXIT_OK
    if kind == "channel":
        who = str(record.get("name") or "anonymous")
        when = _format_message_time(float(record.get("created_at", 0)))
    else:
        who = _message_peer_label(registry, record)
        when = _format_message_time(float(record.get("received_at", 0)))
    print(f"{state} [{short}] {who} ({when}) — {label}")
    return _EXIT_OK


def cmd_saved_messages(args: argparse.Namespace) -> int:
    """List starred messages, newest save first, with their text."""
    config = _make_config(args)
    try:
        saved = load_saved_messages(config.saved_messages_path)
    except SavedMessagesError as exc:
        # A listing is display-only: a corrupt list fails open with
        # a warning, like the pinned-conversation listing, instead
        # of hiding stars the owner marked.
        log.warning(
            "Starred-message list at %s is unusable (%s); showing none",
            config.saved_messages_path,
            exc,
        )
        saved = SavedMessages()
    limit = getattr(args, "limit", 30)
    if limit is not None and limit < 0:
        raise NodeError("--limit must not be negative")
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    store = GroupStore(config.groups_path)
    # Same block rules as the history views: a blocked sender's received
    # message stays hidden, outbound stays visible, and an unreadable
    # list fails closed (channel messages hidden, received hidden).
    blocked = load_blocked_set(config.blocklist_path)
    channel_conn = None
    channel_app = None
    try:

        def message_for(entry: Any) -> Optional[Dict[str, Any]]:
            """The stored message a star names, or ``None`` when it is gone."""
            nonlocal channel_conn, channel_app
            if entry.kind == "channel":
                if channel_app is None:
                    channel_app = ChannelApp(
                        config, _tan_identity(config), name=config.name
                    )
                    channel_conn = channel_app.connect()
                assert channel_conn is not None
                return get_visible_channel_message(channel_conn, entry.message_id)
            if entry.kind == "peer":
                matches = inbox.find_prefix(entry.message_id, peer=entry.scope)
            else:
                matches = inbox.find_prefix(entry.message_id, group=entry.scope)
            exact = [
                m for m in matches if str(m.get("id")) == entry.message_id
            ]
            return exact[0] if exact else None

        def hidden(entry: Any, record: Dict[str, Any]) -> bool:
            """Whether a blocked or unreadable-list record must stay out of view."""
            if entry.kind == "channel":
                return blocked is None or str(record.get("author", "")) in blocked
            if record.get("direction") == "out":
                return False
            return blocked is None or record.get("destination") in blocked

        own_hash = _own_destination_hash(config)
        pairs: List[Tuple[Any, Optional[Dict[str, Any]]]] = []
        for entry in saved.entries:
            if limit is not None and len(pairs) >= limit:
                break
            record = message_for(entry)
            if record is None:
                # A stub has no record to check the sender against:
                # filter it from the sender/direction recorded with
                # the star instead, so a blocked sender's star does
                # not resurface once its message is gone.
                if _saved_stub_hidden(entry, blocked):
                    continue
            elif hidden(entry, record):
                continue
            pairs.append((entry, record))
        # Polls carry their tally on the record itself, derived from
        # the conversation's full history like the normal views do —
        # without this, a starred poll renders zero votes.
        records = [record for _, record in pairs if record is not None]
        _attach_dm_poll_tallies(inbox, records, own_hash)
        _attach_group_poll_tallies(inbox, records, own_hash)
        if channel_app is not None:
            _attach_channel_poll_tallies(
                channel_app, records, own_hash, blocked=blocked
            )
        rendered: List[Dict[str, Any]] = []
        for entry, record in pairs:
            rendered.append(
                {
                    "kind": entry.kind,
                    "scope": entry.scope,
                    "scope_label": _saved_scope_label(
                        registry, store, entry.kind, entry.scope
                    ),
                    "message_id": entry.message_id,
                    "saved_at": entry.saved_at,
                    "message": record,
                }
            )
    finally:
        if channel_conn is not None:
            channel_conn.close()
    if _json_mode(args):
        _emit({"saved": rendered, "count": len(rendered)}, True)
        return _EXIT_OK
    if not rendered:
        print("No starred messages — star one with `tau save`.")
        return _EXIT_OK
    for item in rendered:
        entry_label = item["scope_label"]
        when = _format_message_time(float(item["saved_at"]))
        print(f"★ saved {when} · {entry_label}")
        record = item["message"]
        if record is None:
            print(f"  (message {item['message_id'][:8]} no longer stored)")
            continue
        if item["kind"] == "channel":
            _print_channel_messages(
                [record],
                json_output=False,
                channel=item["scope"],
            )
        else:
            _print_message_entry(
                inbox, registry, record, blocked=blocked, own_hash=own_hash
            )
    return _EXIT_OK


def cmd_mute(args: argparse.Namespace) -> int:
    config = _make_config(args)
    channel = getattr(args, "channel", None)
    if (
        getattr(args, "off", False)
        and args.peer is None
        and args.group is None
        and channel is None
    ):
        print(
            "error: --off needs a --peer, --group or --channel to unmute",
            file=sys.stderr,
        )
        return _EXIT_ERROR
    for_duration = getattr(args, "for_duration", None)
    if (
        for_duration is not None
        and args.peer is None
        and args.group is None
        and channel is None
    ):
        print(
            "error: --for needs a --peer, --group or --channel to mute",
            file=sys.stderr,
        )
        return _EXIT_ERROR
    if getattr(args, "off", False) and for_duration is not None:
        print("error: --off and --for cannot be combined", file=sys.stderr)
        return _EXIT_ERROR
    if args.peer is None and args.group is None and channel is None:
        # No target lists what is muted, mirroring `tau archived`.
        muted = load_muted(config.muted_path)
        now = time.time()
        if _json_mode(args):
            payload: Dict[str, Any] = {
                "peers": sorted(muted.peers),
                "groups": sorted(muted.groups),
                "channels": sorted(muted.channels),
            }
            peer_until = {
                key: until
                for key, until in sorted(muted.peer_until.items())
                if key in muted.peers
            }
            if peer_until:
                payload["peer_until"] = peer_until
            group_until = {
                key: until
                for key, until in sorted(muted.group_until.items())
                if key in muted.groups
            }
            if group_until:
                payload["group_until"] = group_until
            channel_until = {
                key: until
                for key, until in sorted(muted.channel_until.items())
                if key in muted.channels
            }
            if channel_until:
                payload["channel_until"] = channel_until
            _emit(payload, True)
            return _EXIT_OK
        registry = PeerRegistry(config.peers_path)
        store = GroupStore(config.groups_path)
        if not muted.peers and not muted.groups and not muted.channels:
            print("No muted conversations.")
            return _EXIT_OK
        for destination in sorted(muted.peers):
            peer = registry.get(destination)
            name = peer.display_name if peer is not None else "unknown"
            until = muted.muted_until_peer(destination, now=now)
            suffix = (
                f" — muted for {_ago(until - now)} more"
                if until is not None
                else ""
            )
            print(f"{name} ({short_hash(destination)}){suffix}")
        for group_id in sorted(muted.groups):
            try:
                group = store.find(group_id)
                label = f"group {group['name']} ({short_hash(group_id)})"
            except (ValueError, KeyError):
                label = f"group {short_hash(group_id)}"
            until = muted.muted_until_group(group_id, now=now)
            suffix = (
                f" — muted for {_ago(until - now)} more"
                if until is not None
                else ""
            )
            print(f"{label}{suffix}")
        for channel_name in sorted(muted.channels):
            until = muted.muted_until_channel(channel_name, now=now)
            suffix = (
                f" — muted for {_ago(until - now)} more"
                if until is not None
                else ""
            )
            print(f"channel #{channel_name}{suffix}")
        return _EXIT_OK
    if channel is not None:
        # A channel mute is meaningless for a channel whose messages
        # are not stored here, so muting requires membership — like
        # a group mute requires the group to exist.
        try:
            name = clean_channel_name(channel)
        except ChannelError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        app = ChannelApp(config, _tan_identity(config), name=config.name)
        if name not in {m["channel"] for m in app.memberships()}:
            print(f"error: not a member of #{name} — join it first", file=sys.stderr)
            return _EXIT_ERROR
        key = name
        label = f"channel #{name}"
        is_group = False
        is_channel = True
    elif args.group is not None:
        store = GroupStore(config.groups_path)
        group = store.find(args.group)
        key = group["id"]
        # The whole rename lineage moves together, like the bridge.
        lineage = group_lineage(group)
        label = f"group {group['name']} ({short_hash(key)})"
        is_group = True
        is_channel = False
    else:
        inbox = Inbox(config.inbox_path)
        registry = PeerRegistry(config.peers_path)
        key = _resolve_inbox_peer(inbox, registry, args.peer)
        peer = registry.get(key)
        name = peer.display_name if peer is not None else "unknown"
        label = f"conversation with {name} ({short_hash(key)})"
        is_group = False
        is_channel = False
    seconds = None
    until = None
    if for_duration is not None:
        try:
            seconds = _parse_duration(for_duration, flag="--for")
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        until = time.time() + seconds
    # Reload inside the lock: two mute commands racing must not
    # discard each other's change to a different conversation.
    with muted_lock(config.muted_path):
        try:
            muted = load_muted(config.muted_path)
        except MutedError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        if is_group:
            previous = muted.is_muted_group_lineage(lineage)
            previous_until = muted.muted_until_group_lineage(lineage)
            if getattr(args, "off", False):
                updated = muted.without_group_lineage(lineage)
            else:
                updated = muted.with_group_lineage(lineage, until=until)
        elif is_channel:
            previous = muted.is_muted_channel(key)
            previous_until = muted.muted_until_channel(key)
            if getattr(args, "off", False):
                updated = muted.without_channel(key)
            else:
                updated = muted.with_channel(key, until=until)
        else:
            previous = muted.is_muted_peer(key)
            previous_until = muted.muted_until_peer(key)
            if getattr(args, "off", False):
                updated = muted.without_peer(key)
            else:
                updated = muted.with_peer(key, until=until)
        # Compare whole objects: re-muting with a different expiry is
        # a change even when the conversation was already muted.
        changed = updated != muted
        if changed:
            save_muted(config.muted_path, updated)
    current = not getattr(args, "off", False)
    # The expiry of the state just saved: a plain re-mute clears a
    # timed mute back to indefinite, so read it off `updated`, not
    # off the previous state.
    if is_group:
        effective_until = updated.muted_until_group_lineage(lineage)
    elif is_channel:
        effective_until = updated.muted_until_channel(key)
    else:
        effective_until = updated.muted_until_peer(key)
    if _json_mode(args):
        _emit(
            {
                "target": key,
                "group": is_group,
                "channel": is_channel,
                "muted": current,
                "changed": changed,
                "until": effective_until,
            },
            True,
        )
        return _EXIT_OK
    if getattr(args, "off", False):
        if previous:
            print(f"Unmuted {label}: its messages count as unread again.")
        else:
            print(f"{label} was not muted.")
    elif previous and not changed:
        if previous_until is not None:
            print(
                f"{label} is already muted for "
                f"{_ago(previous_until - time.time())} more; pass --for to "
                "change it, or --off to unmute."
            )
        else:
            print(f"{label} is already muted.")
    elif seconds is not None:
        print(
            f"Muted {label} for {_describe_duration(seconds)}: its messages "
            "still arrive, but never light the unread badge until then."
        )
    else:
        print(
            f"Muted {label}: its messages still arrive, but never light "
            "the unread badge."
        )
    return _EXIT_OK


def _quiet_hours_payload(
    quiet_hours: Optional[QuietHours], now: Optional[float] = None
) -> Dict[str, Any]:
    """A quiet-hours window as JSON: the window, and whether it is quiet now."""
    now = time.time() if now is None else now
    if quiet_hours is None:
        return {
            "enabled": False,
            "start": None,
            "end": None,
            "quiet_now": False,
            "quiet_minutes_left": None,
        }
    remaining = quiet_hours.minutes_until_end(now)
    return {
        "enabled": quiet_hours.enabled,
        "start": format_time(quiet_hours.start),
        "end": format_time(quiet_hours.end),
        "quiet_now": quiet_hours.is_quiet(now),
        "quiet_minutes_left": remaining,
    }


def _quiet_hours_line(
    quiet_hours: QuietHours, now: Optional[float] = None
) -> str:
    """One human-readable line for a quiet-hours window."""
    now = time.time() if now is None else now
    window = f"{format_time(quiet_hours.start)}–{format_time(quiet_hours.end)}"
    if not quiet_hours.enabled:
        return f"Quiet hours {window} are off."
    remaining = quiet_hours.minutes_until_end(now)
    if remaining is not None:
        return (
            f"Quiet hours {window} — quiet now, "
            f"{_ago(remaining * 60)} left."
        )
    return f"Quiet hours {window} — not quiet now."


def cmd_quiet_hours_set(args: argparse.Namespace) -> int:
    config = _make_config(args)
    try:
        window = QuietHours.from_dict(
            {"start": args.start, "end": args.end}
        )
    except QuietHoursError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_ERROR
    # Reload inside the lock: a corrupt file fails loudly instead of
    # being silently overwritten, like the mute command.
    with quiet_hours_lock(config.quiet_hours_path):
        try:
            load_quiet_hours(config.quiet_hours_path)
        except QuietHoursError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        save_quiet_hours(config.quiet_hours_path, window)
    if _json_mode(args):
        _emit(_quiet_hours_payload(window), True)
        return _EXIT_OK
    print(f"Quiet hours set: {format_time(window.start)}–{format_time(window.end)} every day.")
    if window.is_quiet():
        print("It is quiet now — the unread badge stays dark until the window ends.")
    return _EXIT_OK


def cmd_quiet_hours_off(args: argparse.Namespace) -> int:
    config = _make_config(args)
    path = Path(config.quiet_hours_path)
    if not path.exists():
        if _json_mode(args):
            _emit(_quiet_hours_payload(None), True)
            return _EXIT_OK
        print("No quiet hours set.")
        return _EXIT_OK
    with quiet_hours_lock(config.quiet_hours_path):
        try:
            current = load_quiet_hours(config.quiet_hours_path)
        except QuietHoursError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        updated = QuietHours(
            enabled=False, start=current.start, end=current.end
        )
        save_quiet_hours(config.quiet_hours_path, updated)
    if _json_mode(args):
        _emit(_quiet_hours_payload(updated), True)
        return _EXIT_OK
    print(
        f"Quiet hours off (window {format_time(updated.start)}–"
        f"{format_time(updated.end)} kept)."
    )
    return _EXIT_OK


def cmd_away_on(args: argparse.Namespace) -> int:
    config = _make_config(args)
    # Reload inside the lock: two away commands racing must not
    # discard each other's change.
    with away_lock(config.away_path):
        try:
            load_away(config.away_path)
        except AwayError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        try:
            state = AwayState.on(getattr(args, "message", None))
        except AwayError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        save_away(config.away_path, state)
    if _json_mode(args):
        _emit(
            {
                "enabled": True,
                "message": state.message,
                "since": state.since,
            },
            True,
        )
        return _EXIT_OK
    print(
        "Away reply on: the first DM from each peer gets one reply:\n"
        f"{state.message}"
    )
    return _EXIT_OK


def cmd_quiet_hours_status(args: argparse.Namespace) -> int:
    config = _make_config(args)
    path = Path(config.quiet_hours_path)
    if not path.exists():
        if _json_mode(args):
            _emit(_quiet_hours_payload(None), True)
            return _EXIT_OK
        print("No quiet hours set.")
        return _EXIT_OK
    try:
        current = load_quiet_hours(config.quiet_hours_path)
    except QuietHoursError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_ERROR
    if _json_mode(args):
        _emit(_quiet_hours_payload(current), True)
        return _EXIT_OK
    print(_quiet_hours_line(current))
    return _EXIT_OK


def cmd_away_off(args: argparse.Namespace) -> int:
    config = _make_config(args)
    with away_lock(config.away_path):
        try:
            current = load_away(config.away_path)
        except AwayError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        was_on = current.enabled
        if was_on:
            save_away(config.away_path, current.off())
    if _json_mode(args):
        _emit(
            {"enabled": False, "message": current.message},
            True,
        )
        return _EXIT_OK
    if was_on:
        print("Away reply off.")
    else:
        print("Away reply is already off.")
    return _EXIT_OK


def cmd_away_status(args: argparse.Namespace) -> int:
    config = _make_config(args)
    try:
        state = load_away(config.away_path)
    except AwayError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_ERROR
    if _json_mode(args):
        _emit(
            {
                "enabled": state.enabled,
                "message": state.message,
                "since": state.since,
                "replied_to": sorted(state.replied_to),
            },
            True,
        )
        return _EXIT_OK
    if not state.enabled:
        print("Away reply is off.")
    else:
        print(f"Away reply is on:\n{state.message}")
        if state.replied_to:
            print(
                f"{len(state.replied_to)} peer(s) answered in this away period."
            )
    return _EXIT_OK


def _beacon_target(config: TauConfig, args: argparse.Namespace) -> Tuple[str, bool, str]:
    """Resolve a beacon stop/update target locally: hash, group?, label.

    Local-only, like the mute command: a peer resolves from the
    inbox and registry, a group from the group store.  Starting a
    beacon resolves on the node instead, with discovery, so a peer
    never seen before can still get one.
    """
    if getattr(args, "group", None) is not None:
        store = GroupStore(config.groups_path)
        group = store.find(args.group)
        return (
            group["id"],
            True,
            f"group {group['name']} ({short_hash(group['id'])})",
        )
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    key = _resolve_inbox_peer(inbox, registry, args.peer, direction=None)
    peer = registry.get(key)
    name = peer.display_name if peer is not None else "unknown"
    return key, False, f"conversation with {name} ({short_hash(key)})"


def _beacon_summary(beacon: Any, now: Optional[float] = None) -> Dict[str, Any]:
    """A beacon as JSON: its schedule, position and live state."""
    now = time.time() if now is None else now
    return {
        "id": beacon.id,
        "target": beacon.target,
        "target_kind": beacon.target_kind,
        "target_name": beacon.target_name,
        "started_at": beacon.started_at,
        "expires_at": beacon.expires_at,
        "interval_seconds": beacon.interval,
        "coarse": beacon.coarse,
        "location": dict(beacon.location),
        "location_file": beacon.location_file,
        "next_update_at": beacon.next_update_at,
        "updates_sent": beacon.updates_sent,
        "stopped": beacon.stopped,
        "active": beacon.is_active(now),
        "expires_in_seconds": max(0.0, beacon.expires_at - now),
    }


def _beacon_line(beacon: Any, now: Optional[float] = None) -> str:
    """One human-readable line for a beacon."""
    now = time.time() if now is None else now
    target = beacon.target_name or short_hash(beacon.target)
    kind = "group" if beacon.target_kind == "group" else "peer"
    place = location_default_text(beacon.location)
    coarse = " (coarse)" if beacon.coarse else ""
    if beacon.is_active(now):
        state = (
            f"expires in {_ago(beacon.expires_at - now)}, "
            f"next update in {_ago(beacon.next_update_at - now)}"
        )
    elif beacon.stopped:
        state = "stopped"
    else:
        state = "expired"
    return (
        f"{target} ({kind}) — {place}{coarse}: "
        f"{beacon.updates_sent} update(s) sent, {state}"
    )


def cmd_beacon_start(args: argparse.Namespace) -> int:
    config = _make_config(args)
    location = _location_from_args(args)
    if location is None:  # --location is required by the parser
        raise NodeError("--location is required to start a beacon")
    location_file = getattr(args, "location_file", None)
    if location_file is not None and read_location_file(location_file) is None:
        raise NodeError(
            f"location file {location_file} has no valid position"
        )
    duration = float(getattr(args, "duration_mins", 60.0)) * 60.0
    interval = float(getattr(args, "interval_mins", 5.0)) * 60.0
    if not 0 < duration <= BEACON_MAX_DURATION:
        raise NodeError(
            "beacon duration must be between 1 and "
            f"{int(BEACON_MAX_DURATION // 60)} minutes"
        )
    if not BEACON_MIN_INTERVAL <= interval <= BEACON_MAX_INTERVAL:
        raise NodeError(
            "beacon interval must be between "
            f"{int(BEACON_MIN_INTERVAL // 60)} and "
            f"{int(BEACON_MAX_INTERVAL // 60)} minutes"
        )
    is_group = getattr(args, "group", None) is not None
    target = args.group if is_group else args.peer
    coarse = bool(getattr(args, "coarse", False))
    # A serving node owns the data directory: start through it instead
    # of starting a second Reticulum instance on the same identity.
    if _send_routes_via_running_node(config):
        from .local_send import send_running

        result = send_running(
            config.data_dir,
            op="beacon_start",
            target=target,
            group=is_group,
            location=location,
            location_file=location_file,
            coarse=coarse,
            duration=duration,
            interval=interval,
        )
        summary = result.get("beacon") or {}
        update = result.get("update") or {}
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            beacon = node.start_beacon(
                target,
                location,
                group=is_group,
                coarse=coarse,
                duration=duration,
                interval=interval,
                location_file=location_file,
            )
            update = node.send_beacon_update(beacon)
            summary = _beacon_summary(beacon)
        finally:
            node.stop()
    if _json_mode(args):
        _emit({"beacon": summary, "update": update}, True)
        return _EXIT_OK
    label = summary.get("target_name") or short_hash(
        str(summary.get("target") or "")
    )
    kind = "group" if summary.get("target_kind") == "group" else "peer"
    outcome = (
        "queued" if update.get("queued") else "sent"
    )
    print(
        f"Started a live beacon for {label} ({kind}): updates every "
        f"{_ago(float(summary.get('interval_seconds') or BEACON_UPDATE_INTERVAL))} "
        f"until it expires in "
        f"{_ago(float(summary.get('expires_in_seconds') or 0))} — "
        f"first update {outcome}."
    )
    if not _send_routes_via_running_node(config):
        print(
            "Keep `tau serve` running: later updates go out from the "
            "serving node."
        )
    return _EXIT_OK


def cmd_beacon_stop(args: argparse.Namespace) -> int:
    config = _make_config(args)
    key, is_group, label = _beacon_target(config, args)
    kind = "group" if is_group else "dm"
    with beacons_lock(config.beacons_path):
        beacons = load_beacons(config.beacons_path)
        stopped = beacons.stop(key, kind)
        if stopped is None:
            if _json_mode(args):
                _emit({"target": key, "group": is_group, "stopped": False}, True)
                return _EXIT_OK
            print(f"No beacon is running for {label}.")
            return _EXIT_OK
        save_beacons(config.beacons_path, beacons)
        summary = _beacon_summary(stopped)
    if _json_mode(args):
        _emit({"target": key, "group": is_group, "stopped": True, "beacon": summary}, True)
        return _EXIT_OK
    print(
        f"Stopped the live beacon for {label} after "
        f"{stopped.updates_sent} update(s)."
    )
    return _EXIT_OK


def cmd_beacon_update(args: argparse.Namespace) -> int:
    config = _make_config(args)
    location = _location_from_args(args)
    location_file = getattr(args, "location_file", None)
    if location is None and location_file is None:
        print(
            "error: --location or --location-file is required",
            file=sys.stderr,
        )
        return _EXIT_ERROR
    if location_file is not None and read_location_file(location_file) is None:
        raise NodeError(
            f"location file {location_file} has no valid position"
        )
    key, is_group, label = _beacon_target(config, args)
    kind = "group" if is_group else "dm"
    with beacons_lock(config.beacons_path):
        beacons = load_beacons(config.beacons_path)
        beacon = beacons.update_location(
            key, kind, location=location, location_file=location_file
        )
        save_beacons(config.beacons_path, beacons)
        summary = _beacon_summary(beacon)
    if _json_mode(args):
        _emit({"target": key, "group": is_group, "beacon": summary}, True)
        return _EXIT_OK
    place = location_default_text(beacon.location)
    source = (
        f"position file {location_file}"
        if location_file is not None
        else place
    )
    print(
        f"Beacon for {label} now uses {source}; "
        f"next update in {_ago(beacon.next_update_at - time.time())}."
    )
    return _EXIT_OK


def cmd_beacon_list(args: argparse.Namespace) -> int:
    config = _make_config(args)
    beacons = load_beacons(config.beacons_path)
    now = time.time()
    active = beacons.active(now)
    ended = sorted(
        (b for b in beacons.beacons.values() if not b.is_active(now)),
        key=lambda b: b.started_at,
        reverse=True,
    )[:5]
    if _json_mode(args):
        _emit(
            {
                "beacons": [_beacon_summary(b, now) for b in active],
                "ended": [_beacon_summary(b, now) for b in ended],
            },
            True,
        )
        return _EXIT_OK
    if not active and not ended:
        print("No beacons.")
        return _EXIT_OK
    for beacon in active:
        print(_beacon_line(beacon, now))
    for beacon in ended:
        print(f"{_beacon_line(beacon, now)} (ended)")
    return _EXIT_OK


def cmd_archived(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    archived = load_archived_set(config.archived_path)
    counts: Dict[str, int] = {}
    for entry in inbox.list_messages(limit=None):
        destination = str(entry.get("destination") or "")
        # Group posts are never part of an archived conversation.
        if entry.get("kind") != "group" and destination in archived:
            counts[destination] = counts.get(destination, 0) + 1

    def describe(destination: str) -> Dict[str, Any]:
        peer = registry.get(destination)
        return {
            "destination": destination,
            "name": peer.display_name if peer is not None else "",
            "messages": counts.get(destination, 0),
        }

    entries = [describe(destination) for destination in sorted(archived)]
    if _json_mode(args):
        _emit({"archived": entries, "count": len(entries)}, True)
        return _EXIT_OK
    if not entries:
        print("No archived conversations — hide one with `tau archive PEER`.")
        return _EXIT_OK
    for entry in entries:
        name = entry["name"] or "unknown"
        print(
            f"{name} ({short_hash(entry['destination'])}): "
            f"{entry['messages']} message(s)"
        )
    return _EXIT_OK


def _describe_group(
    group: Dict[str, Any], registry: PeerRegistry
) -> Dict[str, Any]:
    """A group record with member display names for output."""
    members = []
    for member_hash in group["members"]:
        peer = registry.get(member_hash)
        members.append(
            {
                "hash": member_hash,
                "name": peer.display_name if peer is not None else member_hash[:8],
            }
        )
    return {
        "id": group["id"],
        "name": group["name"],
        "members": members,
        "created_at": group["created_at"],
        "created_by": group["created_by"],
        "topic": group.get("topic", ""),
        "announcements_only": bool(group.get("announcements_only", False)),
        "secret": bool(group.get("secret", False)),
    }


def cmd_group_create(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        own_hash = node.destination_hexhash
        members: List[str] = [own_hash]
        for target in getattr(args, "peer", []):
            peer = node.resolve_peer(
                target,
                timeout=float(getattr(args, "discover_timeout", 10.0)),
            )
            if peer.hexhash == own_hash:
                raise NodeError("the creator is already a member; drop --peer self")
            if peer.hexhash not in members:
                members.append(peer.hexhash)
        if len(members) < 2:
            raise NodeError("a group needs at least one other peer")
        group = node.groups.create(
            getattr(args, "group_name"),
            members,
            created_by=own_hash,
            secret=bool(getattr(args, "secret", False)),
            identity=getattr(node, "identity", None),
        )
    finally:
        node.stop()

    described = _describe_group(group, node.registry)
    if _json_mode(args):
        _emit({"group": described}, True)
        return _EXIT_OK
    print(f"created group {group['name']} ({group['id'][:8]})")
    for member in described["members"]:
        print(f"  {member['name']} ({member['hash'][:8]})")
    print("Members learn the group from the first message sent to it.")
    return _EXIT_OK


def cmd_group_list(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    registry = PeerRegistry(config.peers_path)
    ignored = load_ignored_group_ids(config.ignored_groups_path)
    records = store.list_groups()
    # A group reads as pinned when any id in its rename lineage is
    # pinned, like a mute — the pin follows the conversation, not the id.
    pinned_ids = load_pinned_set(config.pinned_path, "group")
    pinned = {
        record["id"]: bool(group_lineage(record) & pinned_ids)
        for record in records
    }
    groups = [_describe_group(group, registry) for group in records]
    for group in groups:
        group["ignored"] = group["id"] in ignored
        group["pinned"] = pinned.get(group["id"], False)
    groups = sorted(groups, key=lambda group: not group["pinned"])
    if _json_mode(args):
        _emit({"groups": groups, "count": len(groups)}, True)
        return _EXIT_OK
    if not groups:
        print("No groups yet — create one with `tau group create NAME --peer ...`.")
        return _EXIT_OK
    for group in groups:
        names = ", ".join(member["name"] for member in group["members"])
        ignored_suffix = " [ignored]" if group["ignored"] else ""
        pinned_suffix = " [pinned]" if group["pinned"] else ""
        announcements_suffix = (
            " [announcements-only]" if group.get("announcements_only") else ""
        )
        print(f"{group['name']} ({group['id'][:8]}) — {names}{ignored_suffix}{pinned_suffix}{announcements_suffix}")
        if group.get("topic"):
            print(f"  topic: {group['topic']}")
    return _EXIT_OK


def cmd_group_show(args: argparse.Namespace) -> int:
    """Show one group's full details: creator, members, bans, flags.

    Read-only: the group store and peer registry are read from disk,
    no node is started and nothing is sent.
    """
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    try:
        group = store.find(args.group)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    registry = PeerRegistry(config.peers_path)
    described = _describe_group(group, registry)
    # Legacy groups carry no creator (records written before the field
    # existed normalize it to ""): they read as unknown, never empty.
    creator_hash = group.get("created_by") or ""
    creator = registry.get(creator_hash) if creator_hash else None
    creator_name = (
        creator.display_name
        if creator is not None
        else (creator_hash[:8] if creator_hash else "unknown")
    )
    banned = _describe_banned(group, registry)
    ignored = group["id"] in load_ignored_group_ids(config.ignored_groups_path)
    # A group reads as pinned when any id in its rename lineage is
    # pinned, like a mute — the pin follows the conversation, not the id.
    pinned = bool(group_lineage(group) & load_pinned_set(config.pinned_path, "group"))
    if _json_mode(args):
        payload = dict(described)
        payload["created_by_name"] = creator_name
        payload["banned"] = banned
        payload["ignored"] = ignored
        payload["pinned"] = pinned
        _emit({"group": payload}, True)
        return _EXIT_OK
    try:
        created = datetime.fromtimestamp(float(group["created_at"])).strftime(
            "%Y-%m-%d %H:%M"
        )
    except (OverflowError, OSError, ValueError):
        # Non-finite or out-of-range stored timestamps cannot be formatted.
        created = "unknown"
    print(f"{group['name']} ({group['id']})")
    if creator_hash:
        print(f"  created: {created} by {creator_name} ({creator_hash[:8]})")
    else:
        print(f"  created: {created} by {creator_name}")
    if group.get("topic"):
        print(f"  topic: {group['topic']}")
    print(f"  announcements-only: {'yes' if group.get('announcements_only') else 'no'}")
    print(f"  members ({len(described['members'])}):")
    for member in described["members"]:
        creator_suffix = (
            " [creator]" if creator_hash and member["hash"] == creator_hash else ""
        )
        print(f"    {member['name']} ({member['hash'][:8]}){creator_suffix}")
    if banned:
        print(f"  banned ({len(banned)}):")
        for entry in banned:
            print(f"    {entry['name']} ({entry['hash'][:8]})")
    else:
        print("  banned: none")
    print(f"  ignored: {'yes' if ignored else 'no'}")
    print(f"  pinned: {'yes' if pinned else 'no'}")
    return _EXIT_OK


def cmd_group_delete(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    group = store.find(args.group)
    if not store.delete(group["id"]):
        raise NodeError(f"unknown group: {args.group!r}")
    if _json_mode(args):
        _emit({"deleted": group["id"], "name": group["name"]}, True)
        return _EXIT_OK
    print(f"deleted group {group['name']} ({group['id'][:8]})")
    return _EXIT_OK


def cmd_group_leave(args: argparse.Namespace) -> int:
    """Leave a group this node joined: drop it and refuse to re-pin it.

    The creator steps out with ``tau group delete`` instead — a group
    this node created cannot be re-pinned by another member's message
    the way a learned group can, so delete is durable for the creator.
    Leaving is local only: the creator is not told and keeps the member
    on the roster, so the messages keep coming and are rejected here.
    """
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    try:
        group = store.find(args.group)
    except ValueError:
        # Leaving twice is not an error: a full id already on the left
        # list reports as already left instead of unknown.
        query = args.group.strip().lower()
        if len(query) == 32 and store.is_left(query):
            if _json_mode(args):
                _emit({"left": query, "already_left": True}, True)
                return _EXIT_OK
            print(f"already left group {query[:8]}")
            return _EXIT_OK
        raise
    if group["local"]:
        # The `local` flag is the durable marker of a group this node
        # created — it survives even if the identity file is gone, and
        # it cannot be forged by a member claiming this node as the
        # creator of a learned group.
        raise NodeError(
            "only members can leave a group they did not create —"
            " delete your own group with `tau group delete`"
        )
    left_ids = store.leave(group["id"])
    if not left_ids:
        raise NodeError(f"unknown group: {args.group!r}")
    siblings = [gid for gid in left_ids if gid != group["id"]]
    if _json_mode(args):
        payload: Dict[str, Any] = {"left": group["id"], "name": group["name"]}
        if siblings:
            payload["also_left"] = siblings
        _emit(payload, True)
        return _EXIT_OK
    print(f"left group {group['name']} ({group['id'][:8]})")
    if siblings:
        print(f"also left {len(siblings)} related roster(s) for the same group")
    print("Future messages for it will be rejected; rejoin with `tau group rejoin <id>`.")
    return _EXIT_OK


def cmd_group_rejoin(args: argparse.Namespace) -> int:
    """Undo a leave, so the next group message pins the group again."""
    config = _make_config(args)
    group_id = args.group.strip().lower()
    valid = len(group_id) == 32
    if valid:
        try:
            bytes.fromhex(group_id)
        except ValueError:
            valid = False
    if not valid:
        raise NodeError(f"invalid group id: {args.group!r}")
    store = GroupStore(config.groups_path)
    if not store.rejoin(group_id):
        raise NodeError(f"group {group_id} was never left")
    if _json_mode(args):
        _emit({"rejoined": group_id}, True)
        return _EXIT_OK
    print(f"rejoined group {group_id[:8]} — the next message will pin it again")
    return _EXIT_OK


def cmd_group_left(args: argparse.Namespace) -> int:
    """List groups this node has left."""
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    entries = store.left_groups()
    if _json_mode(args):
        _emit({"left": entries, "count": len(entries)}, True)
        return _EXIT_OK
    if not entries:
        print("No left groups.")
        return _EXIT_OK
    for entry in entries:
        left_at = time.strftime(
            "%Y-%m-%d %H:%M", time.localtime(entry["left_at"])
        )
        # The full id: `tau group rejoin` needs all 32 characters.
        print(f"{entry['id']} — left {left_at}")
    return _EXIT_OK


def cmd_group_ignore(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    group = store.find(args.group)
    group_id = group["id"]
    # Reload inside the lock: two ignore commands racing must not
    # discard each other's change to a different group.
    with ignored_groups_lock(config.ignored_groups_path):
        try:
            ignored = load_ignored_groups(config.ignored_groups_path)
        except IgnoredGroupsError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        already = ignored.is_ignored(group_id)
        if not already:
            ignored = ignored.with_added(group_id)
            save_ignored_groups(config.ignored_groups_path, ignored)
    if _json_mode(args):
        _emit(
            {
                "group": group_id,
                "name": group["name"],
                "ignored": True,
                "changed": not already,
            },
            True,
        )
        return _EXIT_OK
    if already:
        print(
            f"Group {group['name']} ({group_id[:8]}) is already ignored."
        )
    else:
        print(
            f"Ignored group {group['name']} ({group_id[:8]}). "
            "Its messages keep arriving; `tau group unignore` restores the view."
        )
    return _EXIT_OK


def _stored_group_matches(store: GroupStore, query: str) -> bool:
    """True when any stored group matches *query* by id, id prefix, or name.

    Mirrors :meth:`tau.groups.GroupStore.find`: a full id matches by
    lookup, a shorter query by exact (case-insensitive) name or by an
    id prefix of at least four characters.  Used to tell an ambiguous
    lookup apart from one that found nothing at all.
    """
    needle = query.strip()
    if len(needle) == 32:
        return store.get(needle) is not None
    lowered = needle.lower()
    for group in store.list_groups():
        if group["name"].casefold() == needle.casefold():
            return True
        if len(needle) >= 4 and group["id"].startswith(lowered):
            return True
    return False


def _ignored_id_for_query(ignored: IgnoredGroups, query: str) -> Optional[str]:
    """The ignored id *query* names when no stored group record does.

    Accepts a full 32-character id or a unique id prefix of at least
    four characters, as ``GroupStore.find`` does.  Returns ``None``
    when nothing matches; raises :class:`ValueError` when a prefix
    matches several ignored ids.
    """
    needle = query.strip().lower()
    if len(needle) == 32:
        return needle if ignored.is_ignored(needle) else None
    if len(needle) < 4:
        return None
    matches = sorted(group_id for group_id in ignored.ignored if group_id.startswith(needle))
    if len(matches) > 1:
        raise ValueError(f"{len(matches)} ignored groups match the id prefix {query!r}")
    return matches[0] if matches else None


def cmd_group_unignore(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    # The group record can be gone (deleted, or a backup restored the
    # ignore list without groups.json).  The id must still be removable,
    # or its posts stay hidden with no way out.
    find_error: Optional[ValueError] = None
    try:
        group = store.find(args.group)
    except ValueError as exc:
        group = None
        find_error = exc
    # Reload inside the lock: two ignore commands racing must not
    # discard each other's change to a different group.
    with ignored_groups_lock(config.ignored_groups_path):
        try:
            ignored = load_ignored_groups(config.ignored_groups_path)
        except IgnoredGroupsError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        if group is not None:
            group_id = group["id"]
            name = group["name"]
        else:
            # The orphan fallback is only for a lookup that found no
            # stored group at all: an ambiguous name or id prefix must
            # still be rejected, not silently resolved against an
            # orphaned ignored id that happens to share the prefix.
            if _stored_group_matches(store, args.group):
                raise find_error
            try:
                resolved = _ignored_id_for_query(ignored, args.group)
            except ValueError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return _EXIT_ERROR
            if resolved is None:
                raise find_error
            group_id = resolved
            name = ""
        was_ignored = ignored.is_ignored(group_id)
        if was_ignored:
            ignored = ignored.with_removed(group_id)
            save_ignored_groups(config.ignored_groups_path, ignored)
    if _json_mode(args):
        _emit(
            {
                "group": group_id,
                "name": name,
                "ignored": False,
                "changed": was_ignored,
            },
            True,
        )
        return _EXIT_OK
    label = name or "unknown"
    if was_ignored:
        print(
            f"Restored group {label} ({group_id[:8]}) to `tau messages`."
        )
    else:
        print(f"Group {label} ({group_id[:8]}) was not ignored.")
    return _EXIT_OK


def cmd_group_ignored(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    inbox = Inbox(config.inbox_path)
    ignored = load_ignored_group_ids(config.ignored_groups_path)
    counts: Dict[str, int] = {}
    for entry in inbox.list_messages(limit=None):
        # Only group posts belong to an ignored group; a DM id never
        # collides with a group id, but the kind check keeps the count
        # honest if a future kind reuses the field.
        if entry.get("kind") == "group" and str(entry.get("group") or "") in ignored:
            group_id = str(entry.get("group"))
            counts[group_id] = counts.get(group_id, 0) + 1

    def describe(group_id: str) -> Dict[str, Any]:
        group = store.get(group_id)
        return {
            "id": group_id,
            "name": group["name"] if group is not None else "",
            "messages": counts.get(group_id, 0),
        }

    entries = [describe(group_id) for group_id in sorted(ignored)]
    if _json_mode(args):
        _emit({"ignored": entries, "count": len(entries)}, True)
        return _EXIT_OK
    if not entries:
        print("No ignored groups — hide one with `tau group ignore GROUP`.")
        return _EXIT_OK
    for entry in entries:
        name = entry["name"] or "unknown"
        print(
            f"{name} ({entry['id'][:8]}): "
            f"{entry['messages']} message(s)"
        )
    return _EXIT_OK


def _group_ids_spanning_renames(
    store: "GroupStore", group: Dict[str, Any]
) -> List[str]:
    """This group's id plus its verified rename predecessors, for history lookups.

    The store only keeps a predecessor whose live or retired record
    shares this group's creator, so a claimed id from another creator
    never widens the lookup.  A store read that fails leaves the group
    standing alone rather than trusting the raw claim.
    """
    try:
        return store.lineage(group["id"])
    except Exception:  # pragma: no cover - defensive
        return [group["id"]]


def cmd_group_pin(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    group = store.find(args.group)
    group_ids = _group_ids_spanning_renames(store, group)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    entry = _resolve_group_pin_target(inbox, group_ids, args.message)
    try:
        pinned, changed = inbox.pin(str(entry["id"]), groups=group_ids)
    except InboxError as exc:
        raise NodeError(str(exc)) from exc
    if _json_mode(args):
        _emit(
            {"pinned": pinned, "already_pinned": not changed, "group": group["id"]},
            True,
        )
        return _EXIT_OK
    short = _safe_id_prefix(str(pinned.get("id") or ""))
    when = _format_message_time(float(pinned.get("received_at", 0)))
    state = "pinned" if changed else "already pinned"
    print(
        f"{state} [{short}] {_message_peer_label(registry, pinned)}"
        f" ({when}) in group \"{group['name']}\""
    )
    if changed:
        print(
            "note: pins are local-only; the members' copies are unaffected"
            " and the pin is never sent."
        )
    return _EXIT_OK


def cmd_group_unpin(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    group = store.find(args.group)
    group_ids = _group_ids_spanning_renames(store, group)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    entry = _resolve_group_pin_target(inbox, group_ids, args.message)
    try:
        unpinned, changed = inbox.unpin(str(entry["id"]), groups=group_ids)
    except InboxError as exc:
        raise NodeError(str(exc)) from exc
    if _json_mode(args):
        _emit(
            {
                "unpinned": unpinned,
                "was_pinned": changed,
                "group": group["id"],
            },
            True,
        )
        return _EXIT_OK
    short = _safe_id_prefix(str(unpinned.get("id") or ""))
    when = _format_message_time(float(unpinned.get("received_at", 0)))
    state = "unpinned" if changed else "was not pinned"
    print(
        f"{state} [{short}] {_message_peer_label(registry, unpinned)}"
        f" ({when}) in group \"{group['name']}\""
    )
    return _EXIT_OK


def cmd_group_pinned(args: argparse.Namespace) -> int:
    """List pinned messages in one group, received and sent.

    Same listing as ``tau pinned`` scoped to the group: history spans
    renames, blocked senders' received pins stay hidden like there,
    and the listing never advances the read cursor.
    """
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    group = store.find(args.group)
    group_ids = _group_ids_spanning_renames(store, group)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    limit = getattr(args, "limit", 30)
    if limit is not None and limit < 0:
        raise NodeError("--limit must not be negative")
    entries = inbox.list_messages(
        limit=None, group=group_ids, pinned_only=True, direction=None
    )
    blocked = load_blocked_set(config.blocklist_path)
    if blocked is None:
        entries = [entry for entry in entries if entry.get("direction") == "out"]
    elif blocked:
        entries = [
            entry
            for entry in entries
            if entry.get("direction") == "out"
            or entry.get("destination") not in blocked
        ]
    shown = entries if limit is None else entries[:limit]
    if _json_mode(args):
        _emit({"messages": shown, "count": len(shown), "group": group["id"]}, True)
    elif not shown:
        print(f"No pinned messages in group \"{group['name']}\".")
    else:
        own_hash = _own_destination_hash(config)
        for entry in shown:
            _print_message_entry(
                inbox, registry, entry, blocked=blocked, own_hash=own_hash
            )
    return _EXIT_OK


def _roster_hash(target: Any, members: List[str]) -> Optional[str]:
    """A 32-hex target already in the roster, or ``None``."""
    if not isinstance(target, str):
        return None
    cleaned = target.strip().lower()
    if len(cleaned) != 32 or cleaned not in members:
        return None
    try:
        bytes.fromhex(cleaned)
    except ValueError:
        return None
    return cleaned


def _summarize_roster_announcement(
    announcement: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Count a roster-change announcement's per-member results.

    A leg is delivered when it carries ``delivered``, queued when it
    carries ``queued``, and failed otherwise — a failed leg is
    reported, never silently dropped from the counts. All zeros means
    nothing was announced (the send failed, or the roster did not
    change).
    """
    delivered = sum(1 for result in announcement if result.get("delivered"))
    queued = sum(1 for result in announcement if result.get("queued"))
    failed = len(announcement) - delivered - queued
    return {"delivered": delivered, "queued": queued, "failed": failed}


def _print_roster_announcement(
    announcement: List[Dict[str, Any]], *, changed: bool
) -> None:
    """Summarise a roster-change announcement, or fall back to the old note.

    *announcement* is the per-member send results from
    :meth:`tau.node.TauNode.announce_roster_change`; an empty list
    means nothing was announced (the send failed, or the roster did
    not change), so members still learn the new roster from the next
    message when *changed*.
    """
    if announcement:
        counts = _summarize_roster_announcement(announcement)
        summary = (
            f" ({counts['delivered']} delivered, {counts['queued']} queued"
        )
        if counts["failed"]:
            summary += f", {counts['failed']} failed"
        print("Roster change announced to the group" + summary + ").")
    elif changed:
        print("Members learn the new roster from the next message sent to it.")


def _change_group_members(args: argparse.Namespace, *, adding: bool) -> int:
    """Add peers to, or remove peers from, a group this node created.

    The id derives from the roster, so the change is a successor group
    (same name, new id); the store swaps the old record for the new one
    in a single transaction.  The change is then announced to the new
    roster, so every remaining member pins the successor and retires
    the predecessor at once instead of learning it from the next
    message (see #703).
    """
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    announcement: List[Dict[str, Any]] = []
    try:
        own_hash = node.destination_hexhash
        group = node.groups.find(args.group)
        if not group["local"] or group["creator"] != own_hash:
            raise NodeError("only the creator can change a group's members")
        members = list(group["members"])
        peers: List[Any] = []
        for target in getattr(args, "peer", []):
            # Removing a member only needs the hash already in the
            # roster: an offline member may have left the registry,
            # and resolving it would wait out the whole discover
            # timeout.  Adding still resolves, so a mistyped or
            # unknown peer is refused before the roster changes.
            fast = None if adding else _roster_hash(target, members)
            if fast is not None:
                known = node.registry.get(fast)
                peers.append(
                    SimpleNamespace(
                        hexhash=fast,
                        display_name=(
                            known.display_name
                            if known is not None
                            else fast[:8]
                        ),
                    )
                )
            else:
                peers.append(
                    node.resolve_peer(
                        target,
                        timeout=float(getattr(args, "discover_timeout", 10.0)),
                    )
                )
        for peer in peers:
            if adding:
                if peer.hexhash == own_hash:
                    raise NodeError(
                        "the creator is already a member; drop --peer self"
                    )
                if peer.hexhash in members:
                    raise NodeError(
                        f"{peer.display_name} is already a member of"
                        f" {group['name']!r}"
                    )
                members.append(peer.hexhash)
            else:
                if peer.hexhash == own_hash:
                    raise NodeError("cannot remove the creator from the group")
                if peer.hexhash not in members:
                    raise NodeError(
                        f"{peer.display_name} is not a member of"
                        f" {group['name']!r}"
                    )
                members.remove(peer.hexhash)
        new_group = node.groups.update_members(
            group["id"],
            members,
            created_by=own_hash,
            identity=getattr(node, "identity", None),
        )
        # Announce while the node is still running: the announcement
        # is a group message to the new roster, and a queued leg
        # delivers on retry when a member is unreachable.
        changed_names = [peer.display_name for peer in peers]
        announcement = node.announce_roster_change(
            new_group,
            added=changed_names if adding else (),
            removed=() if adding else changed_names,
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
    finally:
        node.stop()

    described = _describe_group(new_group, node.registry)
    if _json_mode(args):
        _emit(
            {
                "group": described,
                "previous_id": group["id"],
                "announcement": _summarize_roster_announcement(announcement),
            },
            True,
        )
        return _EXIT_OK
    action = "added to" if adding else "removed from"
    names = ", ".join(peer.display_name for peer in peers)
    print(
        f"{names} {action} group {new_group['name']}"
        f" ({group['id'][:8]} -> {new_group['id'][:8]})"
    )
    for member in described["members"]:
        print(f"  {member['name']} ({member['hash'][:8]})")
    _print_roster_announcement(announcement, changed=True)
    return _EXIT_OK


def cmd_group_add_member(args: argparse.Namespace) -> int:
    return _change_group_members(args, adding=True)


def cmd_group_remove_member(args: argparse.Namespace) -> int:
    return _change_group_members(args, adding=False)


def _describe_banned(group: Dict[str, Any], registry: PeerRegistry) -> List[Dict[str, str]]:
    """A group's ban list with display names for output."""
    entries = []
    for banned_hash in group.get("banned") or []:
        peer = registry.get(banned_hash)
        entries.append(
            {
                "hash": banned_hash,
                "name": peer.display_name if peer is not None else banned_hash[:8],
            }
        )
    return entries


def cmd_group_ban(args: argparse.Namespace) -> int:
    """Ban a peer from a group this node created.

    The peer is removed from the roster when they are a member and
    recorded on the ban list either way, so they cannot be re-added
    or approved through a join request afterwards.  Only the creator
    can ban; the ban itself needs a running node only to resolve an
    unknown peer — a member (or an already-banned peer) resolves
    straight from the roster, like a removal.
    """
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        own_hash = node.destination_hexhash
        group = node.groups.find(args.group)
        if not group["local"] or group["creator"] != own_hash:
            raise NodeError("only the creator can ban members of a group")
        members = list(group["members"])
        banned = list(group.get("banned") or [])
        fast = _roster_hash(args.peer, members + banned)
        if fast is not None:
            known = node.registry.get(fast)
            peer = SimpleNamespace(
                hexhash=fast,
                display_name=(
                    known.display_name if known is not None else fast[:8]
                ),
            )
        else:
            peer = node.resolve_peer(
                args.peer,
                timeout=float(getattr(args, "discover_timeout", 10.0)),
            )
        previous_id = group["id"]
        announcement: List[Dict[str, Any]] = []
        try:
            new_group = node.groups.ban(
                group["id"],
                peer.hexhash,
                created_by=own_hash,
                identity=getattr(node, "identity", None),
            )
        except ValueError as exc:
            raise NodeError(str(exc)) from exc
        # A ban that removes a member from the roster rotates the id,
        # so announce it to the new roster while the node is still
        # running (see #703); banning a non-member changes no roster
        # and needs no announcement.
        if new_group["id"] != previous_id:
            announcement = node.announce_roster_change(
                new_group,
                removed=[peer.display_name],
                discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
            )
    finally:
        node.stop()

    described = _describe_group(new_group, node.registry)
    banned_entries = _describe_banned(new_group, node.registry)
    if _json_mode(args):
        _emit(
            {
                "group": described,
                "banned": banned_entries,
                "previous_id": previous_id,
                "announcement": _summarize_roster_announcement(announcement),
            },
            True,
        )
        return _EXIT_OK
    if previous_id != new_group["id"]:
        print(
            f"banned {peer.display_name} from group {new_group['name']}"
            f" ({previous_id[:8]} -> {new_group['id'][:8]})"
        )
    else:
        print(
            f"banned {peer.display_name} from group {new_group['name']}"
            f" ({new_group['id'][:8]})"
        )
    print("They cannot be re-added or approved through a join request.")
    print("Lift it with `tau group unban`.")
    _print_roster_announcement(
        announcement, changed=previous_id != new_group["id"]
    )
    return _EXIT_OK


def _resolve_banned_peer(
    target: str, banned: List[str], registry: PeerRegistry
) -> str:
    """Resolve an unban target to a banned hash.

    A banned hash (full or unique prefix) needs no lookup; anything
    else resolves through the registry, so a name works too.  A bare
    hash for a peer the registry never saw is still usable — the ban
    list is the authority, and unbanning a hash that is not banned
    changes nothing.
    """
    cleaned = target.strip().lower()
    if not cleaned:
        raise NodeError("pass a peer name or destination hash")
    matches = [entry for entry in banned if entry.startswith(cleaned)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise NodeError(
            f"ambiguous peer: {target!r} matches several banned peers;"
            " pass a longer hash prefix"
        )
    try:
        peer = registry.resolve(target)
    except PeerError as exc:
        raise NodeError(str(exc)) from exc
    if peer is not None:
        return peer.destination_hash.hex()
    if len(cleaned) == 32:
        try:
            bytes.fromhex(cleaned)
        except ValueError:
            raise NodeError(f"unknown peer: {target!r}") from None
        return cleaned
    raise NodeError(f"unknown peer: {target!r}")


def cmd_group_unban(args: argparse.Namespace) -> int:
    """Lift a ban on a group this node created.

    Needs no running node: the ban list is local creator state, and
    the target resolves from the ban list or the registry.  The peer
    is not re-added to the roster — they can join again afterwards,
    but only through the normal paths.
    """
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    try:
        group = store.find(args.group)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    own_hash = destination_hash(identity).hex()
    if not group["local"] or group["creator"] != own_hash:
        raise NodeError("only the creator can unban members of a group")
    registry = PeerRegistry(config.peers_path)
    resolved = _resolve_banned_peer(
        args.peer, list(group.get("banned") or []), registry
    )
    was_banned = resolved in (group.get("banned") or [])
    try:
        new_group = store.unban(group["id"], resolved, created_by=own_hash)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    described = _describe_group(new_group, registry)
    peer = registry.get(resolved)
    name = peer.display_name if peer is not None else resolved[:8]
    if _json_mode(args):
        _emit(
            {
                "group": described,
                "unbanned": {"hash": resolved, "name": name},
                "was_banned": was_banned,
            },
            True,
        )
        return _EXIT_OK
    if was_banned:
        print(f"unbanned {name} from group {new_group['name']} ({new_group['id'][:8]})")
    else:
        print(f"{name} was not banned from group {new_group['name']}.")
    return _EXIT_OK


def cmd_group_banned(args: argparse.Namespace) -> int:
    """List the peers banned from a group this node created."""
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    try:
        group = store.find(args.group)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    registry = PeerRegistry(config.peers_path)
    entries = _describe_banned(group, registry)
    if _json_mode(args):
        _emit(
            {
                "group": _describe_group(group, registry),
                "banned": entries,
                "count": len(entries),
            },
            True,
        )
        return _EXIT_OK
    if not entries:
        print(f"No banned peers in {group['name']} ({group['id'][:8]}).")
        return _EXIT_OK
    print(f"Banned from {group['name']} ({group['id'][:8]}):")
    for entry in entries:
        print(f"  {entry['name']} ({entry['hash'][:8]})")
    return _EXIT_OK


def cmd_group_invite(args: argparse.Namespace) -> int:
    """Mint (or revoke) an invite link for a group this node created.

    Minting needs no running node: the token is signed with the
    local identity and the mode/generation are local state until a
    join request arrives.
    """
    if getattr(args, "closed", False) and getattr(args, "open", False):
        raise NodeError("pass --closed or --open, not both")
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    try:
        group = store.find(args.group)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    own_hash = destination_hash(identity).hex()
    if not group["local"] or group["creator"] != own_hash:
        raise NodeError("only the creator can mint invites for a group")
    if getattr(args, "revoke", False):
        generation = revoke_invites(config.invites_path, group_name=group["name"])
        if _json_mode(args):
            _emit(
                {"group": _describe_group(group, PeerRegistry(config.peers_path)),
                 "revoked_generation": generation - 1,
                 "generation": generation},
                True,
            )
            return _EXIT_OK
        print(
            f"revoked every invite for {group['name']} ({group['id'][:8]});"
            " mint a new one to share the group again"
        )
        return _EXIT_OK
    stored = invite_status(config.invites_path, group_name=group["name"])
    if getattr(args, "closed", False):
        mode = "closed"
    elif getattr(args, "open", False):
        mode = "open"
    elif stored is not None:
        mode = stored["mode"]
    else:
        mode = "open"
    link = mint_invite(
        config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode=mode,
        # A secret group's link names the group by its opaque id,
        # never by name — anyone holding it learns nothing about
        # the group.
        group_id=group["id"] if group.get("secret") else None,
        group_created_at=(
            group.get("created_at") if group.get("secret") else None
        ),
    )
    stored = invite_status(config.invites_path, group_name=group["name"])
    if _json_mode(args):
        _emit(
            {
                "group": _describe_group(group, PeerRegistry(config.peers_path)),
                "link": link,
                "mode": mode,
                "generation": stored["generation"] if stored else 0,
            },
            True,
        )
        return _EXIT_OK
    print(f"invite link for {group['name']} ({group['id'][:8]}), {mode} group:")
    print(link)
    if group.get("secret"):
        print("The link names the group by its opaque id, not by name.")
    if mode == "closed":
        print("Invitees ask to join; review them with `tau group requests`.")
    else:
        print("Anyone with this link joins at once.")
    print("Revoke it any time with `tau group invite --revoke`.")
    return _EXIT_OK


def cmd_group_join(args: argparse.Namespace) -> int:
    """Join a group with an invite link.

    Sends the link as a ``group-join-request`` DM to the creator.
    The creator's reply carries the outcome and the roster: an
    open group adds this node at once (the roster is pinned
    locally from the authenticated reply), a closed group leaves
    the request pending, and a queued request is decided when it
    reaches the creator — the welcome group message then pins the
    roster for this node.
    """
    try:
        token = parse_invite_link(args.link)
    except InviteError as exc:
        raise NodeError(str(exc)) from exc
    config = _make_config(args)
    reply_ids: Dict[str, str] = {}
    if _send_routes_via_running_node(config):
        results = _send_via_running_node(
            args,
            config,
            args.link.strip(),
            [token["creator"]],
            None,
            reply_ids,
            kind=JOIN_REQUEST_KIND,
            wait=0.0,
            send_after=None,
        )
    else:
        results = _send_via_local_node(
            args,
            config,
            args.link.strip(),
            [token["creator"]],
            None,
            reply_ids,
            kind=JOIN_REQUEST_KIND,
            wait=0.0,
            send_after=None,
        )
    result = results[0]
    if result.get("queued"):
        if _json_mode(args):
            _emit(
                {"status": "queued", "group": invite_token_group_label(token)},
                True,
            )
            return _EXIT_OK
        print(
            "Join request queued — the creator was unreachable."
            " It is decided when it arrives; the welcome message"
            " then adds the group here."
        )
        return _EXIT_OK
    if not result.get("ok"):
        error = result.get("error") or {}
        message = (
            error.get("message")
            if isinstance(error, dict)
            else str(error) or "join request failed"
        )
        raise NodeError(f"join request failed: {message}")
    status = result.get("status")
    group = result.get("group") if isinstance(result.get("group"), dict) else {}
    if status in ("added", "already_member") and group.get("id"):
        store = GroupStore(config.groups_path)
        store.record(
            group["id"],
            group.get("name", ""),
            list(group.get("members", [])),
            creator=group.get("creator", ""),
            sender=group.get("creator", ""),
            sent_at=time.time(),
            roster_attestation=group.get("roster_attestation"),
        )
        if _json_mode(args):
            pinned = store.get(group["id"])
            _emit(
                {
                    "status": status,
                    "group": _describe_group(pinned, PeerRegistry(config.peers_path))
                    if pinned is not None
                    else group,
                },
                True,
            )
            return _EXIT_OK
        if status == "already_member":
            print(f"already a member of {group.get('name')} ({group['id'][:8]})")
        else:
            print(f"joined group {group.get('name')} ({group['id'][:8]})")
        return _EXIT_OK
    if status == "pending" and group.get("id"):
        if _json_mode(args):
            _emit({"status": "pending", "group": group}, True)
            return _EXIT_OK
        print(
            f"Join request pending: the creator of {group.get('name')}"
            f" ({group['id'][:8]}) has to approve it first."
        )
        return _EXIT_OK
    raise NodeError(f"unexpected join reply: {result}")


def _resolve_join_peer(
    node: TauNode, group_name: str, target: str, *, timeout: float
) -> str:
    """Resolve a join-request peer without waiting on discovery.

    The peer hash is already in the pending request, so a name,
    hash, or hash prefix that names a pending requester resolves
    locally; only an unknown name falls back to peer discovery.
    """
    requests = node.list_join_requests(group_name)
    wanted = target.strip().lower()
    for entry in requests:
        peer_hash = entry["peer"]
        if peer_hash == wanted or peer_hash.startswith(wanted):
            return peer_hash
        peer = node.registry.get(peer_hash)
        if peer is not None and peer.display_name.lower() == wanted:
            return peer_hash
    try:
        known = node.registry.resolve(target)
    except PeerError:
        known = None
    if known is not None:
        return known.hexhash
    return node.resolve_peer(target, timeout=timeout).hexhash


def cmd_group_requests(args: argparse.Namespace) -> int:
    """List pending join requests for groups this node created."""
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        group_name = None
        group = None
        if getattr(args, "group", None):
            group = node.groups.find(args.group)
            group_name = group["name"]
        requests = node.list_join_requests(group_name)
        registry = node.registry
        store = node.groups
    finally:
        node.stop()
    described = []
    for entry in requests:
        peer = registry.get(entry["peer"])
        try:
            group_record = store.find(entry["group"])
        except ValueError:
            group_record = None
        described.append(
            {
                "group": entry["group"],
                "group_id": group_record["id"] if group_record else "",
                "peer": entry["peer"],
                "peer_name": peer.display_name if peer is not None else entry["peer"][:8],
                "requested_at": entry["requested_at"],
            }
        )
    if _json_mode(args):
        _emit({"requests": described}, True)
        return _EXIT_OK
    if not described:
        print("No pending join requests.")
        return _EXIT_OK
    for entry in described:
        when = time.strftime(
            "%Y-%m-%d %H:%M", time.localtime(entry["requested_at"])
        )
        print(
            f"  {entry['peer_name']} ({entry['peer'][:8]})"
            f" -> {entry['group']} ({entry['group_id'][:8]}) at {when}"
        )
    print("Admit one with `tau group approve <group> <peer>`.")
    return _EXIT_OK


def _change_join_request(args: argparse.Namespace, *, approve: bool) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        own_hash = node.destination_hexhash
        group = node.groups.find(args.group)
        if not group["local"] or group["creator"] != own_hash:
            raise NodeError("only the creator can decide join requests")
        peer_hash = _resolve_join_peer(
            node,
            group["name"],
            args.peer,
            timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
        try:
            if approve:
                outcome = node.approve_join_request(group["name"], peer_hash)
            else:
                outcome = node.deny_join_request(group["name"], peer_hash)
        except ValueError as exc:
            raise NodeError(str(exc)) from exc
        described_group = node.groups.get(outcome["group"]["id"])
        if described_group is not None:
            described_group = _describe_group(described_group, node.registry)
        else:  # pragma: no cover - the group was just updated
            described_group = dict(outcome["group"])
    finally:
        node.stop()
    peer = node.registry.get(peer_hash)
    name = peer.display_name if peer is not None else peer_hash[:8]
    if _json_mode(args):
        _emit(
            {
                "status": outcome["status"],
                "peer": peer_hash,
                "group": described_group,
            },
            True,
        )
        return _EXIT_OK
    if outcome["status"] == "added":
        print(
            f"approved {name} into {described_group['name']}"
            f" ({described_group['id'][:8]} -> {outcome['group']['id'][:8]})"
        )
    elif outcome["status"] == "already_member":
        print(f"{name} is already a member of {described_group['name']}")
    else:
        print(f"denied {name} for {described_group['name']}")
    return _EXIT_OK


def cmd_group_approve(args: argparse.Namespace) -> int:
    return _change_join_request(args, approve=True)


def cmd_group_deny(args: argparse.Namespace) -> int:
    return _change_join_request(args, approve=False)


def cmd_group_topic(args: argparse.Namespace) -> int:
    """Set or show the topic of a group this node created.

    With no text (and no ``--clear``) this only reads: the topic is
    visible to any member, so showing it needs no creator check and no
    identity.  Setting it does — only the creator can — and needs no
    running node: the topic is local state until the next group message
    carries it to the members.
    """
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    try:
        group = store.find(args.group)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if getattr(args, "clear", False) and getattr(args, "text", None) is not None:
        raise NodeError("pass a topic or --clear, not both")
    if getattr(args, "file", None) and getattr(args, "text", None) is not None:
        raise NodeError("pass a topic text or --file, not both")
    if getattr(args, "file", None) and getattr(args, "clear", False):
        raise NodeError("pass --file or --clear, not both")
    text = getattr(args, "text", None)
    if getattr(args, "file", None):
        try:
            text = Path(args.file).expanduser().read_text(encoding="utf-8")
        except OSError as exc:
            raise NodeError(f"could not read {args.file}: {exc}") from exc
        if not text.strip():
            raise NodeError(
                f"{args.file} is blank; pass --clear to remove the topic"
            )
    registry = PeerRegistry(config.peers_path)
    if text is None and not getattr(args, "clear", False):
        described = _describe_group(group, registry)
        if _json_mode(args):
            _emit({"group": described}, True)
            return _EXIT_OK
        topic = group.get("topic", "")
        if topic:
            print(f"{group['name']} ({group['id'][:8]}): {topic}")
        else:
            print(f"{group['name']} ({group['id'][:8]}) has no topic set.")
        return _EXIT_OK
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    own_hash = destination_hash(identity).hex()
    try:
        group = store.set_topic(
            group["id"],
            "" if getattr(args, "clear", False) else (text or ""),
            created_by=own_hash,
        )
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    described = _describe_group(group, registry)
    if _json_mode(args):
        _emit({"group": described}, True)
        return _EXIT_OK
    if group.get("topic"):
        print(f"topic for {group['name']} ({group['id'][:8]}): {group['topic']}")
    else:
        print(f"cleared the topic for {group['name']} ({group['id'][:8]})")
    print("Members learn the new topic from the next message sent to it.")
    return _EXIT_OK


def cmd_group_announcements(args: argparse.Namespace) -> int:
    """Switch a group this node created to (or from) announcements-only.

    With no state this only reads: the setting is visible to any
    member, so showing it needs no creator check and no identity.
    Setting it does — only the creator can — and needs no running
    node: the setting is local state until the next group message
    carries it to the members.
    """
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    try:
        group = store.find(args.group)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    state = getattr(args, "state", None)
    registry = PeerRegistry(config.peers_path)
    if state is None:
        described = _describe_group(group, registry)
        if _json_mode(args):
            _emit({"group": described}, True)
            return _EXIT_OK
        if group.get("announcements_only"):
            print(
                f"{group['name']} ({group['id'][:8]}) is announcements-only: "
                "only the creator can post."
            )
        else:
            print(
                f"{group['name']} ({group['id'][:8]}) is open: "
                "every member can post."
            )
        return _EXIT_OK
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    own_hash = destination_hash(identity).hex()
    try:
        group = store.set_announcements_only(
            group["id"],
            state == "on",
            created_by=own_hash,
        )
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    described = _describe_group(group, registry)
    if _json_mode(args):
        _emit({"group": described}, True)
        return _EXIT_OK
    if group.get("announcements_only"):
        print(
            f"{group['name']} ({group['id'][:8]}) is now announcements-only: "
            "only you can post."
        )
    else:
        print(
            f"{group['name']} ({group['id'][:8]}) is open again: "
            "every member can post."
        )
    print("Members learn the new setting from the next message sent to it.")
    return _EXIT_OK


def cmd_group_rename(args: argparse.Namespace) -> int:
    """Rename a group this node created.

    The id derives from the name, so the rename is a successor group
    (same roster, new id); the store swaps the old record for the new
    one in a single transaction.
    """
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        own_hash = node.destination_hexhash
        group = node.groups.find(args.group)
        if not group["local"] or group["creator"] != own_hash:
            raise NodeError("only the creator can rename a group")
        new_group = node.groups.rename(
            group["id"],
            args.new_name,
            created_by=own_hash,
            identity=getattr(node, "identity", None),
        )
    finally:
        node.stop()

    described = _describe_group(new_group, node.registry)
    if _json_mode(args):
        _emit({"group": described, "previous_id": group["id"]}, True)
        return _EXIT_OK
    if new_group["id"] == group["id"]:
        print(
            f"group {new_group['name']} ({new_group['id'][:8]})"
            " already has that name"
        )
    else:
        print(
            f"renamed group {group['name']!r} to {new_group['name']!r}"
            f" ({group['id'][:8]} -> {new_group['id'][:8]})"
        )
    for member in described["members"]:
        print(f"  {member['name']} ({member['hash'][:8]})")
    print("Members learn the new name from the next message sent to it.")
    return _EXIT_OK


def _send_group_via_running_node(
    args: argparse.Namespace,
    config: TauConfig,
    text: str,
    *,
    expires_at: Optional[float],
    send_after: Optional[float],
    wait: float,
    in_reply_to: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Send a group message through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    try:
        # The per-leg timeout travels in the request; the reply wait stays
        # unbounded because a fan-out can outlast any single leg's bound.
        return send_running(
            config.data_dir,
            op="group_send",
            group=args.group,
            text=text,
            leg_timeout=float(getattr(args, "timeout", 20.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
            queue=not getattr(args, "no_queue", False),
            in_reply_to=in_reply_to,
            location=location,
            expires_at=expires_at,
            wait=wait,
            send_after=send_after,
            poll_options=poll_options,
            poll_vote=poll_vote,
            poll_close=poll_close,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "the running node stopped answering mid-send; retry the send"
        ) from exc


def _send_group_via_local_node(
    args: argparse.Namespace,
    config: TauConfig,
    text: str,
    *,
    expires_at: Optional[float],
    send_after: Optional[float],
    wait: float,
    in_reply_to: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
    node: Optional[TauNode] = None,
    delivered_ids: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """One-shot group send on a node this process starts and stops itself.

    Pass an already-started *node* to send on it instead — the caller
    owns starting and stopping it, so several sends can share one
    Reticulum instance in a process (a second ``TauNode.start()``
    fails: Reticulum cannot be reinitialised in the same process).
    When *node* is given, *delivered_ids* must be the set the node was
    constructed with as its ``on_delivered`` target, so the one-shot
    flush can tell a delivered queued leg from one that merely left
    the outbox.
    """
    owned_node = node is None
    if owned_node:
        delivered_ids = set()
        node = TauNode(config, name=config.name, on_delivered=delivered_ids.add)
        node.start()
    if delivered_ids is None:
        delivered_ids = set()
    try:
        group = node.groups.find(args.group)
        response = node.send_group_message(
            group,
            text,
            timeout=float(getattr(args, "timeout", 20.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
            queue=not getattr(args, "no_queue", False),
            in_reply_to=in_reply_to,
            expires_at=expires_at,
            send_after=send_after,
            location=location,
            poll_options=poll_options,
            poll_vote=poll_vote,
            poll_close=poll_close,
        )
        # --wait replaces the one-shot flush: wait for each queued leg
        # for the requested duration, recording delivered or still
        # queued outcomes. Only queued legs go to the helper, since it
        # treats any other leg without an "ok" key as delivered.
        if wait > 0:
            _wait_for_queued_sends(
                node,
                [result for result in response["results"] if result.get("queued")],
                wait,
            )
        # One-shot send: give reachable members a brief chance to take
        # a queued leg before this process exits.
        elif any(result.get("queued") for result in response["results"]):
            node.flush_outbox(timeout=3.0, max_peers=5)
            for result in response["results"]:
                if not result.get("queued"):
                    continue
                # Only a delivery acknowledgement counts as delivered; an
                # entry that merely vanished was cancelled, expired, or
                # taken by another process.
                if result["id"] in delivered_ids:
                    result["queued"] = False
                    result["delivered"] = True
                elif node.mailbox.get_outbox(result["id"]) is None:
                    result["queued"] = False
                    result["ok"] = False
                    result["error"] = "left the outbox without a delivery confirmation"
    finally:
        if owned_node:
            node.stop()
    return response


def _report_group_send_results(
    args: argparse.Namespace,
    response: Dict[str, Any],
    expires_at: Optional[float],
    send_after: Optional[float],
) -> int:
    """Report a group-send *response* the way ``cmd_group_send`` does
    and return its exit code.

    JSON mode emits the response (with ``expires_at`` / ``send_after``
    added when set); human mode prints one line per leg.  A forward
    reuses this for its attribution text, so the two paths report a
    group send the same way.
    """
    failed_legs = [
        r for r in response["results"] if not r.get("delivered") and not r.get("queued")
    ]
    # --wait outcomes are recorded on the results by the wait path
    # (see _wait_for_queued_sends), mirroring cmd_send: a waited leg
    # that is not delivered failed the wait, whether it is still
    # queued or left the queue without a delivery confirmation.
    wait_failed = any(
        result.get("waited") is not None and result.get("delivered") is False
        for result in response["results"]
    )
    exit_code = _EXIT_ERROR if (failed_legs or wait_failed) else _EXIT_OK
    if expires_at is not None:
        response["expires_at"] = expires_at
    if send_after is not None:
        response["send_after"] = send_after
    if _json_mode(args):
        _emit(response, True)
        return exit_code
    delivered = [r for r in response["results"] if r.get("delivered")]
    queued = [r for r in response["results"] if r.get("queued")]
    failed = failed_legs
    for result in delivered:
        name = result.get("to_name") or result["to"][:8]
        waited = result.get("waited")
        if waited is not None:
            print(f"delivered to {name} after {waited:.1f}s")
        else:
            print(f"delivered to {name}")
    for result in queued:
        name = result.get("to_name") or result["to"][:8]
        if result.get("waited") is not None:
            print(f"still queued for {name} after {result['waited']:.1f}s")
        elif result.get("send_after"):
            print(
                f"scheduled for {name}"
                f" — sends in {_ago(result['send_after'] - time.time())}"
            )
        else:
            print(
                f"queued for {name}"
                " — they are unreachable right now"
            )
    for result in failed:
        name = result.get("to_name") or result["to"][:8]
        if result.get("waited") is not None and not result.get("delivered"):
            # The leg left the queue while waiting without a delivery
            # confirmation from this node: cancelled, or delivered by
            # another process.  Say so instead of claiming delivery.
            print(f"no longer queued for {name} after {result['waited']:.1f}s")
            print("  delivery was not confirmed — it may have been cancelled,")
            print("  or delivered by another process sharing this data directory")
        else:
            print(
                f"failed for {name}:"
                f" {result.get('error') or 'unknown error'}"
            )
    if expires_at is not None:
        print(f"disappears in {_ago(expires_at - time.time())}")
    if queued and all(result.get("send_after") for result in queued):
        print("  it will be delivered at its scheduled time;")
        print("  check `tau outbox` for the queue, or just leave a node serving")
    return exit_code


def cmd_group_send(args: argparse.Namespace) -> int:
    delay_text = getattr(args, "delay", None)
    if delay_text is not None and getattr(args, "no_queue", False):
        # Reject the conflict before reading input, so an invalid
        # command never blocks waiting on stdin.
        print(
            "error: --delay cannot be combined with --no-queue",
            file=sys.stderr,
        )
        return 2
    # Same --wait validation as the peer send path (`cmd_send`).
    wait = float(getattr(args, "wait", None) or 0.0)
    if not math.isfinite(wait):
        print("error: --wait must be a finite number of seconds", file=sys.stderr)
        return 2
    if wait < 0:
        print("error: --wait must not be negative", file=sys.stderr)
        return 2
    if getattr(args, "wait", None) is not None and getattr(args, "no_queue", False):
        print("error: --wait cannot be combined with --no-queue", file=sys.stderr)
        return 2
    # Validate the pin before reading the message, so an invalid
    # --location fails at once instead of blocking on stdin.
    location = _location_from_args(args)
    text = _message_text_with_location(args, location)
    config = _make_config(args)
    expires_at = None
    expire_text = getattr(args, "expire", None)
    if expire_text is not None:
        expires_at = time.time() + _parse_duration(expire_text)
    send_after = None
    if delay_text is not None:
        delay = _parse_duration(delay_text, flag="--delay")
        if delay > MAX_MAIL_TTL - 3600.0:
            print(
                "error: --delay must be at most 29d23h (the 30d mail TTL "
                "less an hour of delivery slack)",
                file=sys.stderr,
            )
            return 2
        send_after = time.time() + delay
        if expires_at is not None and send_after >= expires_at:
            print(
                "error: --delay must be shorter than --expire — the "
                "message would expire before it is sent",
                file=sys.stderr,
            )
            return 2
    in_reply_to = None
    if getattr(args, "reply_to", None) is not None:
        group_id = GroupStore(config.groups_path).find(args.group)["id"]
        in_reply_to = _resolve_group_reply_target(
            Inbox(config.inbox_path),
            Mailbox(config.mailbox_path),
            str(args.reply_to),
            group_id,
        )
    # A serving node owns the data directory: send through it instead of
    # starting a second Reticulum instance on the same identity.
    if _send_routes_via_running_node(config):
        response = _send_group_via_running_node(
            args,
            config,
            text,
            expires_at=expires_at,
            send_after=send_after,
            wait=wait,
            in_reply_to=in_reply_to,
            location=location,
        )
    else:
        response = _send_group_via_local_node(
            args,
            config,
            text,
            expires_at=expires_at,
            send_after=send_after,
            wait=wait,
            in_reply_to=in_reply_to,
            location=location,
        )
    return _report_group_send_results(args, response, expires_at, send_after)


def _send_group_poll_message(
    args: argparse.Namespace,
    config: TauConfig,
    text: str,
    *,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
    send_after: Optional[float] = None,
) -> Dict[str, Any]:
    """Fan one group poll, vote, or close out, like ``cmd_group_send``.

    Polls carry no expiry, quote, or wait — the same two routes as a
    plain group send (the serving node, else a one-shot local node)
    with the poll payload attached.  ``send_after`` schedules a poll
    the way it schedules a plain group message: the poll is queued
    with a not-before time and publishes with its options intact once
    the time arrives.
    """
    if _send_routes_via_running_node(config):
        return _send_group_via_running_node(
            args,
            config,
            text,
            expires_at=None,
            send_after=send_after,
            wait=0.0,
            poll_options=poll_options,
            poll_vote=poll_vote,
            poll_close=poll_close,
        )
    return _send_group_via_local_node(
        args,
        config,
        text,
        expires_at=None,
        send_after=send_after,
        wait=0.0,
        poll_options=poll_options,
        poll_vote=poll_vote,
        poll_close=poll_close,
    )


def _report_group_poll_send(
    args: argparse.Namespace,
    response: Dict[str, Any],
    summary: str,
) -> int:
    """Report a group poll/vote/close fan-out: per-member results."""
    results = response.get("results") or []
    failed = [r for r in results if not r.get("delivered") and not r.get("queued")]
    if _json_mode(args):
        _emit(response, True)
        return _EXIT_ERROR if failed else _EXIT_OK
    print(summary)
    for result in results:
        name = result.get("to_name") or (result.get("to") or "")[:8]
        if result.get("delivered"):
            print(f"delivered to {name}")
        elif result.get("queued"):
            if result.get("send_after"):
                print(
                    f"scheduled for {name}"
                    f" — sends in {_ago(result['send_after'] - time.time())}"
                )
            else:
                print(f"queued for {name} — they are unreachable right now")
        else:
            print(f"failed for {name}: {result.get('error') or 'unknown error'}")
    queued = [result for result in results if result.get("queued")]
    if queued and all(result.get("send_after") for result in queued):
        print("  it will be delivered at its scheduled time;")
        print("  check `tau outbox` for the queue, or just leave a node serving")
    return _EXIT_ERROR if failed else _EXIT_OK


def _group_poll_history(
    inbox: Inbox, group_id: str
) -> List[Dict[str, Any]]:
    """This group's stored messages, for tallying one of its polls."""
    return inbox.list_messages(limit=None, group=group_id, direction=None)


def _resolve_dm_poll(
    inbox: Inbox, ref: str, peer_hash: str
) -> Dict[str, Any]:
    """Resolve ``tau vote`` / ``close-poll`` to one stored DM poll.

    Same scoping as :func:`_resolve_group_poll` but for a DM
    conversation: the reference (a message id or unique prefix, as
    shown by ``tau messages``) must match exactly one stored poll
    record with this peer.  A vote or close record never resolves
    — only the poll itself can be voted on or closed.
    """
    ref = ref.strip()
    if not ref:
        raise NodeError("poll must not be empty")
    matches = [
        item
        for item in inbox.find_prefix(ref, peer=peer_hash)
        if is_dm_poll(item)
    ]
    exact = [m for m in matches if str(m["id"]) == ref]
    if not exact:
        folded = ref.lower()
        exact = [m for m in matches if str(m["id"]).lower() == folded]
    if exact:
        matches = exact
    if len(matches) > 1:
        raise NodeError(
            f"{len(matches)} polls with this peer match {ref!r};"
            " use a longer prefix"
        )
    if not matches:
        raise NodeError(
            f"no poll {ref!r} with this peer;"
            " votes name a poll from this conversation's history"
        )
    return matches[0]


def _dm_poll_history(inbox: Inbox, peer_hash: str) -> List[Dict[str, Any]]:
    """This conversation's stored messages, for tallying one of its polls."""
    return inbox.list_messages(limit=None, peer=peer_hash, direction=None)


def _send_dm_poll_message(
    args: argparse.Namespace,
    config: TauConfig,
    peer: str,
    text: str,
    *,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Send one DM poll vote or close, like ``cmd_send``.

    Votes and closes carry no expiry, schedule, quote, or wait —
    the same two routes as a plain DM send (the serving node, else
    a one-shot local node) with the poll payload attached.
    """
    if _send_routes_via_running_node(config):
        results = _send_via_running_node(
            args,
            config,
            text,
            [peer],
            expires_at=None,
            reply_ids={},
            wait=0.0,
            poll_vote=poll_vote,
            poll_close=poll_close,
        )
    else:
        results = _send_via_local_node(
            args,
            config,
            text,
            [peer],
            expires_at=None,
            reply_ids={},
            wait=0.0,
            poll_vote=poll_vote,
            poll_close=poll_close,
        )
    return {"results": results}


def _report_dm_poll_send(
    args: argparse.Namespace,
    response: Dict[str, Any],
    summary: str,
) -> int:
    """Report a DM poll vote/close send, like ``cmd_send`` does."""
    results = response.get("results") or []
    # A direct delivery keeps no "delivered" key — the single-recipient
    # send shape carries "ok" instead (see _finalize_send_result) — so
    # delivered is "ok" and not queued and not explicitly undelivered,
    # the same rule cmd_send reports by.
    failed = [
        result
        for result in results
        if not result.get("ok", True) or result.get("delivered") is False
    ]
    if _json_mode(args):
        _emit(response, True)
        return _EXIT_ERROR if failed else _EXIT_OK
    print(summary)
    for result in results:
        name = result.get("target") or (result.get("to") or "")[:8]
        if result.get("queued"):
            print(f"queued for {name} — they are unreachable right now")
        elif result.get("ok", True) and result.get("delivered") is not False:
            print(f"delivered to {name}")
        else:
            print(f"failed for {name}: {result.get('error') or 'unknown error'}")
    return _EXIT_ERROR if failed else _EXIT_OK


def cmd_vote(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        peer_hash = _resolve_inbox_peer(
            inbox, registry, args.to, direction=None
        )
    except NodeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        poll = _resolve_dm_poll(inbox, args.poll, peer_hash)
    except NodeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    history = _dm_poll_history(inbox, peer_hash)
    own_hash = _own_destination_hash(config)
    tally = dm_poll_tally(history, poll, own_hash=own_hash)
    if tally["closed"]:
        print("error: this poll is closed", file=sys.stderr)
        return 2
    options = tally["options"]
    choice = str(args.option).strip()
    index: Optional[int] = None
    if choice.isdigit():
        index = int(choice) - 1
    else:
        for position, option in enumerate(options):
            if option == choice:
                index = position
                break
    if index is None or not 0 <= index < len(options):
        print(
            "error: option must be a number 1-"
            f"{len(options)} or one of: {', '.join(options)}",
            file=sys.stderr,
        )
        return 2
    identity = poll_identity(poll, own_hash)
    if identity is None:
        print(
            "error: this poll has no usable identity;"
            " it may predate polls or lack an identity",
            file=sys.stderr,
        )
        return 2
    try:
        response = _send_dm_poll_message(
            args,
            config,
            args.to,
            f"voted: {options[index]}",
            poll_vote={
                "target": {"author": identity[0], "sent_at": identity[1]},
                "option": index,
            },
        )
    except (NodeError, MailboxError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    peer = registry.get(peer_hash)
    name = peer.display_name if peer is not None else short_hash(peer_hash)
    summary = (
        f"voted for {options[index]!r} on {poll.get('text')!r}"
        f" with {name}"
    )
    return _report_dm_poll_send(args, response, summary)


def cmd_close_poll(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        peer_hash = _resolve_inbox_peer(
            inbox, registry, args.to, direction=None
        )
    except NodeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        poll = _resolve_dm_poll(inbox, args.poll, peer_hash)
    except NodeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    own_hash = _own_destination_hash(config)
    identity = poll_identity(poll, own_hash)
    if identity is None:
        print(
            "error: this poll has no usable identity;"
            " it may predate polls or lack an identity",
            file=sys.stderr,
        )
        return 2
    if own_hash is None or identity[0] != own_hash.lower():
        print(
            "error: only the poll's creator can close it",
            file=sys.stderr,
        )
        return 2
    history = _dm_poll_history(inbox, peer_hash)
    tally = dm_poll_tally(history, poll, own_hash=own_hash)
    if tally["closed"]:
        print("error: this poll is already closed", file=sys.stderr)
        return 2
    try:
        response = _send_dm_poll_message(
            args,
            config,
            args.to,
            "closed the poll",
            poll_close={"author": identity[0], "sent_at": identity[1]},
        )
    except (NodeError, MailboxError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    peer = registry.get(peer_hash)
    name = peer.display_name if peer is not None else short_hash(peer_hash)
    summary = f"closed the poll {poll.get('text')!r} with {name}"
    return _report_dm_poll_send(args, response, summary)


def cmd_group_poll(args: argparse.Namespace) -> int:
    delay_text = getattr(args, "delay", None)
    if delay_text is not None and getattr(args, "no_queue", False):
        # Reject the conflict before doing anything else, like
        # ``cmd_group_send``: a scheduled poll is queued by definition.
        print(
            "error: --delay cannot be combined with --no-queue",
            file=sys.stderr,
        )
        return 2
    config = _make_config(args)
    try:
        options = clean_poll_options(list(args.options))
    except PostError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    group = GroupStore(config.groups_path).find(args.group)
    if group is None:
        print(f"error: unknown group: {args.group!r}", file=sys.stderr)
        return 2
    send_after = None
    if delay_text is not None:
        delay = _parse_duration(delay_text, flag="--delay")
        if delay > MAX_MAIL_TTL - 3600.0:
            print(
                "error: --delay must be at most 29d23h (the 30d mail TTL "
                "less an hour of delivery slack)",
                file=sys.stderr,
            )
            return 2
        send_after = time.time() + delay
    try:
        response = _send_group_poll_message(
            args, config, args.question, poll_options=options,
            send_after=send_after,
        )
    except (NodeError, MailboxError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    poll_id = ""
    results = response.get("results") or []
    if results:
        poll_id = str(results[0].get("id") or "")
    verb = "scheduled for" if send_after is not None else "posted to"
    summary = (
        f"poll {verb} {group['name']}"
        + (f" #{_safe_id_prefix(poll_id)}" if poll_id else "")
        + ": "
        + args.question
    )
    if send_after is not None:
        response["send_after"] = send_after
    return _report_group_poll_send(args, response, summary)


def cmd_group_vote(args: argparse.Namespace) -> int:
    config = _make_config(args)
    group = GroupStore(config.groups_path).find(args.group)
    if group is None:
        print(f"error: unknown group: {args.group!r}", file=sys.stderr)
        return 2
    inbox = Inbox(config.inbox_path)
    try:
        poll = _resolve_group_poll(inbox, args.poll, group["id"])
    except NodeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    history = _group_poll_history(inbox, group["id"])
    own_hash = _own_destination_hash(config)
    tally = group_poll_tally(history, poll, own_hash=own_hash)
    if tally["closed"]:
        print("error: this poll is closed", file=sys.stderr)
        return 2
    options = tally["options"]
    choice = str(args.option).strip()
    index: Optional[int] = None
    if choice.isdigit():
        index = int(choice) - 1
    else:
        for position, option in enumerate(options):
            if option == choice:
                index = position
                break
    if index is None or not 0 <= index < len(options):
        print(
            "error: option must be a number 1-"
            f"{len(options)} or one of: {', '.join(options)}",
            file=sys.stderr,
        )
        return 2
    identity = poll_identity(poll, own_hash)
    if identity is None:
        print(
            "error: this poll has no usable identity;"
            " it may predate polls or lack an identity",
            file=sys.stderr,
        )
        return 2
    try:
        response = _send_group_poll_message(
            args,
            config,
            f"voted: {options[index]}",
            poll_vote={
                "target": {"author": identity[0], "sent_at": identity[1]},
                "option": index,
            },
        )
    except (NodeError, MailboxError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    summary = (
        f"voted for {options[index]!r} on {poll.get('text')!r}"
        f" in {group['name']}"
    )
    return _report_group_poll_send(args, response, summary)


def cmd_group_close_poll(args: argparse.Namespace) -> int:
    config = _make_config(args)
    group = GroupStore(config.groups_path).find(args.group)
    if group is None:
        print(f"error: unknown group: {args.group!r}", file=sys.stderr)
        return 2
    inbox = Inbox(config.inbox_path)
    try:
        poll = _resolve_group_poll(inbox, args.poll, group["id"])
    except NodeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    own_hash = _own_destination_hash(config)
    identity = poll_identity(poll, own_hash)
    if identity is None:
        print(
            "error: this poll has no usable identity;"
            " it may predate polls or lack an identity",
            file=sys.stderr,
        )
        return 2
    if own_hash is None or identity[0] != own_hash.lower():
        print(
            "error: only the poll's creator can close it",
            file=sys.stderr,
        )
        return 2
    history = _group_poll_history(inbox, group["id"])
    tally = group_poll_tally(history, poll, own_hash=own_hash)
    if tally["closed"]:
        print("error: this poll is already closed", file=sys.stderr)
        return 2
    try:
        response = _send_group_poll_message(
            args,
            config,
            "closed the poll",
            poll_close={"author": identity[0], "sent_at": identity[1]},
        )
    except (NodeError, MailboxError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    summary = f"closed the poll {poll.get('text')!r} in {group['name']}"
    return _report_group_poll_send(args, response, summary)


def _report_group_reaction(
    args: argparse.Namespace,
    response: Dict[str, Any],
    token: str,
    target: Dict[str, Any],
) -> int:
    """Report a group-reaction fan-out: per-member results in both modes."""
    results = response.get("results") or []
    failed_legs = [
        r for r in results if not r.get("delivered") and not r.get("queued")
    ]
    if _json_mode(args):
        _emit(response, True)
        return _EXIT_ERROR if failed_legs else _EXIT_OK
    if results and all(result.get("queued") for result in results):
        if not token:
            print(
                "retraction queued for the group message "
                f"{_safe_id_prefix(str(target['id']))}"
            )
        else:
            print(
                f"reaction {token} queued for the group message "
                f"{_safe_id_prefix(str(target['id']))}"
            )
    elif not token:
        print(
            "retracted your reaction to the group message "
            f"{_safe_id_prefix(str(target['id']))}"
        )
    else:
        print(
            f"reacted {token} to the group message "
            f"{_safe_id_prefix(str(target['id']))}"
        )
    for result in results:
        name = result.get("to_name") or (result.get("to") or "")[:8]
        if result.get("delivered"):
            print(f"delivered to {name}")
        elif result.get("queued"):
            print(f"queued for {name}")
        else:
            print(
                f"failed for {name}: "
                f"{_safe_text(str(result.get('error') or 'unknown error'))}"
            )
    return _EXIT_ERROR if failed_legs else _EXIT_OK


def cmd_group_send_file(args: argparse.Namespace) -> int:
    config = _make_config(args)
    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        delay = _parse_duration(delay_text, flag="--delay")
        if delay > MAX_MAIL_TTL - 3600.0:
            print(
                "error: --delay must be at most 29d23h (the 30d mail TTL "
                "less an hour of delivery slack)",
                file=sys.stderr,
            )
            return 2
        # Resolve the group without starting a node: scheduling only
        # needs the group to exist.  The name is stored for display
        # and the id plus roster snapshot pin the delivery target, so
        # a later roster change or name reuse cannot redirect the file.
        group = GroupStore(config.groups_path).find(args.group)
        entry = _queue_file_for_later(
            config,
            target=group["name"],
            kind="group",
            path=args.path,
            send_after=time.time() + delay,
            group_id=group["id"],
            group_members=tuple(sorted(group["members"])),
            timeout=float(getattr(args, "timeout", 300.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
        if _json_mode(args):
            _emit_scheduled_file(entry)
        else:
            _print_scheduled_file(entry, delay=delay)
        return _EXIT_OK
    node = TauNode(config, name=config.name)
    node.start()
    try:
        group = node.groups.find(args.group)
        response = node.send_group_file(
            group,
            args.path,
            timeout=float(getattr(args, "timeout", 300.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
    finally:
        node.stop()

    failed_legs = [
        r for r in response["results"] if not r.get("delivered")
    ]
    exit_code = _EXIT_ERROR if failed_legs else _EXIT_OK
    if _json_mode(args):
        _emit(response, True)
        return exit_code
    if not response["results"]:
        print(
            f"group {response['name']} has no other members"
            " — nothing to send to."
        )
        return _EXIT_OK
    for result in response["results"]:
        name = result.get("to_name") or result["to"][:8]
        if result.get("delivered"):
            print(
                f"delivered {result['name']} ({result['size']} bytes) to {name}"
            )
        else:
            print(
                f"failed for {name}: {result.get('error') or 'unknown error'}"
            )
    return exit_code


def _describe_broadcast(
    broadcast: Dict[str, Any], registry: PeerRegistry
) -> Dict[str, Any]:
    """A broadcast list record with member display names for output."""
    members = []
    for member_hash in broadcast["members"]:
        peer = registry.get(member_hash)
        members.append(
            {
                "hash": member_hash,
                "name": peer.display_name if peer is not None else member_hash[:8],
            }
        )
    return {
        "name": broadcast["name"],
        "members": members,
        "created_at": broadcast["created_at"],
    }


def cmd_broadcast_create(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        own_hash = node.destination_hexhash
        members: List[str] = []
        for target in getattr(args, "peer", []):
            peer = node.resolve_peer(
                target,
                timeout=float(getattr(args, "discover_timeout", 10.0)),
            )
            if peer.hexhash == own_hash:
                raise NodeError(
                    "cannot add yourself to a broadcast list;"
                    " you are the sender, not a member"
                )
            if peer.hexhash not in members:
                members.append(peer.hexhash)
        if not members:
            raise NodeError("a broadcast list needs at least one peer")
        store = BroadcastStore(config.broadcasts_path)
        try:
            broadcast = store.create(args.broadcast_name, members)
        except ValueError as exc:
            raise NodeError(str(exc)) from exc
    finally:
        node.stop()

    described = _describe_broadcast(broadcast, node.registry)
    if _json_mode(args):
        _emit({"broadcast": described}, True)
        return _EXIT_OK
    print(f"created broadcast list {broadcast['name']}")
    for member in described["members"]:
        print(f"  {member['name']} ({member['hash'][:8]})")
    print("Members get a private DM each — they never see the list.")
    return _EXIT_OK


def cmd_broadcast_list(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = BroadcastStore(config.broadcasts_path)
    registry = PeerRegistry(config.peers_path)
    lists = [_describe_broadcast(entry, registry) for entry in store.list_broadcasts()]
    if _json_mode(args):
        _emit({"broadcasts": lists, "count": len(lists)}, True)
        return _EXIT_OK
    if not lists:
        print("No broadcast lists yet — create one with `tau broadcast create NAME --peer ...`.")
        return _EXIT_OK
    for entry in lists:
        names = ", ".join(member["name"] for member in entry["members"])
        print(f"{entry['name']} ({len(entry['members'])} members) — {names}")
    return _EXIT_OK


def cmd_broadcast_show(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = BroadcastStore(config.broadcasts_path)
    try:
        broadcast = store.find(args.broadcast)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    described = _describe_broadcast(broadcast, PeerRegistry(config.peers_path))
    if _json_mode(args):
        _emit({"broadcast": described}, True)
        return _EXIT_OK
    print(f"{described['name']} ({len(described['members'])} members)")
    for member in described["members"]:
        print(f"  {member['name']} ({member['hash'][:8]})")
    return _EXIT_OK


def _change_broadcast_members(args: argparse.Namespace, *, adding: bool) -> int:
    """Add peers to, or remove peers from, a broadcast list.

    Adding resolves each peer (with discovery, like ``tau group
    add-member``), so a mistyped or unknown peer is refused before the
    list changes.  Removing only needs the hash already in the list:
    an offline member may have left the registry, and resolving it
    would wait out the whole discover timeout.
    """
    config = _make_config(args)
    store = BroadcastStore(config.broadcasts_path)
    try:
        broadcast = store.find(args.broadcast)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    members = list(broadcast["members"])
    if adding:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            own_hash = node.destination_hexhash
            resolved: List[str] = []
            for target in getattr(args, "peer", []):
                peer = node.resolve_peer(
                    target,
                    timeout=float(getattr(args, "discover_timeout", 10.0)),
                )
                if peer.hexhash == own_hash:
                    raise NodeError(
                        "cannot add yourself to a broadcast list;"
                        " you are the sender, not a member"
                    )
                if peer.hexhash in members:
                    raise NodeError(
                        f"{peer.display_name} is already on"
                        f" broadcast list {broadcast['name']!r}"
                    )
                if peer.hexhash not in resolved:
                    resolved.append(peer.hexhash)
            try:
                updated = store.add_members(broadcast["name"], resolved)
            except ValueError as exc:
                raise NodeError(str(exc)) from exc
        finally:
            node.stop()
    else:
        registry = PeerRegistry(config.peers_path)
        resolved = []
        for target in getattr(args, "peer", []):
            fast = _roster_hash(target, members)
            if fast is not None:
                resolved.append(fast)
                continue
            peer = registry.resolve(target)
            if peer is None:
                raise NodeError(f"unknown peer: {target!r}")
            if peer.hexhash not in members:
                raise NodeError(
                    f"{peer.display_name} is not on"
                    f" broadcast list {broadcast['name']!r}"
                )
            resolved.append(peer.hexhash)
        try:
            updated = store.remove_members(broadcast["name"], resolved)
        except ValueError as exc:
            raise NodeError(str(exc)) from exc
    described = _describe_broadcast(updated, PeerRegistry(config.peers_path))
    if _json_mode(args):
        _emit({"broadcast": described}, True)
        return _EXIT_OK
    names = ", ".join(member["name"] for member in described["members"])
    print(f"{described['name']} ({len(described['members'])} members) — {names}")
    return _EXIT_OK


def cmd_broadcast_add_member(args: argparse.Namespace) -> int:
    return _change_broadcast_members(args, adding=True)


def cmd_broadcast_remove_member(args: argparse.Namespace) -> int:
    return _change_broadcast_members(args, adding=False)


def cmd_broadcast_delete(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = BroadcastStore(config.broadcasts_path)
    try:
        broadcast = store.find(args.broadcast)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not store.delete(broadcast["name"]):
        raise NodeError(f"unknown broadcast list: {args.broadcast!r}")
    if _json_mode(args):
        _emit({"deleted": broadcast["name"]}, True)
        return _EXIT_OK
    print(f"deleted broadcast list {broadcast['name']}")
    return _EXIT_OK


def cmd_broadcast_rename(args: argparse.Namespace) -> int:
    """Rename a broadcast list in place, keeping its members.

    Local-only, like ``delete``: the list name never travels on the
    wire, so no node is needed and members never learn the new name —
    they only ever see their own private DM.
    """
    config = _make_config(args)
    store = BroadcastStore(config.broadcasts_path)
    try:
        old = store.find(args.broadcast)
        renamed = store.rename(args.broadcast, args.new_name)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    described = _describe_broadcast(renamed, PeerRegistry(config.peers_path))
    if _json_mode(args):
        _emit({"broadcast": described, "previous_name": old["name"]}, True)
        return _EXIT_OK
    if renamed["name"] == old["name"]:
        print(f"broadcast list {renamed['name']!r} already has that name")
    else:
        print(f"renamed broadcast list {old['name']!r} to {renamed['name']!r}")
    return _EXIT_OK


def cmd_broadcast_send(args: argparse.Namespace) -> int:
    """Send a message to every member of a broadcast list, as a private DM each.

    Delivery reuses :func:`cmd_send` wholesale: each member gets their
    own send, sent record and store-and-forward envelope, so an offline
    member queues exactly like a one-to-one DM and no list metadata
    ever reaches a recipient.
    """
    config = _make_config(args)
    store = BroadcastStore(config.broadcasts_path)
    try:
        broadcast = store.find(args.broadcast)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not broadcast["members"]:
        # Unreachable through the store (it refuses empty lists), but
        # a hand-edited broadcasts.json must not send to nobody silently.
        raise NodeError(f"broadcast list {broadcast['name']!r} has no members")
    # `cmd_send` reads its text from `args.message` and its targets from
    # `args.to`; hand it the list's members and let it own delivery,
    # queueing, and output.
    args.to = list(broadcast["members"])
    return cmd_send(args)


def cmd_broadcast_poll(args: argparse.Namespace) -> int:
    """Post a poll to every member of a broadcast list, as a private DM poll each.

    Delivery reuses :func:`cmd_send` wholesale, exactly like
    :func:`cmd_broadcast_send`: each member gets their own DM poll,
    with its own poll identity and tally in the sender's thread with
    that member, so an offline member queues exactly like a one-to-one
    DM poll and no list metadata ever reaches a recipient.  Votes come
    back as ordinary DM poll votes; there is no cross-member tally.
    """
    config = _make_config(args)
    store = BroadcastStore(config.broadcasts_path)
    try:
        broadcast = store.find(args.broadcast)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not broadcast["members"]:
        # Unreachable through the store (it refuses empty lists), but
        # a hand-edited broadcasts.json must not poll nobody silently.
        raise NodeError(f"broadcast list {broadcast['name']!r} has no members")
    # `cmd_send` reads its text from `args.message`, its targets from
    # `args.to`, and its options from `args.poll_options`; hand it the
    # list's members and let it own validation, delivery, queueing,
    # scheduling, and output.
    args.to = list(broadcast["members"])
    args.message = args.question
    args.poll_options = list(args.options)
    return cmd_send(args)


def cmd_broadcast_send_file(args: argparse.Namespace) -> int:
    """Send a file to every member of a broadcast list, as a private DM each.

    Mirrors :func:`cmd_group_send_file`: one ``/tau/file`` offer +
    RNS Resource transfer per member, each with its own transfer id,
    and no list metadata ever reaches a recipient.
    """
    config = _make_config(args)
    delay_text = getattr(args, "delay", None)
    if delay_text is not None:
        delay = _parse_duration(delay_text, flag="--delay")
        if delay > MAX_MAIL_TTL - 3600.0:
            print(
                "error: --delay must be at most 29d23h (the 30d mail TTL "
                "less an hour of delivery slack)",
                file=sys.stderr,
            )
            return 2
        # Resolve the list without starting a node: scheduling only
        # needs the list to exist.  The name is stored for display
        # and the member snapshot pins the delivery target, so a
        # later member change or name reuse cannot redirect the file.
        store = BroadcastStore(config.broadcasts_path)
        try:
            broadcast = store.find(args.broadcast)
        except ValueError as exc:
            raise NodeError(str(exc)) from exc
        entry = _queue_file_for_later(
            config,
            target=broadcast["name"],
            kind="broadcast",
            path=args.path,
            send_after=time.time() + delay,
            group_members=tuple(sorted(broadcast["members"])),
            timeout=float(getattr(args, "timeout", 300.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
        if _json_mode(args):
            _emit_scheduled_file(entry)
        else:
            _print_scheduled_file(entry, delay=delay)
        return _EXIT_OK
    node = TauNode(config, name=config.name)
    node.start()
    try:
        response = node.send_broadcast_file(
            args.broadcast,
            args.path,
            timeout=float(getattr(args, "timeout", 300.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
    finally:
        node.stop()

    failed_legs = [
        r for r in response["results"] if not r.get("delivered")
    ]
    exit_code = _EXIT_ERROR if failed_legs else _EXIT_OK
    if _json_mode(args):
        _emit(response, True)
        return exit_code
    if not response["results"]:
        print(
            f"broadcast list {response['broadcast']} has no other members"
            " — nothing to send to."
        )
        return _EXIT_OK
    for result in response["results"]:
        name = result.get("to_name") or result["to"][:8]
        if result.get("delivered"):
            print(
                f"delivered {result['name']} ({result['size']} bytes) to {name}"
            )
        else:
            print(
                f"failed for {name}: {result.get('error') or 'unknown error'}"
            )
    return exit_code


def cmd_note(args: argparse.Namespace) -> int:
    """Save a private note to self in the local inbox.

    Local-only: no network, no announce, nothing queued.  The note is
    an inbox record with kind ``note`` from this node to itself, so it
    shows in `tau messages`, is found by `--search` and `--kind note`,
    and is deleted and pruned like any other stored message.  It is
    read the moment it is written, so it never counts as unread.
    """
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    text = normalize_message(_read_message(args))
    inbox = Inbox(config.inbox_path)
    entry = inbox.add_note(
        text,
        sender_name=config.name,
        destination=destination_hash(identity).hex(),
    )
    if _json_mode(args):
        _emit({"note": entry}, True)
    else:
        print(f"note saved #{_safe_id_prefix(str(entry['id']))} [note]")
        for line in str(entry.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK


def _resolve_draft_target(
    config: TauConfig, args: argparse.Namespace
) -> Tuple[str, str, str]:
    """Resolve a draft command's ``--to``/``--group``/``--channel`` to ``(kind, id, name)``.

    Peers resolve through the local registry only — a draft is local
    state for someone this node already knows, so there is no reason to
    start a node and discover.  Groups resolve through the group store.
    Channels need no lookup: the name is the key, so it is cleaned and
    returned as-is; ``cmd_draft_save`` checks membership separately.
    """
    if getattr(args, "channel", None):
        try:
            name = clean_channel_name(args.channel)
        except ChannelError as exc:
            raise NodeError(str(exc)) from exc
        return "channel", name, name
    if getattr(args, "group", None):
        try:
            group = GroupStore(config.groups_path).find(args.group)
        except ValueError:
            raise NodeError(f"unknown group: {args.group!r}") from None
        return "group", str(group["id"]), str(group["name"])
    registry = PeerRegistry(config.peers_path)
    peer_hash = _resolve_sent_peer(registry, args.to)
    identity = load_identity(config.identity_path)
    if identity is not None and peer_hash == destination_hash(identity).hex():
        raise NodeError(
            "cannot draft a message to yourself — save a note instead"
        )
    peer = registry.get(peer_hash)
    name = peer.display_name if peer is not None else short_hash(peer_hash)
    return "peer", peer_hash, name


def _describe_draft(
    draft: Dict[str, Any], config: TauConfig
) -> Dict[str, Any]:
    """A draft plus its conversation's display name, for output."""
    if draft["kind"] == "group":
        group = GroupStore(config.groups_path).get(str(draft["target"]))
        name = str(group["name"]) if group is not None else short_hash(draft["target"])
    elif draft["kind"] == "channel":
        name = str(draft["target"])
    else:
        peer = PeerRegistry(config.peers_path).get(str(draft["target"]))
        name = peer.display_name if peer is not None else short_hash(draft["target"])
    return {
        "kind": draft["kind"],
        "target": draft["target"],
        "target_name": name,
        "text": draft["text"],
        "updated_at": draft["updated_at"],
    }


def _draft_preview(text: str) -> str:
    """First line of a draft, truncated for a list row."""
    first = text.splitlines()[0] if text.splitlines() else ""
    return first if len(first) <= 60 else first[:57] + "…"


def cmd_draft_save(args: argparse.Namespace) -> int:
    """Save a message as a draft for a peer, a group, or a channel.

    Local-only: the text is validated like a send but never queued or
    transmitted.  Saving again overwrites the conversation's draft.
    A channel draft requires membership, like a group draft requires
    the group to exist — so a typo'd channel fails here, at save time,
    instead of silently parking a draft that can never be sent.
    """
    config = _make_config(args)
    kind, target, name = _resolve_draft_target(config, args)
    if kind == "channel":
        conn = init_channel_db(config.channels_db_path)
        try:
            member = is_channel_member(conn, target)
        finally:
            conn.close()
        if not member:
            raise NodeError(f"not a member of channel {target!r}; join it first")
    text = _read_message(args)
    try:
        draft = DraftStore(config.drafts_path).save(kind, target, text)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    described = _describe_draft(draft, config)
    if _json_mode(args):
        _emit({"draft": described}, True)
    else:
        print(f"draft saved for {name}")
        for line in described["text"].splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK


def cmd_draft_list(args: argparse.Namespace) -> int:
    """List saved message drafts, oldest first."""
    config = _make_config(args)
    drafts = [
        _describe_draft(draft, config)
        for draft in DraftStore(config.drafts_path).list_drafts()
    ]
    if _json_mode(args):
        _emit({"drafts": drafts, "count": len(drafts)}, True)
        return _EXIT_OK
    if not drafts:
        print("No drafts yet — save one with `tau draft save --to PEER \"text\"`.")
        return _EXIT_OK
    for draft in drafts:
        if draft["kind"] == "channel":
            where = "channel "
        elif draft["kind"] == "group":
            where = "group "
        else:
            where = ""
        # A channel draft's target is its name, so the hash prefix
        # other drafts show would just repeat it.
        target = "" if draft["kind"] == "channel" else f" ({draft['target'][:8]})"
        age = _ago(time.time() - float(draft["updated_at"]))
        print(
            f"{where}{draft['target_name']}{target} — "
            f"{_draft_preview(str(draft['text']))} [{age} ago]"
        )
    return _EXIT_OK


def cmd_draft_show(args: argparse.Namespace) -> int:
    """Print a saved draft's full text."""
    config = _make_config(args)
    kind, target, name = _resolve_draft_target(config, args)
    draft = DraftStore(config.drafts_path).get(kind, target)
    if draft is None:
        raise NodeError(f"no draft for {name}")
    if _json_mode(args):
        _emit({"draft": _describe_draft(draft, config)}, True)
    else:
        print(draft["text"])
    return _EXIT_OK


def cmd_draft_delete(args: argparse.Namespace) -> int:
    """Discard a saved draft."""
    config = _make_config(args)
    kind, target, name = _resolve_draft_target(config, args)
    if not DraftStore(config.drafts_path).delete(kind, target):
        raise NodeError(f"no draft for {name}")
    if _json_mode(args):
        _emit(
            {"deleted": {"kind": kind, "target": target, "target_name": name}},
            True,
        )
    else:
        print(f"deleted draft for {name}")
    return _EXIT_OK


def cmd_draft_send(args: argparse.Namespace) -> int:
    """Send a saved draft through the normal send path.

    The draft is deleted once the message is queued or delivered — a
    failed send keeps it, so nothing is lost to a transient failure.
    Delivery reuses :func:`cmd_send` wholesale for a peer target,
    :func:`cmd_group_send` for a group target, or
    :func:`cmd_channel_send` for a channel target, exactly like a
    forward.
    """
    config = _make_config(args)
    kind, target, name = _resolve_draft_target(config, args)
    store = DraftStore(config.drafts_path)
    draft = store.get(kind, target)
    if draft is None:
        raise NodeError(f"no draft for {name}")
    # `cmd_send`/`cmd_group_send`/`cmd_channel_send` read their text
    # from `args.message`; hand them the draft text and let them own
    # delivery, queueing, and output.  The remaining flags come from
    # this subcommand's parser.
    args.message = str(draft["text"])
    args.file = None
    args.location = None
    args.kind = None
    args.delay = None
    args.expire = None
    args.reply_to = None
    if kind == "group":
        args.group = target
        code = cmd_group_send(args)
    elif kind == "channel":
        args.channel = target
        code = cmd_channel_send(args)
    else:
        args.to = [target]
        code = cmd_send(args)
    if code != _EXIT_OK:
        print(
            f"draft for {name} kept — the send failed",
            file=sys.stderr,
        )
        return code
    store.delete(kind, target)
    if not _json_mode(args):
        print(f"sent draft to {name}")
    return _EXIT_OK


def _reminder_sender_blocked(
    inbox: Inbox, reminder: Dict[str, Any], blocked: Any
) -> bool:
    """Whether the reminded message's sender is blocked now (fail closed).

    A DM reminder's target is the sender.  A group reminder names the
    group, so the sender is read from the stored message; when that
    message is gone its sender is unknown, and the reminder is hidden.
    """
    if reminder["kind"] != "group":
        return reminder["target"] in blocked
    entry = inbox.find(
        str(reminder["message_id"]), group=str(reminder["target"])
    )
    if entry is None:
        return True
    return entry.get("destination") in blocked


def _describe_reminder(
    reminder: Dict[str, Any], config: TauConfig
) -> Dict[str, Any]:
    """A reminder plus its conversation's display name, for output."""
    if reminder["kind"] == "group":
        group = GroupStore(config.groups_path).get(str(reminder["target"]))
        name = str(group["name"]) if group is not None else short_hash(reminder["target"])
    else:
        peer = PeerRegistry(config.peers_path).get(str(reminder["target"]))
        name = peer.display_name if peer is not None else short_hash(reminder["target"])
    return {
        "id": reminder["id"],
        "message_id": reminder["message_id"],
        "kind": reminder["kind"],
        "target": reminder["target"],
        "target_name": name,
        "text": reminder["text"],
        "sender": reminder["sender"],
        "due_at": reminder["due_at"],
        "created_at": reminder["created_at"],
    }


def cmd_remind_add(args: argparse.Namespace) -> int:
    """Snooze a stored DM or group message: be re-pinged about it later.

    Local-only: the reminder is a snapshot of the message's text and
    sender, stored in the data dir and fired by the serving node's
    mail loop — nothing is sent to the peer.  A reminder on a message
    from a blocked peer is refused, like a forward is: blocked
    senders' messages are hidden locally, so they cannot be the
    subject of a reminder either.
    """
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        record = inbox.resolve(args.message)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not record.get("text"):
        raise NodeError("cannot remind on a file message: it has no text")
    if record.get("kind") == "note":
        raise NodeError("cannot remind on a private note: remind on a DM or group message")
    if record.get("direction") == "out":
        # A reminder re-pings by marking a received message unread; a
        # conversation with nothing received has nothing to light up.
        raise NodeError("cannot remind on a message you sent: remind on a received message")
    if record.get("direction") != "out":
        blocked = load_blocked_set(config.blocklist_path)
        if blocked is None:
            raise NodeError("cannot remind: the block-list could not be read")
        if record.get("destination") in blocked:
            raise NodeError("cannot remind on a message from a blocked peer")
    if record.get("kind") == "group":
        kind, target = "group", str(record.get("group") or "")
    else:
        kind, target = "peer", str(record.get("destination") or "")
    if record.get("direction") == "out":
        sender_label = "you"
    else:
        sender_hash = record.get("destination")
        stored_name = record.get("from")
        if not stored_name or stored_name == "unknown":
            sender_label = (
                short_hash(sender_hash)
                if isinstance(sender_hash, str) and sender_hash
                else "unknown"
            )
        else:
            sender_label = str(stored_name)
        peer = (
            registry.get(sender_hash)
            if isinstance(sender_hash, str) and sender_hash
            else None
        )
        if peer is not None:
            sender_label = peer.display_name
    if getattr(args, "remind_in", None) is not None:
        try:
            seconds = _parse_duration(
                args.remind_in, max_seconds=MAX_REMINDER_DELAY, flag="--in"
            )
        except ValueError as exc:
            raise NodeError(str(exc)) from exc
        due = time.time() + seconds
        when = f"in {_describe_duration(seconds)}"
    else:
        try:
            due = parse_remind_at(args.remind_at)
        except ValueError as exc:
            raise NodeError(str(exc)) from exc
        when = "at " + datetime.fromtimestamp(due).strftime("%Y-%m-%d %H:%M")
    try:
        reminder = ReminderStore(config.reminders_path).add(
            message_id=str(record.get("id") or ""),
            kind=kind,
            target=target,
            text=str(record.get("text") or ""),
            sender=sender_label,
            due_at=due,
        )
    except (ReminderError, OSError) as exc:
        raise NodeError(str(exc)) from exc
    described = _describe_reminder(reminder, config)
    if _json_mode(args):
        _emit({"reminder": described}, True)
    else:
        print(f"reminder set for {described['target_name']} {when}")
        for line in _draft_preview(str(record.get("text") or "")).splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK


def cmd_remind_list(args: argparse.Namespace) -> int:
    """List pending message reminders, soonest first."""
    config = _make_config(args)
    # A reminder whose sender is blocked now stays hidden, like the
    # message itself; an unreadable block-list hides everything.
    blocked = load_blocked_set(config.blocklist_path)
    inbox = Inbox(config.inbox_path)
    reminders = [
        _describe_reminder(reminder, config)
        for reminder in ReminderStore(config.reminders_path).list_reminders()
        if blocked is not None
        and not _reminder_sender_blocked(inbox, reminder, blocked)
    ]
    if _json_mode(args):
        _emit({"reminders": reminders, "count": len(reminders)}, True)
        return _EXIT_OK
    if not reminders:
        print("No reminders yet — set one with `tau remind add MSG --in 1h`.")
        return _EXIT_OK
    now = time.time()
    for reminder in reminders:
        if reminder["kind"] == "group":
            where = "group "
        else:
            where = ""
        # Round up: a reminder due in 3599s reads as "in 1h", not "in 3599s".
        due_in = max(0, math.ceil(reminder["due_at"] - now))
        print(
            f"[{reminder['id'][:8]}] {where}{reminder['target_name']} — "
            f"{_draft_preview(str(reminder['text']))} "
            f"[in {_describe_duration(max(1, due_in))}, from {reminder['sender']}]"
        )
    return _EXIT_OK


def cmd_remind_cancel(args: argparse.Namespace) -> int:
    """Drop a pending reminder before it fires."""
    config = _make_config(args)
    try:
        removed = ReminderStore(config.reminders_path).cancel(args.reminder)
    except (ReminderError, OSError) as exc:
        raise NodeError(str(exc)) from exc
    described = _describe_reminder(removed, config)
    if _json_mode(args):
        _emit({"cancelled": described}, True)
    else:
        print(
            f"cancelled reminder for {described['target_name']} "
            f"({_draft_preview(str(described['text']))})"
        )
    return _EXIT_OK


def cmd_react(args: argparse.Namespace) -> int:
    if bool(getattr(args, "clear", False)) == bool(getattr(args, "reaction", None)):
        raise NodeError("give a reaction, or --clear to retract it (not both)")
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        target = inbox.resolve(args.message)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not target.get("id"):
        raise NodeError("that message has no id to react to")
    if target.get("kind") == NOTE_KIND:
        # A note is local-only: its destination is this node, not a
        # remote author, so there is no peer to react to.
        raise NodeError("notes are local-only and cannot be reacted to")
    author = target.get("destination") or ""
    if not author and not (
        target.get("kind") == "group" and target.get("direction") == "out"
    ):
        raise NodeError("that message has no known sender to react to")
    if getattr(args, "clear", False):
        token = ""
    else:
        # Validate the token before starting the node: a bad token is a
        # usage error, not a delivery failure.  A blank argument cleans to
        # the retraction token, so reject it unless --clear was given.
        token = clean_reaction(str(args.reaction))
        if not token:
            raise NodeError("reaction is empty; use --clear to retract it")
    node = TauNode(config, name=config.name)
    node.start()
    try:
        if target.get("kind") == "group":
            # A group message reacts to every member, not just the
            # author: the same signed reaction fans out, one leg per
            # member, and the per-member results are the report.  The
            # author and send time of this copy travel in the signed
            # payload so each member can resolve its own copy — group
            # sends carry a different wire id per leg.
            group_id = target.get("group")
            if not isinstance(group_id, str) or not group_id:
                raise NodeError("that group message has no group to react to")
            # Resolve the stored id exactly, as the console does: a
            # retired id that also matches a group name must not
            # resolve to that other group and send the reaction to
            # its members.
            group = node.groups.get(group_id)
            if group is None:
                raise NodeError(f"unknown group: {group_id!r}")
            author = target.get("destination")
            if not author and target.get("direction") == "out":
                # An outbound group message names no single
                # destination: its author is this node.
                author = node.destination_hexhash
            sent_at = target.get("sent_at")
            try:
                response = node.send_group_reaction(
                    group,
                    str(target["id"]),
                    token,
                    timeout=float(getattr(args, "timeout", 20.0)),
                    discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
                    queue=not getattr(args, "no_queue", False),
                    target_author=author if isinstance(author, str) else None,
                    target_sent_at=(
                        sent_at
                        if isinstance(sent_at, (int, float))
                        and not isinstance(sent_at, bool)
                        else None
                    ),
                )
            except NodeError as exc:
                # With queueing on, a transport failure returns queued
                # legs instead of raising; only a refusal or a
                # --no-queue failure reaches here.
                raise NodeError(
                    f"could not deliver group reaction: {exc}"
                ) from exc
            return _report_group_reaction(args, response, token, target)
        author = target.get("destination") or ""
        if not author:
            raise NodeError("that message has no known sender to react to")
        try:
            response = node.send_reaction(
                author,
                str(target["id"]),
                token,
                timeout=float(getattr(args, "timeout", 20.0)),
                discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
                queue=not getattr(args, "no_queue", False),
            )
        except NodeError as exc:
            # With queueing on, an unreachable peer returns a queued
            # result instead of raising; only a refusal or a
            # --no-queue failure reaches here.
            raise NodeError(
                f"could not deliver reaction: {exc}"
            ) from exc
    finally:
        node.stop()

    peer = registry.get(author)
    name = peer.display_name if peer is not None else target.get("from") or author
    if response.get("queued"):
        if _json_mode(args):
            _emit(response, True)
        elif not token:
            print(f"retraction queued — will deliver when {name} is reachable")
        else:
            print(
                f"reaction {token} queued — will deliver when {name} "
                "is reachable"
            )
        return _EXIT_OK
    if _json_mode(args):
        _emit(
            {
                "target": target["id"],
                "to": author,
                "to_name": name,
                "reaction": token,
                "cleared": not token,
                "id": response.get("id"),
            },
            True,
        )
    elif not token:
        print(f"retracted your reaction to {name}'s message {_safe_id_prefix(str(target['id']))}")
    else:
        print(f"reacted {token} to {name}'s message {_safe_id_prefix(str(target['id']))}")
    return _EXIT_OK


def _report_group_typing(
    args: argparse.Namespace,
    response: Dict[str, Any],
    state: str,
) -> int:
    """Report a group-typing fan-out: per-member results in both modes."""
    results = response.get("results") or []
    failed_legs = [r for r in results if not r.get("delivered")]
    if _json_mode(args):
        _emit(response, True)
        return _EXIT_ERROR if failed_legs else _EXIT_OK
    verb = "typing in" if state == "start" else "stopped typing in"
    print(f"{verb} group {response.get('name') or response.get('group')}")
    for result in results:
        name = result.get("to_name") or (result.get("to") or "")[:8]
        if result.get("delivered"):
            print(f"delivered to {name}")
        else:
            print(
                f"failed for {name}: "
                f"{_safe_text(str(result.get('error') or 'unknown error'))}"
            )
    return _EXIT_ERROR if failed_legs else _EXIT_OK


def cmd_typing(args: argparse.Namespace) -> int:
    """Send a typing event to a peer or a group, from the CLI.

    Typing is live-only: an offline peer is a plain failure, not a
    queued send — a typing event that cannot be delivered now is
    stale by the time the peer returns.
    """
    state = str(args.state)
    config = _make_config(args)
    timeout = float(getattr(args, "timeout", 20.0))
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    group = getattr(args, "group", None)
    target = getattr(args, "to", None)
    if not group and not target:
        raise NodeError("give a peer with --to, or a group with --group")
    # A serving node owns the data directory: type through it instead of
    # starting a second Reticulum instance on the same identity (a second
    # instance cannot bind the serving node's TCP listen port).
    try:
        if _send_routes_via_running_node(config):
            if group:
                response = _typing_group_via_running_node(
                    config,
                    str(group),
                    state,
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                )
            else:
                response = _typing_via_running_node(
                    config,
                    str(target),
                    state,
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                )
        else:
            node = TauNode(config, name=config.name)
            node.start()
            try:
                if group:
                    response = _typing_group_local_message(
                        node,
                        group=str(group),
                        state=state,
                        timeout=timeout,
                        discover_timeout=discover_timeout,
                    )
                else:
                    response = _typing_local_message(
                        node,
                        target=str(target),
                        state=state,
                        timeout=timeout,
                        discover_timeout=discover_timeout,
                    )
            finally:
                node.stop()
    except NodeError as exc:
        # Typing rides the live link only in v1 — there is no outbox
        # queue for it — so an unreachable peer or member is a plain
        # failure, not a silent queue.
        if group:
            raise NodeError(
                f"could not deliver group typing event: {exc} "
                "(typing needs the members online in v1)"
            ) from exc
        raise NodeError(
            f"could not deliver typing event: {exc} "
            "(typing needs the peer online in v1)"
        ) from exc

    if group:
        return _report_group_typing(args, response, state)
    name = str(response.get("to_name") or target)
    if _json_mode(args):
        _emit(response, True)
        return _EXIT_OK
    verb = "typing to" if state == "start" else "stopped typing to"
    print(f"{verb} {name}")
    return _EXIT_OK


def _retract_group_message(
    args: argparse.Namespace,
    config: TauConfig,
    inbox: Inbox,
    registry: PeerRegistry,
    target: Dict[str, Any],
) -> int:
    """Retract a group message, fanning out to every member.

    An author retracts their own sent message; a group creator may
    also retract a member's message (moderation — see
    :func:`cmd_retract`), in which case the fan-out carries that
    member as the target author and reaches the author too.  Queued
    legs are cancelled by the fan-out (see
    :meth:`tau.node.TauNode.send_group_retraction`), so a member whose
    leg never left never receives the message.  The local copy is
    dropped only when every member received the retract: a partial
    fan-out keeps it, so the user can retry the members still holding
    the message — re-sending is idempotent, and a member who already
    deleted their copy answers ``deleted`` False.  A leg whose member
    cannot be reached is queued instead (unless ``--no-queue``): the
    sweep delivers it when the member is heard from again, and
    deposits a sealed copy with a holder meanwhile.  Queued legs keep
    the local copy too — the sweep drops it once the fan-out
    completes.
    """
    group_id = target.get("group") or ""
    if not group_id:
        raise NodeError("that group message has no group to retract from")
    moderating = target.get("direction") == "in"
    leg_sent_at = target.get("leg_sent_at")
    if leg_sent_at is None and moderating:
        # A received copy carries the legs' shared send time as its
        # sent_at (the author's send time); a sent record carries it
        # as leg_sent_at instead.
        leg_sent_at = target.get("sent_at")
    if leg_sent_at is None:
        raise NodeError(
            "that group message was sent before group retracts landed; "
            "it carries no leg send time, so it cannot be retracted"
        )
    # The author of the message being retracted: this node for an
    # author retract, the sender of the received copy for a moderator
    # retract.  Receivers resolve their copy — and key the durable
    # marker — by this hash (see TauNode._handle_retract).
    target_author = target.get("destination") or None
    timeout = float(getattr(args, "timeout", 20.0))
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    queue = not getattr(args, "no_queue", False)
    via_running_node = _send_routes_via_running_node(config)
    if via_running_node:
        # A serving node owns the data directory: retract through it
        # instead of starting a second Reticulum instance on the same
        # identity — a second instance exits when it cannot bind the
        # TCP listen port the serving node holds (see cmd_send).
        try:
            response = _retract_group_via_running_node(
                config,
                group_id,
                str(target["id"]),
                float(leg_sent_at),
                timeout=timeout,
                discover_timeout=discover_timeout,
                target_author=target_author,
                queue=queue,
            )
        except NodeError as exc:
            # With queueing on, a transport failure returns a queued
            # result instead of raising; only a refusal or a
            # --no-queue failure reaches here.
            raise NodeError(
                f"could not deliver group retract: {exc}"
            ) from exc
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            try:
                response = node.send_group_retraction(
                    group_id,
                    str(target["id"]),
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                    target_sent_at=float(leg_sent_at),
                    target_author=target_author,
                    queue=queue,
                )
            except NodeError as exc:
                # With queueing on, a transport failure returns a queued
                # result instead of raising; only a refusal or a
                # --no-queue failure reaches here.
                raise NodeError(
                    f"could not deliver group retract: {exc}"
                ) from exc
        finally:
            node.stop()

    results = response.get("results") or []
    failed = [
        r for r in results if not r.get("delivered") and not r.get("queued")
    ]
    queued = [r for r in results if r.get("queued")]
    if moderating:
        author_peer = registry.get(target_author or "")
        author_name = (
            author_peer.display_name
            if author_peer is not None
            else short_hash(target_author or "")
        )
        message_label = f"{author_name}'s group message"
    else:
        message_label = "your group message"
    if _json_mode(args):
        _emit(
            {
                "target": target["id"],
                "group": group_id,
                "ok": not failed,
                "queued": len(queued),
                "cancelled_queued": response.get("cancelled_queued") or [],
                "results": results,
                "id": response.get("id"),
            },
            True,
        )
    else:
        for result in results:
            member = result.get("to") or ""
            peer = registry.get(member)
            name = peer.display_name if peer is not None else short_hash(member)
            if result.get("queued"):
                print(
                    f"retraction queued for {name} — will deliver when "
                    "they are reachable"
                )
            elif result.get("delivered"):
                if result.get("deleted"):
                    print(
                        f"retracted {message_label} "
                        f"{_safe_id_prefix(str(target['id']))} from {name}'s inbox"
                    )
                else:
                    print(
                        f"{name} had no copy of "
                        f"{_safe_id_prefix(str(target['id']))} to retract"
                    )
            else:
                print(
                    f"could not retract from {name}: "
                    f"{result.get('error') or 'unknown error'}"
                )
        for leg_id in response.get("cancelled_queued") or []:
            # The leg is gone from this node's outbox, so no further
            # attempt goes out from here — but a sealed copy already
            # held by another peer may still arrive if the retract
            # does not reach that member (the same caveat as
            # `tau outbox cancel`).
            print(
                f"cancelled queued leg {_safe_id_prefix(str(leg_id))} — "
                "no further delivery attempts from this node; a sealed "
                "copy already held by another peer may still arrive"
            )
    if failed:
        # Keep the local sent copy: the failed members still hold the
        # message, and the record carries the leg ids and send time a
        # retry needs.  A failed retract never erases the local copy.
        raise NodeError(
            "group retract reached only some members: "
            + "; ".join(
                f"{short_hash(str(r.get('to') or ''))}: "
                f"{r.get('error') or 'unknown error'}"
                for r in failed
            )
            + "; retry `tau retract` when they are online"
        )
    if queued:
        # Queued legs keep the local copy too: the sweep drops it once
        # the fan-out completes (see TauNode._deliver_queued_retract),
        # and until then the user can retry the members still holding
        # the message.  The retract itself is stored, not failed.
        return _EXIT_OK

    # Every member received the retract: drop this node's own sent copy
    # too, so the message is gone on all sides.  A local delete failure
    # is reported, not hidden — the retract itself already succeeded.
    try:
        inbox.delete(str(target["id"]))
    except InboxError as exc:
        raise NodeError(
            "group retract delivered but the local copy could not be deleted:"
            f" {exc}"
        ) from exc
    # The sent record lives in mailbox.json, not the inbox: drop it
    # too, or `tau messages --sent` keeps showing the retracted text.
    # Each leg has its own sent record, so drop every leg id.
    mailbox = Mailbox(config.mailbox_path)
    try:
        for leg_id in target.get("leg_ids") or [target["id"]]:
            mailbox.discard_sent(str(leg_id))
        # Record the retraction durably too, BEFORE telling the serving
        # console, so the bridge drops the console's outbound legs when
        # it loads after a retract that ran while `tau serve` was down
        # (see Mailbox.record_sent_group_retraction) — and so a console
        # notification that hangs or fails cannot leave the retraction
        # unrecorded.  Only on a full fan-out: a partial fan-out keeps
        # the local copy — and the console legs — so the retract stays
        # retryable.
        mailbox.record_sent_group_retraction(group_id, float(leg_sent_at))
    except Exception:
        # The remote retract and inbox delete already succeeded, so the
        # serving console must still drop its copy even though the
        # durable record failed; then surface the original failure.
        if via_running_node:
            _drop_console_copy_via_running_node(
                config,
                group=group_id,
                target_sent_at=float(leg_sent_at),
            )
        raise
    if via_running_node:
        # The inbox copy is gone and the marker is durable, so the
        # serving console may drop its legs now.
        _drop_console_copy_via_running_node(
            config,
            group=group_id,
            target_sent_at=float(leg_sent_at),
        )
    return _EXIT_OK


def _edit_group_message(
    args: argparse.Namespace,
    config: TauConfig,
    inbox: Inbox,
    registry: PeerRegistry,
    target: Dict[str, Any],
    cleaned: str,
) -> int:
    """Edit a sent group message, fanning out to every member.

    Queued legs are cancelled by the fan-out (see
    :meth:`tau.node.TauNode.send_group_edit`), so a member whose
    leg never left never receives the message.  The local sent copy
    updates only when every member received the edit: a partial
    fan-out keeps it, so the user can retry the members still
    holding the old text — re-sending is idempotent, and a member
    who already applied a newer edit answers ``edited`` False.
    """
    group_id = target.get("group") or ""
    if not group_id:
        raise NodeError("that group message has no group to edit in")
    leg_sent_at = target.get("leg_sent_at")
    if leg_sent_at is None:
        raise NodeError(
            "that group message was sent before group edits landed; "
            "it carries no leg send time, so it cannot be edited"
        )
    timeout = float(getattr(args, "timeout", 20.0))
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    queue = not getattr(args, "no_queue", False)
    via_running_node = _send_routes_via_running_node(config)
    if via_running_node:
        # A serving node owns the data directory: edit through it
        # instead of starting a second Reticulum instance on the same
        # identity — a second instance exits when it cannot bind the
        # TCP listen port the serving node holds (see cmd_send).
        try:
            response = _edit_group_via_running_node(
                config,
                group_id,
                str(target["id"]),
                cleaned,
                float(leg_sent_at),
                timeout=timeout,
                discover_timeout=discover_timeout,
                queue=queue,
            )
        except NodeError as exc:
            # With queueing on, a transport failure returns queued
            # legs instead of raising; only a refusal or a
            # --no-queue failure reaches here.
            raise NodeError(
                f"could not deliver group edit: {exc}"
            ) from exc
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            try:
                response = node.send_group_edit(
                    group_id,
                    str(target["id"]),
                    cleaned,
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                    target_sent_at=float(leg_sent_at),
                    queue=queue,
                )
            except NodeError as exc:
                # With queueing on, a transport failure returns queued
                # legs instead of raising; only a refusal or a
                # --no-queue failure reaches here.
                raise NodeError(
                    f"could not deliver group edit: {exc}"
                ) from exc
        finally:
            node.stop()

    results = response.get("results") or []
    failed = [
        r for r in results if not r.get("delivered") and not r.get("queued")
    ]
    if _json_mode(args):
        _emit(
            {
                "target": target["id"],
                "group": group_id,
                # Success matches the exit status: queued legs are
                # accepted for delivery, so they do not fail the edit.
                "ok": not failed and not response.get("local_errors"),
                "queued": sum(1 for r in results if r.get("queued")),
                "cancelled_queued": response.get("cancelled_queued") or [],
                "results": results,
                "id": response.get("id"),
            },
            True,
        )
    else:
        for result in results:
            member = result.get("to") or ""
            peer = registry.get(member)
            name = peer.display_name if peer is not None else short_hash(member)
            if result.get("delivered"):
                if result.get("deferred"):
                    # The member's leg is still scheduled: the edit
                    # rides the re-queued leg instead of a live edit,
                    # so the replacement text stays unsent until then.
                    print(
                        f"edit is queued for {name} — it goes out with "
                        "the scheduled message at its time"
                    )
                elif result.get("edited"):
                    print(
                        f"edited your group message "
                        f"{_safe_id_prefix(str(target['id']))} for {name}"
                    )
                else:
                    print(
                        f"edit was not applied to {name}'s copy of "
                        f"{_safe_id_prefix(str(target['id']))} "
                        "(unknown message, or a newer edit already applied)"
                    )
            elif result.get("queued"):
                print(f"edit queued for {name}")
            else:
                print(
                    f"could not edit for {name}: "
                    f"{result.get('error') or 'unknown error'}"
                )
        requeued = set(response.get("requeued") or [])
        for leg_id in response.get("cancelled_queued") or []:
            if str(leg_id) in requeued:
                # The leg went back in the outbox carrying the edited
                # text, so the offline member still receives the
                # message — edited — on a later retry.
                print(
                    f"re-queued leg {_safe_id_prefix(str(leg_id))} "
                    "with the edited text"
                )
                continue
            # The leg is gone from this node's outbox, so no further
            # attempt goes out from here — but a sealed copy already
            # held by another peer may still arrive if the edit does
            # not reach that member (the same caveat as
            # `tau outbox cancel`).
            print(
                f"cancelled queued leg {_safe_id_prefix(str(leg_id))} — "
                "no further delivery attempts from this node; a sealed "
                "copy already held by another peer may still arrive"
            )
    if failed:
        # Keep the local sent copy: the failed members still hold the
        # old text, and the record carries the leg ids and send time a
        # retry needs.  A failed edit never rewrites the local copy.
        raise NodeError(
            "group edit reached only some members: "
            + "; ".join(
                f"{short_hash(str(r.get('to') or ''))}: "
                f"{r.get('error') or 'unknown error'}"
                for r in failed
            )
            + "; retry `tau edit` when they are online"
        )
    local_errors = response.get("local_errors") or []
    if local_errors:
        # Every member received the edit, but this node's own copies
        # did not update: report it instead of a silent success, so
        # the edit stays retryable — a retry sends a newer edit and
        # retries the local update.
        raise NodeError(
            "group edit delivered but the local copy could not be updated: "
            + "; ".join(str(e) for e in local_errors)
            + "; retry `tau edit`"
        )
    return _EXIT_OK


def cmd_retract(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        target = inbox.resolve(args.message)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not target.get("id"):
        raise NodeError("that message has no id to retract")
    if target.get("direction") != "out":
        # A group creator may retract a member's message (moderation):
        # a received group message is retractable when this node
        # created the group.  Anything else still needs a sent copy.
        if target.get("kind") != "group" or not _is_group_creator(
            config, target
        ):
            raise NodeError(
                "that message was not sent by this node;"
                " only your own sent messages can be retracted"
            )
    if target.get("kind") == "group":
        return _retract_group_message(args, config, inbox, registry, target)
    recipient = target.get("destination") or ""
    if not recipient:
        raise NodeError("that message has no known recipient to retract from")
    # A message still in the outbox has not reached its recipient yet; a
    # retract answered "not found" would let the queued copy arrive later.
    # Cancel the queued send instead.
    if Mailbox(config.mailbox_path).get_outbox(str(target["id"])) is not None:
        raise NodeError(
            "that message is still queued for delivery;"
            " use `tau outbox cancel` to stop it instead"
        )
    timeout = float(getattr(args, "timeout", 20.0))
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    queue = not getattr(args, "no_queue", False)
    via_running_node = _send_routes_via_running_node(config)
    if via_running_node:
        # A serving node owns the data directory: retract through it
        # instead of starting a second Reticulum instance on the same
        # identity — a second instance exits when it cannot bind the
        # TCP listen port the serving node holds (see cmd_send).
        try:
            response = _retract_via_running_node(
                config,
                recipient,
                str(target["id"]),
                timeout=timeout,
                discover_timeout=discover_timeout,
                queue=queue,
            )
        except NodeError as exc:
            # With queueing on, a transport failure returns a queued
            # result instead of raising; only a refusal or a
            # --no-queue failure reaches here.
            raise NodeError(f"could not deliver retract: {exc}") from exc
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            try:
                response = node.send_retraction(
                    recipient,
                    str(target["id"]),
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                    queue=queue,
                )
            except NodeError as exc:
                # With queueing on, a transport failure returns a queued
                # result instead of raising; only a refusal or a
                # --no-queue failure reaches here.
                raise NodeError(f"could not deliver retract: {exc}") from exc
        finally:
            node.stop()

    if response.get("queued"):
        # The retract is stored, not delivered: the local copies stay
        # until the sweep delivers it, so the sender's history only
        # ever loses a message that actually landed.
        peer = registry.get(recipient)
        name = peer.display_name if peer is not None else short_hash(recipient)
        if _json_mode(args):
            _emit(
                {
                    "target": target["id"],
                    "to": recipient,
                    "to_name": name,
                    "queued": True,
                    "id": response.get("id"),
                    "queued_id": response.get("queued_id"),
                },
                True,
            )
        else:
            print(
                f"retraction queued — will deliver when {name} is reachable"
            )
        return _EXIT_OK

    # The peer confirmed (or already had it gone): drop this node's own
    # sent copy too, so the message is gone on both sides.  A local
    # delete failure is reported, not hidden — the retract itself
    # already succeeded.
    try:
        inbox.delete(str(target["id"]))
    except InboxError as exc:
        raise NodeError(
            "retract delivered but the local copy could not be deleted:"
            f" {exc}"
        ) from exc
    # The sent record lives in mailbox.json, not the inbox: drop it
    # too, or `tau messages --sent` keeps showing the retracted text.
    # Only after the retract and the inbox delete succeeded, so a
    # failed retract never erases the sent record.  Record the
    # retraction durably as well, BEFORE telling the serving console:
    # the console's outbound entry lives in a separate log the CLI
    # cannot reach while `tau serve` is down, and the bridge drops
    # entries carrying this marker when it loads (see
    # Mailbox.record_sent_retraction).  The marker is recorded first
    # so a console notification that hangs or fails cannot leave the
    # retraction unrecorded — the marker heals a missed drop at the
    # next serve start.
    mailbox = Mailbox(config.mailbox_path)
    try:
        mailbox.discard_sent(str(target["id"]))
        mailbox.record_sent_retraction(str(target["id"]))
    except Exception:
        # The remote retract and inbox delete already succeeded, so the
        # serving console must still drop its copy even though the
        # durable record failed; then surface the original failure.
        if via_running_node:
            _drop_console_copy_via_running_node(
                config, message_id=str(target["id"])
            )
        raise
    if via_running_node:
        # The inbox copy is gone and the marker is durable, so the
        # serving console may drop its own copy now.
        _drop_console_copy_via_running_node(
            config, message_id=str(target["id"])
        )

    peer = registry.get(recipient)
    name = peer.display_name if peer is not None else short_hash(recipient)
    if _json_mode(args):
        _emit(
            {
                "target": target["id"],
                "to": recipient,
                "to_name": name,
                "deleted": bool(response.get("deleted")),
                "id": response.get("id"),
            },
            True,
        )
    elif response.get("deleted"):
        print(
            f"retracted your message {_safe_id_prefix(str(target['id']))}"
            f" from {name}'s inbox"
        )
    else:
        print(
            f"{name} had no copy of {_safe_id_prefix(str(target['id']))}"
            " to retract"
        )
    return _EXIT_OK

def cmd_edit(args: argparse.Namespace) -> int:
    text = _read_message(args)
    try:
        cleaned = normalize_message(text)
    except ProtocolError as exc:
        raise NodeError(str(exc)) from exc
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        target = inbox.resolve(args.target)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not target.get("id"):
        raise NodeError("that message has no id to edit")
    if target.get("kind") == "group":
        if target.get("direction") != "out":
            raise NodeError(
                "that message was not sent by this node;"
                " only your own sent messages can be edited"
            )
        return _edit_group_message(args, config, inbox, registry, target, cleaned)
    if target.get("kind") == "file":
        raise NodeError("a file record has no text to edit")
    if target.get("direction") != "out":
        raise NodeError(
            "that message was not sent by this node;"
            " only your own sent messages can be edited"
        )
    author = target.get("destination") or ""
    if not author:
        raise NodeError("that message has no known recipient to edit it for")
    # A message still in the outbox has not reached its recipient yet; an
    # edit answered "not found" would let the queued copy arrive later
    # with the old text. Cancel the queued send instead.
    if Mailbox(config.mailbox_path).get_outbox(str(target["id"])) is not None:
        raise NodeError(
            "that message is still queued for delivery;"
            " use `tau outbox cancel` to stop it and send a new message instead"
        )
    timeout = float(getattr(args, "timeout", 20.0))
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    queue = not getattr(args, "no_queue", False)
    if _send_routes_via_running_node(config):
        # A serving node owns the data directory: edit through it
        # instead of starting a second Reticulum instance on the same
        # identity — a second instance exits when it cannot bind the
        # TCP listen port the serving node holds (see cmd_send).
        try:
            response = _edit_via_running_node(
                config,
                author,
                str(target["id"]),
                cleaned,
                timeout=timeout,
                discover_timeout=discover_timeout,
                queue=queue,
            )
        except NodeError as exc:
            # With queueing on, a transport failure returns a queued
            # result instead of raising; only a refusal or a
            # --no-queue failure reaches here.
            raise NodeError(f"could not deliver edit: {exc}") from exc
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            try:
                response = node.send_edit(
                    author,
                    str(target["id"]),
                    cleaned,
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                    queue=queue,
                )
            except NodeError as exc:
                # With queueing on, an unreachable peer returns a queued
                # result instead of raising; only a refusal or a
                # --no-queue failure reaches here.
                raise NodeError(f"could not deliver edit: {exc}") from exc
        finally:
            node.stop()

    peer = registry.get(author)
    name = peer.display_name if peer is not None else target.get("from") or author
    if response.get("queued"):
        if _json_mode(args):
            _emit(
                {
                    "target": target["id"],
                    "to": author,
                    "to_name": name,
                    "text": cleaned,
                    "queued": True,
                    "queued_id": response.get("queued_id"),
                    "id": response.get("id"),
                },
                True,
            )
        else:
            print(
                f"edit queued — will deliver when {name} is reachable"
            )
        return _EXIT_OK
    if _json_mode(args):
        _emit(
            {
                "target": target["id"],
                "to": author,
                "to_name": name,
                "text": cleaned,
                "edited": bool(response.get("edited", True)),
                "id": response.get("id"),
            },
            True,
        )
    elif response.get("edited") is False:
        print(
            f"edit was not applied to {name}'s copy of "
            f"{_safe_id_prefix(str(target['id']))} "
            "(unknown message, or a newer edit already applied)"
        )
    else:
        print(f"edited your message to {name} {_safe_id_prefix(str(target['id']))}")
    return _EXIT_OK


def _print_more_unread(
    registry: PeerRegistry, more_unread: Dict[str, int]
) -> None:
    """Hint lines for unread mail a limited listing did not show."""
    for destination, count in sorted(more_unread.items()):
        peer = registry.get(destination)
        name = peer.display_name if peer is not None else "unknown"
        print(
            f"{count} more unread from {name} ({short_hash(destination)})"
            " — raise --limit to see them."
        )


def _print_ignored_unread(ignored_unread: int) -> None:
    """Hint line for unread mail hidden in ignored groups."""
    print(
        f"{ignored_unread} unread in ignored groups"
        " — see them with `tau messages --ignored`."
    )


def _print_archived_unread(archived_unread: int) -> None:
    """Hint line for unread mail hidden in archived conversations."""
    print(
        f"{archived_unread} unread in archived conversations"
        " — see `tau messages --archived`."
    )


def _safe_id_prefix(ref: str, length: int = 8) -> str:
    """First *length* printable characters of *ref*.

    Held ids arrive from remote senders and may contain terminal escapes
    or newlines; never print them verbatim. JSON output keeps the raw id.
    Non-printable characters are dropped from the full id first, then
    the result is truncated — so the tag is always a true prefix of the
    stored id that ``Inbox.resolve`` can match.
    """
    cleaned = "".join(char for char in ref if char.isprintable())
    return cleaned[:length]


_ANSI_SEQUENCE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def _safe_text(text: str) -> str:
    """Printable characters of *text* only.

    Error text can arrive from remote peers and may contain terminal
    escapes; never print it verbatim.  Whole ANSI sequences are
    removed first — dropping just the ESC would leave their printable
    remnants behind — then any other non-printable character is
    dropped.  JSON output keeps the raw text.
    """
    stripped = _ANSI_SEQUENCE.sub("", text)
    return "".join(char for char in stripped if char.isprintable())


def cmd_outbox_held(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    envelopes = mailbox.held_list()
    usage = mailbox.held_usage()

    def describe_sender(row: Dict[str, Any]) -> Dict[str, Any]:
        sender = row["sender"]
        peer = registry.get(bytes.fromhex(sender)) if sender else None
        return {
            "sender": sender,
            "name": peer.display_name if peer is not None else "",
            "count": row["count"],
            "bytes": row["bytes"],
        }

    def describe(envelope: Any) -> Dict[str, Any]:
        peer = registry.get(envelope.to)
        sender_peer = (
            registry.get(bytes.fromhex(envelope.depositor))
            if envelope.depositor
            else None
        )
        return {
            "id": envelope.id,
            "to": envelope.to_hex,
            "to_name": peer.display_name if peer is not None else "",
            "from": envelope.depositor,
            "from_name": sender_peer.display_name if sender_peer is not None else "",
            "stored_at": envelope.stored_at,
            "expires_at": envelope.expires_at,
            "blob_bytes": len(envelope.blob),
        }

    if _json_mode(args):
        _emit(
            {
                "held": [describe(e) for e in envelopes],
                "count": len(envelopes),
                "senders": [describe_sender(row) for row in usage],
            },
            True,
        )
    else:
        if not envelopes:
            print("Not holding any sealed messages for other peers.")
        else:
            now = time.time()
            header = ("ID", "TO", "NAME", "AGE", "EXPIRES IN")
            rows: List[tuple] = [header]
            for envelope in envelopes:
                peer = registry.get(envelope.to)
                name = peer.display_name if peer is not None else "-"
                rows.append(
                    (
                        _safe_id_prefix(envelope.id),
                        envelope.to_hex,
                        name,
                        _ago(now - envelope.stored_at),
                        _ago(envelope.expires_at - now),
                    )
                )
            widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
            for row in rows:
                print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
            print(f"\n{len(envelopes)} sealed message(s) held for other peers.")
        if usage:
            print("\nHeld per sender:")
            sender_header = ("SENDER", "NAME", "COUNT", "BYTES")
            sender_rows: List[tuple] = [sender_header]
            for row in usage:
                peer = (
                    registry.get(bytes.fromhex(row["sender"]))
                    if row["sender"]
                    else None
                )
                sender_rows.append(
                    (
                        row["sender"] if row["sender"] else "unknown",
                        peer.display_name if peer is not None else "-",
                        str(row["count"]),
                        str(row["bytes"]),
                    )
                )
            sender_widths = [
                max(len(row[i]) for row in sender_rows)
                for i in range(len(sender_header))
            ]
            for row in sender_rows:
                print(
                    "  ".join(
                        value.ljust(sender_widths[i])
                        for i, value in enumerate(row)
                    )
                )
    return _EXIT_OK


def cmd_outbox_drop_held(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        # Resolve and remove under one cross-process transaction, so the
        # reported recipient belongs to the envelope actually removed and
        # a concurrent fetch or prune cannot slip in between.
        with mailbox.transaction():
            envelope = mailbox.resolve_held(args.target)
            dropped = mailbox.drop_held(envelope.id, tombstone=True)
    except MailboxError as exc:
        raise NodeError(str(exc)) from exc
    if not dropped:
        raise NodeError(f"held message {args.target!r} is no longer present")
    peer = registry.get(envelope.to)
    name = peer.display_name if peer is not None else ""
    if _json_mode(args):
        payload = {
            "id": envelope.id,
            "to": envelope.to_hex,
            "to_name": name,
            # The delivery caveat is part of the result, not just the
            # text rendering: a copy already fetched by the recipient
            # may still be delivered.
            "note": (
                "a copy already fetched by the recipient may still be"
                " delivered; dropping stops this node handing it out again"
            ),
        }
        _emit({"dropped": payload}, True)
    else:
        print(
            f"dropped {_safe_id_prefix(envelope.id)}"
            f" — held for {name or envelope.to_hex}"
        )
        print(
            "note: a copy already fetched by the recipient may still be"
            " delivered; dropping stops this node handing it out again."
        )
    return _EXIT_OK


def cmd_outbox_cancel(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    # Resolve and remove in one transaction, so a delivery cannot land
    # between the two and leave a "cancelled" report for a leg that
    # was already sent.
    with mailbox.transaction():
        try:
            entry = mailbox.resolve_outbox(args.target)
        except MailboxError as exc:
            raise NodeError(str(exc)) from exc
        # Cancelling one leg of a queued group edit aborts the whole
        # edit: the remaining legs can no longer complete the fan-out,
        # so they go with it instead of lingering until their TTL.
        if not mailbox.remove_outbox(entry.id, abort_group_edit=True):
            raise NodeError(
                f"queued message {args.target!r} is no longer queued"
            )
        if (
            entry.kind == "retract"
            and entry.group_id is not None
            and entry.group_retract_id is not None
        ):
            # Cancelling a queued group-retract leg ends the fan-out
            # the same way a refusal does: the member keeps the
            # message, so a sibling leg's later delivery must not drop
            # the sender's local copies.
            mailbox.record_retract_cleanup_blocked(entry.group_retract_id)
    peer = registry.get(entry.to)
    name = peer.display_name if peer is not None else entry.to_name
    if _json_mode(args):
        payload = entry.to_dict()
        payload["to_name"] = name
        _emit({"cancelled": payload}, True)
    else:
        print(f"cancelled {entry.id[:8]} — queued for {name or entry.to_hex}")
        print(
            "note: sealed copies already held by other peers may still be"
            " delivered; cancelling stops this node's future attempts."
        )
    return _EXIT_OK


def cmd_outbox_edit(args: argparse.Namespace) -> int:
    text = getattr(args, "text", None)
    delay_text = getattr(args, "delay", None)
    if text is None and delay_text is None:
        print(
            "error: nothing to change — pass --text and/or --delay",
            file=sys.stderr,
        )
        return 2
    send_after = None
    if delay_text is not None:
        try:
            delay = _parse_duration(delay_text, flag="--delay")
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        if delay > MAX_MAIL_TTL - 3600.0:
            print(
                "error: --delay must be at most 29d23h (the 30d mail TTL "
                "less an hour of delivery slack)",
                file=sys.stderr,
            )
            return 2
        send_after = time.time() + delay
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        entries = mailbox.edit_scheduled_outbox(
            args.target, text=text, send_after=send_after
        )
    except MailboxError as exc:
        raise NodeError(str(exc)) from exc
    entry = entries[0]
    peer = registry.get(entry.to)
    name = peer.display_name if peer is not None else entry.to_name
    if _json_mode(args):
        _emit(
            {
                "edited": [item.to_dict() for item in entries],
                "legs": len(entries),
            },
            True,
        )
    else:
        print(
            f"edited {entry.id[:8]} — scheduled for "
            f"{name or entry.to_hex}"
        )
        if len(entries) > 1:
            print(f"  ({len(entries)} legs of the group message updated)")
        if send_after is not None:
            print(f"  now sends in {_ago(send_after - time.time())}")
    return _EXIT_OK


def cmd_outbox_retry(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    target_entry = None
    if getattr(args, "target", None):
        try:
            target_entry = mailbox.resolve_outbox(args.target)
        except MailboxError as exc:
            raise NodeError(str(exc)) from exc
    elif mailbox.counts()["outbox"] == 0:
        # No work to do: do not start a node just to report an empty queue.
        if _json_mode(args):
            _emit(
                {
                    "delivered": 0,
                    "deposited": 0,
                    "received": 0,
                    "still_queued": 0,
                    "left_queue_unconfirmed": 0,
                    "rounds_pending": False,
                },
                True,
            )
        else:
            print("Outbox is empty — nothing waiting on offline peers.")
        return _EXIT_OK

    target_to = None
    target_name = None
    if target_entry is not None:
        peer = registry.get(target_entry.to)
        if peer is None:
            raise NodeError(
                "recipient "
                f"{target_entry.to_name or target_entry.to_hex} is not in the"
                " peer registry — there is no route to retry delivery on"
            )
        target_to = target_entry.to
        target_name = peer.display_name or target_entry.to_name

    # One-shot attempt, like `tau send`: a transient node shares the data
    # directory with a running serve by design, and the flush ignores
    # backoff — the operator asked for a retry *now*, not at the next
    # scheduled attempt.  The node runs without its background mail loop
    # so the loop's first sweep cannot take the mail lock and make this
    # flush silently report nothing moved; the flush waits briefly for
    # an announce-triggered round instead.  A round that wins the race
    # still moves the mail, so the outcome below is read back from the
    # queue itself rather than trusted from the flush summary alone:
    # deliveries are confirmed by the node's delivery callback (which
    # also fires for announce-triggered rounds in this process), and
    # new holders on the entries left behind are deposits.  An entry
    # that vanished without a delivery confirmation was cancelled,
    # expired, or delivered by another process — never counted delivered.
    queued_before = {e.id: e for e in mailbox.outbox_list()}
    delivered_ids: Set[str] = set()
    node = TauNode(config, name=config.name, on_delivered=delivered_ids.add)
    node.start(mail_loop=False)
    try:
        summary = node.flush_outbox(
            timeout=float(getattr(args, "timeout", 6.0)),
            # One-shot retry-all visits every queued recipient; the
            # background sweep's peer cap does not apply here.
            max_peers=None,
            respect_backoff=False,
            to=target_to,
            message_id=target_entry.id if target_entry is not None else None,
            # A single announce-triggered round for one peer finishes in
            # seconds when the peer is reachable; only a round stuck on
            # per-request timeouts to an unreachable peer can run long,
            # and then there is nothing this flush could have delivered
            # anyway, so waiting longer buys nothing.
            wait_for_lock=60.0,
        )
        if summary.get("lock_busy"):
            raise NodeError(
                "another mail round is still running after 60s; nothing"
                " was attempted — try again, or leave a node serving"
            )
        # Let a late announce-triggered round finish before reading
        # final state, so a delivery landing just after this flush is
        # still reported instead of showing up as still queued.
        rounds_pending = not node.wait_for_mail_rounds(timeout=30.0)
    finally:
        node.stop()

    # The node's mailbox is a separate instance on the same file, so reload
    # before reporting what is still queued.
    mailbox.load()
    now = time.time()
    remaining = mailbox.outbox_list()
    remaining_ids = {e.id for e in remaining}
    delivered = sum(1 for entry_id in queued_before if entry_id in delivered_ids)
    expired = sum(
        1
        for entry_id, entry in queued_before.items()
        if entry_id not in remaining_ids
        and entry_id not in delivered_ids
        and entry.expires_at <= now
    )
    left_unconfirmed = sum(
        1
        for entry_id, entry in queued_before.items()
        if entry_id not in remaining_ids
        and entry_id not in delivered_ids
        and entry.expires_at > now
    )
    # Deposits are read back from the holders persisted on the entries
    # left in the queue, not from the flush summary: counting both would
    # report one deposit twice, and the summary cannot see deposits made
    # by a concurrent announce round.
    deposited = 0
    for entry in remaining:
        prior = queued_before.get(entry.id)
        if prior is not None:
            deposited += len(set(entry.held_by) - set(prior.held_by))
    payload: Dict[str, Any] = {
        "delivered": delivered,
        "deposited": deposited,
        "received": summary["received"],
        "still_queued": len(remaining),
        "left_queue_unconfirmed": left_unconfirmed,
        "rounds_pending": rounds_pending,
    }
    fresh = None
    if target_entry is not None:
        fresh = mailbox.get_outbox(target_entry.id)
        payload["target"] = target_entry.id
        payload["target_delivered"] = target_entry.id in delivered_ids
        if fresh is not None:
            payload["target_attempts"] = fresh.attempts
            payload["target_next_attempt_at"] = _next_try_at(fresh)

    if _json_mode(args):
        _emit(payload, True)
    elif target_entry is not None:
        label = f"{target_entry.id[:8]} for {target_name or target_entry.to_hex}"
        if fresh is None and target_entry.id in delivered_ids:
            print(f"delivered {label}")
        elif fresh is None:
            print(
                f"no longer queued: {label} — it left the queue without"
                " a delivery confirmation (cancelled, expired, or"
                " delivered by another process)"
            )
        else:
            next_try = _next_try_at(fresh)
            if next_try is None or next_try <= time.time():
                next_text = "now"
            else:
                next_text = f"in {_ago(next_try - time.time())}"
            print(
                f"still queued: {label} (attempt {fresh.attempts}, next try {next_text})"
            )
            if deposited:
                print(
                    f"  left a sealed copy with {deposited} peer(s)"
                    " for store-and-forward delivery"
                )
    else:
        print(
            f"retried {len(queued_before)} queued message(s):"
            f" {delivered} delivered,"
            f" {deposited} deposited,"
            f" {summary['received']} received"
        )
        if remaining:
            print(
                f"{len(remaining)} message(s) still queued — they will be"
                " retried automatically, or run `tau outbox retry` again"
            )
        if left_unconfirmed:
            print(
                f"{left_unconfirmed} message(s) left the queue without a"
                " delivery confirmation (cancelled, expired, or delivered"
                " by another process)"
            )
        if expired:
            print(f"{expired} message(s) expired during the retry")
    if rounds_pending and not _json_mode(args):
        print(
            "note: a mail round was still running when the retry"
            " finished; re-run `tau outbox retry` to confirm the outcome"
        )
    return _EXIT_OK


def _resolve_allowlist_peer(registry: PeerRegistry, value: str) -> str:
    """Resolve a peer name, hash or prefix to a destination hash.

    Aliases are excluded on purpose: the allow-list is a security
    boundary, so a local label must never stand in for the peer's
    asserted name.
    """
    peer = registry.resolve(value, include_alias=False)
    if peer is not None:
        return peer.hexhash
    try:
        return clean_destination(value)
    except AllowListError:
        raise AllowListError(f"unknown peer or destination: {value!r}") from None


def _announce_interval_value(text: str) -> float:
    """Parse the `tau config announce-interval` argument (see #619)."""
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"invalid announce interval: {text!r}"
        ) from None
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError(
            "announce interval must be 0 or more seconds"
        )
    if value > ANNOUNCE_INTERVAL_MAX:
        raise argparse.ArgumentTypeError(
            f"announce interval must be at most {ANNOUNCE_INTERVAL_MAX:g} seconds"
        )
    return value


def cmd_config(args: argparse.Namespace) -> int:
    """Show or change this node's own settings (stored in node.json)."""
    config = _make_config(args)
    json_output = _json_mode(args)
    command = getattr(args, "config_command", None)

    if command == "accept-files":
        enabled = getattr(args, "state", "on") == "on"
        set_node_setting(config.data_dir, "accept_files", enabled)
        state = "on" if enabled else "off"
        if json_output:
            _emit({"accept_files": enabled}, True)
        else:
            print(f"Accept files: {state}")
            if not enabled:
                print("  this node refuses file transfers unless the sender")
                print("  is on its DM allow-list")
        return _EXIT_OK

    if command == "read-receipts":
        enabled = getattr(args, "state", "on") == "on"
        set_node_setting(config.data_dir, "send_read_receipts", enabled)
        state = "on" if enabled else "off"
        if json_output:
            _emit({"send_read_receipts": enabled}, True)
        else:
            print(f"Read receipts: {state}")
            if not enabled:
                print("  this node will not send read receipts; senders see")
                print("  its messages as delivered, never read")
        return _EXIT_OK

    if command == "tan-sync-depth":
        depth_arg = getattr(args, "depth", None)
        if depth_arg is None:
            depth = tan_sync_depth(config.data_dir)
            if json_output:
                _emit({"tan_sync_depth": depth}, True)
            else:
                print(f"Tan sync depth: {depth}")
                print("  1 = followed authors only")
                print("  2 = plus the authors they follow (default)")
            return _EXIT_OK
        depth = int(depth_arg)
        set_node_setting(config.data_dir, "tan_sync_depth", depth)
        if json_output:
            _emit({"tan_sync_depth": depth}, True)
        else:
            print(f"Tan sync depth: {depth}")
            if depth == 1:
                print("  sync pulls followed authors only")
            else:
                print("  sync also pulls the authors they follow")
        return _EXIT_OK

    if command == "announce-interval":
        seconds_arg = getattr(args, "seconds", None)
        if seconds_arg is None:
            interval = announce_interval(config.data_dir)
            if json_output:
                _emit({"announce_interval": interval}, True)
            else:
                if interval == 0:
                    print("Announce interval: off")
                    print("  automatic announces are disabled")
                else:
                    print(f"Announce interval: {interval:g} seconds")
                    print("  0 disables automatic announces (default 300)")
            return _EXIT_OK
        interval = float(seconds_arg)
        set_node_setting(config.data_dir, "announce_interval", interval)
        if json_output:
            _emit({"announce_interval": interval}, True)
        else:
            if interval == 0:
                print("Announce interval: off")
                print("  this node announces only on startup (or when asked)")
            else:
                print(f"Announce interval: {interval:g} seconds")
                print("  a running `tau serve` picks this up at its next cycle")
        return _EXIT_OK

    settings = {
        "accept_files": accept_files_enabled(config.data_dir),
        "send_read_receipts": send_read_receipts_enabled(config.data_dir),
        "tan_sync_depth": tan_sync_depth(config.data_dir),
        "announce_interval": announce_interval(config.data_dir),
    }
    if json_output:
        _emit(settings, True)
    else:
        accept_state = "on" if settings["accept_files"] else "off"
        receipts_state = "on" if settings["send_read_receipts"] else "off"
        interval_state = (
            "off"
            if settings["announce_interval"] == 0
            else f"{settings['announce_interval']:g} seconds"
        )
        print(f"Settings ({config.data_dir / 'node.json'}):")
        print(f"  accept files: {accept_state}")
        print(f"  send read receipts: {receipts_state}")
        print(f"  tan sync depth: {settings['tan_sync_depth']}")
        print(f"  announce interval: {interval_state}")
        print("  change with: tau config accept-files on|off")
        print("  change with: tau config read-receipts on|off")
        print("  change with: tau config tan-sync-depth 1|2")
        print("  change with: tau config announce-interval <seconds>")
    return _EXIT_OK


def cmd_allowlist(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    command = getattr(args, "allowlist_command", None)
    registry = PeerRegistry(config.peers_path)
    changed = False

    if command in ("add", "remove", "on", "off"):
        # Reload inside the lock: two allow-list commands racing
        # must not discard each other's change to a different sender.
        with allowlist_lock(config.allowlist_path):
            try:
                allowlist = load_allowlist(config.allowlist_path)
            except AllowListError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return _EXIT_ERROR
            if command == "add":
                destination = _resolve_allowlist_peer(registry, args.peer)
                if destination not in allowlist.allow:
                    allowlist = allowlist.with_added(destination)
                    changed = True
            elif command == "remove":
                destination = _resolve_allowlist_peer(registry, args.peer)
                if destination in allowlist.allow:
                    allowlist = allowlist.with_removed(destination)
                    changed = True
            elif command == "on":
                if not allowlist.enabled:
                    allowlist = allowlist.with_enabled(True)
                    changed = True
            elif command == "off":
                if allowlist.enabled:
                    allowlist = allowlist.with_enabled(False)
                    changed = True
            if changed:
                save_allowlist(config.allowlist_path, allowlist)
    else:
        try:
            allowlist = load_allowlist(config.allowlist_path)
        except AllowListError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR

    def describe(destination: str) -> Dict[str, Any]:
        peer = registry.get(destination)
        return {
            "destination": destination,
            # The asserted name, not the local alias: the operator must
            # see who was actually allowed, and an alias could mask that.
            "name": (
                (peer.name or short_hash(peer.hexhash))
                if peer is not None
                else ""
            ),
        }

    if json_output:
        payload = allowlist.to_dict()
        payload["entries"] = [describe(value) for value in sorted(allowlist.allow)]
        payload["path"] = str(config.allowlist_path)
        _emit(payload, True)
    else:
        state = "enforced" if allowlist.enabled else "off"
        print(f"DM allow-list ({config.allowlist_path}): {state}")
        if not allowlist.allow:
            print("  no senders listed — when enforced, nobody may message this node")
        for entry in sorted(allowlist.allow):
            peer = registry.get(entry)
            name = (
                f" ({peer.name or short_hash(peer.hexhash)})"
                if peer is not None
                else ""
            )
            print(f"  {entry}{name}")
        if changed:
            print("  (a running serve applies saved changes when it restarts)")
    return _EXIT_OK


def cmd_block(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)

    # Forms: `tau block <peer>` blocks, `tau unblock <peer>` unblocks,
    # `tau block list` lists, and `tau block add/remove <peer>` keep
    # working as the allow-list-shaped aliases.  A peer literally
    # named "list", "add" or "remove" is blocked via `block add <name>`.
    command = getattr(args, "block_command", None)
    target = getattr(args, "target", None)
    peer_arg = getattr(args, "peer", None)
    registry = PeerRegistry(config.peers_path)
    changed = False

    # The whole load-modify-save runs under the block-list lock, so a
    # concurrent decline-and-block cannot save from the same stale
    # snapshot and silently drop this change (or vice versa).
    with blocklist_edit_lock(config.blocklist_path):
        try:
            blocklist = load_blocklist(config.blocklist_path)
        except BlockListError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR

        if command == "remove":
            destination = _resolve_allowlist_peer(registry, peer_arg)
            if destination in blocklist.blocked:
                blocklist = blocklist.with_removed(destination)
                changed = True
        elif target in (None, "list") and peer_arg is None:
            pass  # list
        elif target == "add":
            if peer_arg is None:
                raise NodeError("`tau block add` needs a peer")
            destination = _resolve_allowlist_peer(registry, peer_arg)
            if destination not in blocklist.blocked:
                blocklist = blocklist.with_added(destination)
                changed = True
        elif target == "remove":
            if peer_arg is None:
                raise NodeError("`tau block remove` needs a peer")
            destination = _resolve_allowlist_peer(registry, peer_arg)
            if destination in blocklist.blocked:
                blocklist = blocklist.with_removed(destination)
                changed = True
        elif peer_arg is not None:
            raise NodeError(f"unexpected argument: {peer_arg!r}")
        else:
            destination = _resolve_allowlist_peer(registry, target)
            if destination not in blocklist.blocked:
                blocklist = blocklist.with_added(destination)
                changed = True

        if changed:
            save_blocklist(config.blocklist_path, blocklist)

    def describe(destination: str) -> Dict[str, Any]:
        peer = registry.get(destination)
        return {
            "destination": destination,
            "name": peer.display_name if peer is not None else "",
        }

    if json_output:
        payload = blocklist.to_dict()
        payload["entries"] = [describe(value) for value in sorted(blocklist.blocked)]
        payload["path"] = str(config.blocklist_path)
        _emit(payload, True)
    else:
        print(f"Block-list ({config.blocklist_path}):")
        if not blocklist.blocked:
            print("  nobody blocked")
        for entry in sorted(blocklist.blocked):
            peer = registry.get(entry)
            name = f" ({peer.display_name})" if peer is not None else ""
            print(f"  {entry}{name}")
        if changed:
            print("  (a running serve applies saved changes when it restarts)")
    return _EXIT_OK


def cmd_ping(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        response = node.ping(
            args.to,
            timeout=float(getattr(args, "timeout", 15.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(response, True)
    else:
        rtt = response.get("_rtt")
        latency = f" ({rtt * 1000:.0f} ms)" if isinstance(rtt, (int, float)) else ""
        print(f"pong from {response.get('server', args.to)}{latency}")
    return _EXIT_OK


def cmd_trace(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        result = node.trace(
            args.peer,
            timeout=float(getattr(args, "timeout", 15.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(result, True)
    else:
        print(f"trace to {result['peer']} ({result['destination']})")
        if result["has_path"]:
            print(f"  path: known, {result['hops']} hop(s)")
        else:
            print("  path: none known")
        if result["rtt_ms"] is not None:
            print(f"  probe: pong in {result['rtt_ms']:.0f} ms")
        elif result["probe_error"]:
            print(f"  probe: no reply ({result['probe_error']})")
        print(f"  last seen: {_ago(time.time() - result['last_seen'])} ago")
        print(f"  note: {result['note']}")
    return _EXIT_OK if result["reachable"] else _EXIT_ERROR


def cmd_mesh(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        rows = node.mesh_snapshot()
    finally:
        node.stop()

    if _json_mode(args):
        _emit({"peers": rows, "count": len(rows)}, True)
        return _EXIT_OK
    if not rows:
        print("No peers known. Run `tau discover` or `tau serve` first.")
        return _EXIT_OK
    now = time.time()
    header = ("NAME", "DESTINATION", "HOPS", "REACHABLE", "LAST SEEN")
    table: List[tuple] = [header]
    for row in rows:
        table.append(
            (
                row["name"],
                row["destination"],
                str(row["hops"]) if row["hops"] is not None else "—",
                "yes" if row["reachable"] else "no",
                _ago(now - row["last_seen"]),
            )
        )
    widths = [max(len(row[i]) for row in table) for i in range(len(header))]
    for row in table:
        print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
    print(
        "\nReachability is a snapshot from Reticulum's path table, not a "
        "promise — paths change as the mesh shifts."
    )
    return _EXIT_OK


def cmd_doctor(args: argparse.Namespace) -> int:
    """Report the local data directory's health without changing it."""
    config = _make_config(args)
    checks = run_checks(config)
    if _json_mode(args):
        _emit(
            {"ok": all_ok(checks), "checks": [check.to_dict() for check in checks]},
            True,
        )
    else:
        for check in checks:
            print(f"{check.status:<4} {check.name}: {check.detail}")
        failed = [check for check in checks if check.failed]
        if failed:
            print(f"\n{len(failed)} check(s) failed")
        else:
            print("\ndata directory healthy")
    return _EXIT_ERROR if not all_ok(checks) else _EXIT_OK


def cmd_info(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        response = node.request_info(
            args.to,
            timeout=float(getattr(args, "timeout", 15.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(response, True)
    else:
        for key in (
            "name",
            "destination",
            "version",
            "rns_version",
            "protocol",
            "uptime",
            "peers",
            "caller",
        ):
            if key in response:
                print(f"{key + ':':<13} {response[key]}")
        lxmf_info = response.get("lxmf") or {}
        if lxmf_info.get("enabled"):
            print(f"{'lxmf:':<13} delivery {lxmf_info.get('delivery')}")
            if lxmf_info.get("propagation"):
                print(
                    f"{'lxmf:':<13} propagation {lxmf_info.get('propagation')}"
                )
            else:
                print(f"{'lxmf:':<13} propagation disabled")
    return _EXIT_OK


# -- entry point -----------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args)

    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        return _EXIT_INTERRUPTED
    except (NodeError, PeerError, IdentityError, ProtocolError) as exc:
        print(f"tau: error: {exc}", file=sys.stderr)
        return _EXIT_ERROR
    except (OSError, ValueError) as exc:
        print(f"tau: error: {exc}", file=sys.stderr)
        return _EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())


def _cmd_messages_sent(
    args: argparse.Namespace,
    config: TauConfig,
    registry: PeerRegistry,
    limit: Optional[int],
) -> int:
    """List messages this node sent, with their read state.

    A group send folds into one row: its legs share one send, so the
    author sees who has read it — ``seen by Alice, Bob · delivered to
    Carol · queued for Dave`` — instead of one identical row per
    member.  The fold is keyed on the sender's own group inbox record
    (``leg_ids``); a leg whose sent record was evicted counts as
    unknown, and a send whose inbox record is gone (retracted,
    deleted) keeps today's per-leg rows.
    """
    mailbox = Mailbox(config.mailbox_path)
    inbox = Inbox(config.inbox_path)
    peer_hash = None
    if getattr(args, "peer", None):
        peer_hash = _resolve_sent_peer(registry, args.peer)
    entries = mailbox.sent_list()
    queued_ids = {entry.id for entry in mailbox.outbox_list()}
    sent_by_id = {entry["id"]: entry for entry in entries}
    fold_by_leg: Dict[str, Dict[str, Any]] = {}
    for record in inbox.list_messages(
        direction="out", kind="group", limit=None
    ):
        leg_ids = record.get("leg_ids")
        if not isinstance(leg_ids, list):
            continue
        for leg_id in leg_ids:
            if isinstance(leg_id, str) and leg_id:
                fold_by_leg.setdefault(leg_id, record)

    def leg_state(entry: Dict[str, Any]) -> str:
        read_at = entry.get("read_at")
        # ``delivered_at`` is set only once a delivery was confirmed.
        # Records from before it was tracked have no such key and count
        # as delivered.
        delivered_at = entry.get("delivered_at", entry.get("sent_at"))
        if entry["id"] in queued_ids:
            return "queued"
        if read_at is not None:
            return "read"
        if delivered_at is not None:
            return "delivered"
        # Cancelled or expired before any delivery was confirmed.
        return "unknown"

    def describe(entry: Dict[str, Any]) -> Dict[str, Any]:
        peer = registry.get(entry.get("to") or "")
        return {
            "id": entry["id"],
            "to": entry.get("to"),
            "to_name": peer.display_name if peer is not None else "",
            "text": entry.get("text", ""),
            "sent_at": entry.get("sent_at"),
            "read_at": entry.get("read_at"),
            "state": leg_state(entry),
        }

    def describe_group(record: Dict[str, Any]) -> Dict[str, Any]:
        legs = []
        for leg_id in record.get("leg_ids") or []:
            sent = sent_by_id.get(leg_id)
            if sent is None:
                # The sent record was evicted (see MAX_SENT): the leg
                # went out, but who it went to is no longer known, so
                # it counts as unknown without a name.
                legs.append(
                    {
                        "id": leg_id,
                        "to": None,
                        "to_name": "",
                        "state": "unknown",
                        "read_at": None,
                    }
                )
                continue
            legs.append(describe(sent))
        states = {leg["state"] for leg in legs}
        if states == {"read"}:
            state = "read"
        elif states <= {"read", "delivered"}:
            state = "delivered"
        elif states == {"queued"}:
            state = "queued"
        else:
            state = "partial"
        return {
            "id": record.get("id"),
            "group": record.get("group"),
            "group_name": record.get("group_name") or "group",
            "text": record.get("text", ""),
            "sent_at": record.get("sent_at"),
            "state": state,
            "legs": legs,
        }

    # Walk the sent records oldest first, as before: a group send's
    # legs share one send time, so they sit together and the fold
    # takes the first leg's place.  A fold with no leg left in the
    # sent records (all evicted) never appears — exactly like the
    # per-leg rows it replaces.
    described: List[Dict[str, Any]] = []
    folded: Set[str] = set()
    for entry in entries:
        record = fold_by_leg.get(entry["id"])
        if record is not None:
            key = str(record.get("id")) + str(record.get("sent_at"))
            if key in folded:
                continue
            folded.add(key)
            described.append(describe_group(record))
        else:
            described.append(describe(entry))
    described.reverse()  # newest first
    if peer_hash is not None:
        described = [
            entry
            for entry in described
            if (
                entry.get("to") == peer_hash
                if "group" not in entry
                else any(
                    leg.get("to") == peer_hash for leg in entry.get("legs") or []
                )
            )
        ]
    if limit is not None:
        described = described[: max(0, limit)]
    if _json_mode(args):
        _emit({"messages": described, "count": len(described)}, True)
        return _EXIT_OK
    if not described:
        print("No messages sent yet.")
        return _EXIT_OK
    for entry in described:
        when = _format_message_time(float(entry.get("sent_at") or 0))
        if "group" in entry:
            seen = [
                leg["to_name"] or short_hash(leg["to"] or "")
                for leg in entry.get("legs") or []
                if leg["state"] == "read"
            ]
            delivered = [
                leg["to_name"] or short_hash(leg["to"] or "")
                for leg in entry.get("legs") or []
                if leg["state"] == "delivered"
            ]
            queued = [
                leg["to_name"] or short_hash(leg["to"] or "")
                for leg in entry.get("legs") or []
                if leg["state"] == "queued"
            ]
            unknown = sum(
                1 for leg in entry.get("legs") or [] if leg["state"] == "unknown"
            )
            parts = []
            if seen:
                parts.append("seen by " + ", ".join(seen))
            if delivered:
                parts.append("delivered to " + ", ".join(delivered))
            if queued:
                parts.append("queued for " + ", ".join(queued))
            if unknown:
                parts.append(f"{unknown} unknown")
            state = " · ".join(parts) or "unknown"
            print(f"[{when}] to {entry.get('group_name')} — {state}")
        else:
            name = entry["to_name"] or short_hash(entry["to"] or "")
            if entry["state"] == "read":
                state = f"read {_format_message_time(float(entry['read_at'] or 0))}"
            else:
                state = entry["state"]
            print(f"[{when}] to {name} — {state}")
        for line in str(entry.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK
