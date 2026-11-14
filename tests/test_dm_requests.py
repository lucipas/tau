"""DM requests: hold denied DMs for review, then accept or decline."""

from __future__ import annotations

import base64
import json
import os
import stat
import time
from types import SimpleNamespace

import pytest
import RNS

from tau import cli
from tau.allowlist import AllowList, load_allowlist, save_allowlist
from tau.blocklist import BlockList, save_blocklist
from tau.config import TauConfig
from tau.dm_requests import (
    DM_REQUEST_TTL,
    MAX_DM_REQUESTS,
    DmRequest,
    DmRequestsError,
    accept_dm_request,
    list_dm_requests,
    load_dm_requests,
    record_dm_request,
    remove_dm_request,
    resolve_dm_request,
    save_dm_requests,
)
from tau.identity import destination_hash
from tau.inbox import Inbox, InboxError
from tau.mailbox import Mailbox
from tau.node import TauNode
from tau.peers import PeerRegistry
from tau.protocol import decode, encode, encode_message, retract_dict


ALICE = "aa" * 16
BOB = "bb" * 16


def make_request(request_id="req-1", sender=BOB, received_at=1000.0, **overrides):
    fields = {
        "id": request_id,
        "sender": sender,
        "text": "hello, let me in",
        "kind": "message",
        "sent_at": 999.0,
        "received_at": received_at,
        "request_expires_at": received_at + DM_REQUEST_TTL,
    }
    fields.update(overrides)
    return DmRequest(**fields)


# -- store ------------------------------------------------------------------

def test_missing_file_loads_empty(tmp_path):
    assert load_dm_requests(tmp_path / "dm-requests.json") == []
    assert list_dm_requests(tmp_path / "dm-requests.json") == []


def test_record_and_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "dm-requests.json"
    assert record_dm_request(path, make_request()) is True
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_dm_requests(path)
    assert loaded == [make_request()]
    assert json.loads(path.read_text())["requests"][0]["sender"] == BOB


def test_duplicate_id_keeps_first_receipt(tmp_path):
    path = tmp_path / "dm-requests.json"
    assert record_dm_request(path, make_request(received_at=1000.0), now=1000.0) is True
    assert (
        record_dm_request(path, make_request(received_at=2000.0, text="retry"), now=2000.0)
        is False
    )
    loaded = load_dm_requests(path)
    assert len(loaded) == 1
    assert loaded[0].received_at == 1000.0
    assert loaded[0].text == "hello, let me in"


def test_cap_drops_oldest(tmp_path):
    path = tmp_path / "dm-requests.json"
    for index in range(MAX_DM_REQUESTS + 1):
        record_dm_request(
            path,
            make_request(request_id=f"req-{index}", received_at=1000.0 + index),
            now=2000.0,
        )
    loaded = load_dm_requests(path)
    assert len(loaded) == MAX_DM_REQUESTS
    assert loaded[0].id == "req-1"
    assert loaded[-1].id == f"req-{MAX_DM_REQUESTS}"


def test_expired_pruned_on_record_and_list(tmp_path):
    path = tmp_path / "dm-requests.json"
    record_dm_request(path, make_request("old", received_at=1000.0), now=1000.0)
    # "old" expires at 1000 + 7d; recording past that prunes it.
    assert record_dm_request(
        path, make_request("new", received_at=700000.0), now=700000.0
    )
    loaded = load_dm_requests(path)
    assert [entry.id for entry in loaded] == ["new"]
    stale = make_request("stale", received_at=3000.0)
    save_dm_requests(path, [stale])
    assert list_dm_requests(path, now=stale.request_expires_at + 1) == []


def test_list_is_oldest_first(tmp_path):
    path = tmp_path / "dm-requests.json"
    record_dm_request(path, make_request("b", received_at=2000.0), now=2000.0)
    record_dm_request(path, make_request("a", received_at=1000.0), now=2000.0)
    assert [entry.id for entry in list_dm_requests(path, now=2000.0)] == ["a", "b"]


def test_remove_resolves_prefix_and_reports_unknown(tmp_path):
    path = tmp_path / "dm-requests.json"
    record_dm_request(path, make_request("abcdef01"), now=1000.0)
    removed = remove_dm_request(path, "abcdef", now=1000.0)
    assert removed.id == "abcdef01"
    assert load_dm_requests(path) == []
    with pytest.raises(DmRequestsError, match="no DM request"):
        remove_dm_request(path, "abcdef01", now=1000.0)


def test_resolve_ambiguous_prefix(tmp_path):
    requests = [make_request("abc-1"), make_request("abc-2")]
    with pytest.raises(DmRequestsError, match="ambiguous"):
        resolve_dm_request(requests, "abc")
    assert resolve_dm_request(requests, "abc-1").id == "abc-1"


def test_corrupt_file_raises(tmp_path):
    path = tmp_path / "dm-requests.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(DmRequestsError):
        load_dm_requests(path)
    with pytest.raises(DmRequestsError):
        DmRequest.from_dict({"id": "x", "sender": "zz" * 16})


def test_from_dict_rejects_bad_values():
    with pytest.raises(DmRequestsError):
        DmRequest.from_dict(["not", "a", "dict"])
    with pytest.raises(DmRequestsError):
        DmRequest.from_dict(make_request().to_dict() | {"sender": "not-a-hash"})
    with pytest.raises(DmRequestsError):
        DmRequest.from_dict(make_request().to_dict() | {"text": ""})
    with pytest.raises(DmRequestsError):
        DmRequest.from_dict(make_request().to_dict() | {"received_at": "soon"})


def test_message_expiry_check():
    request = make_request(expires_at=1500.0)
    assert request.message_expired(now=1499.0) is False
    assert request.message_expired(now=1500.0) is True
    assert make_request().message_expired(now=10**9) is False


# -- node enforcement ------------------------------------------------------

def make_node(tmp_path, name="holder", on_message=None):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
    )
    node.destination = SimpleNamespace(hexhash="ab" * 16, hash=b"\xab" * 16)
    return node


def call_handler(handler, data, remote_identity=None):
    return decode(
        handler("/tau/test", data, b"request-id", remote_identity, time.time())
    )


def _enable_allowlist(node, allowed=frozenset({ALICE})):
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=allowed),
    )


def test_denied_dm_is_recorded(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    _enable_allowlist(node)
    sender = RNS.Identity()
    result = call_handler(
        node._handle_message,
        encode_message("hello, let me in", message_id="req-1"),
        sender,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "not_allowed"
    assert received == []
    requests = load_dm_requests(node.config.dm_requests_path)
    assert len(requests) == 1
    assert requests[0].id == "req-1"
    assert requests[0].sender == destination_hash(sender).hex()
    assert requests[0].text == "hello, let me in"
    assert requests[0].kind == "chat"


def test_denied_retry_dedupes_to_one_request(tmp_path):
    node = make_node(tmp_path)
    _enable_allowlist(node)
    sender = RNS.Identity()
    data = encode_message("hello again", message_id="req-1")
    call_handler(node._handle_message, data, sender)
    call_handler(node._handle_message, data, sender)
    assert len(load_dm_requests(node.config.dm_requests_path)) == 1


def test_blocked_sender_leaves_no_request(tmp_path):
    node = make_node(tmp_path)
    _enable_allowlist(node)
    sender = RNS.Identity()
    save_blocklist(
        node.config.blocklist_path,
        BlockList(blocked=frozenset({destination_hash(sender).hex()})),
    )
    result = call_handler(
        node._handle_message,
        encode_message("hello", message_id="req-1"),
        sender,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "unavailable"
    assert load_dm_requests(node.config.dm_requests_path) == []


def test_allowed_sender_leaves_no_request(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    _enable_allowlist(node, frozenset({destination_hash(sender).hex()}))
    result = call_handler(
        node._handle_message,
        encode_message("hi", message_id="req-1"),
        sender,
    )
    assert result["ok"] is True
    assert len(received) == 1
    assert load_dm_requests(node.config.dm_requests_path) == []


def test_anonymous_denial_leaves_no_request(tmp_path):
    node = make_node(tmp_path)
    _enable_allowlist(node)
    result = call_handler(node._handle_message, encode_message("hi"))
    assert result["ok"] is False
    assert result["error"]["code"] == "not_allowed"
    assert load_dm_requests(node.config.dm_requests_path) == []


def test_idless_denial_leaves_no_request(tmp_path):
    node = make_node(tmp_path)
    _enable_allowlist(node)
    sender = RNS.Identity()
    data = encode(
        {"type": "message", "text": "hi", "kind": "message", "sent_at": 1.0}
    )
    result = call_handler(node._handle_message, data, sender)
    assert result["ok"] is False
    assert result["error"]["code"] == "not_allowed"
    assert load_dm_requests(node.config.dm_requests_path) == []


def test_recording_failure_keeps_the_denial(tmp_path):
    node = make_node(tmp_path)
    _enable_allowlist(node)
    # The requests path is a directory, so the store cannot be written;
    # the denial must still go out unchanged.
    os.mkdir(node.config.dm_requests_path)
    sender = RNS.Identity()
    result = call_handler(
        node._handle_message,
        encode_message("hello", message_id="req-1"),
        sender,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "not_allowed"


# -- CLI -------------------------------------------------------------------

def run_cli(*argv):
    return cli.main(list(argv))


def test_cli_requests_empty(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert run_cli("messages", "requests", "--data-dir", str(data_dir)) == 0
    assert "no DM requests" in capsys.readouterr().out


def test_cli_requests_lists_and_json(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    now = time.time()
    record_dm_request(
        config.dm_requests_path, make_request("req-1", received_at=now - 60), now=now
    )
    record_dm_request(
        config.dm_requests_path,
        make_request("req-2", sender=ALICE, received_at=now - 30),
        now=now,
    )
    assert run_cli("messages", "requests", "--data-dir", str(data_dir)) == 0
    out = capsys.readouterr().out
    assert "req-1" in out
    assert "hello, let me in" in out
    assert run_cli("messages", "requests", "--json", "--data-dir", str(data_dir)) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [entry["id"] for entry in payload["requests"]] == ["req-1", "req-2"]


def test_cli_accept_delivers_and_allowlists(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    now = time.time()
    record_dm_request(
        config.dm_requests_path,
        make_request("req-1", sender=BOB, received_at=now - 60),
        now=now,
    )
    assert run_cli("messages", "accept-request", "req-1", "--data-dir", str(data_dir)) == 0
    out = capsys.readouterr().out
    assert "accepted" in out
    assert "delivered to the inbox" in out
    assert BOB in load_allowlist(config.allowlist_path).allow
    assert load_dm_requests(config.dm_requests_path) == []
    inbox = Inbox(config.inbox_path)
    entries = inbox.list_messages(limit=None)
    assert len(entries) == 1
    assert entries[0]["id"] == "req-1"
    assert entries[0]["text"] == "hello, let me in"
    assert entries[0]["destination"] == BOB
    # A retry of the same id after acceptance is a duplicate, not a
    # second copy: the accept marked the id seen.
    assert Mailbox(config.mailbox_path).is_seen("req-1") is True


def test_cli_accept_expired_message_allowlists_without_delivering(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    now = time.time()
    record_dm_request(
        config.dm_requests_path,
        make_request(
            "req-1", sender=BOB, received_at=now - 60, expires_at=now - 10
        ),
        now=now,
    )
    assert run_cli("messages", "accept-request", "req-1", "--data-dir", str(data_dir)) == 0
    out = capsys.readouterr().out
    assert "not delivered" in out
    assert "expiry had passed" in out
    assert BOB in load_allowlist(config.allowlist_path).allow
    assert Inbox(config.inbox_path).list_messages(limit=None) == []
    assert load_dm_requests(config.dm_requests_path) == []


def test_cli_decline_drops_without_allowing(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    record_dm_request(
        config.dm_requests_path,
        make_request("req-1", received_at=time.time() - 60),
        now=time.time(),
    )
    assert run_cli("messages", "decline-request", "req", "--data-dir", str(data_dir)) == 0
    assert "declined" in capsys.readouterr().out
    assert load_dm_requests(config.dm_requests_path) == []
    assert BOB not in load_allowlist(config.allowlist_path).allow
    assert Inbox(config.inbox_path).list_messages(limit=None) == []


def test_cli_accept_unknown_errors(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert run_cli("messages", "accept-request", "nope", "--data-dir", str(data_dir)) == 1
    assert "no DM request" in capsys.readouterr().err


def _signed_retract(sender, target, message_id):
    """A retract payload signed by *sender*, as send_retraction builds it."""
    payload = retract_dict(target, message_id=message_id)
    signature = sender.sign(encode(payload))
    payload["sig"] = base64.b64encode(signature).decode("ascii")
    return encode(payload)


# -- bot-review follow-ups ---------------------------------------------------

def test_denied_quoted_dm_records_and_accepts(tmp_path, capsys):
    # A quoted DM carries in_reply_to as a dict; the store must hold
    # that shape and the accept must deliver it back intact.
    received = []
    node = make_node(tmp_path, on_message=received.append)
    _enable_allowlist(node)
    sender = RNS.Identity()
    result = call_handler(
        node._handle_message,
        encode_message(
            "quoting you",
            message_id="req-q",
            in_reply_to={"id": "orig-1", "excerpt": "abcd"},
        ),
        sender,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "not_allowed"
    requests = load_dm_requests(node.config.dm_requests_path)
    assert len(requests) == 1
    assert requests[0].in_reply_to == {"id": "orig-1", "excerpt": "abcd"}
    assert run_cli("messages", "accept-request", "req-q", "--data-dir", str(tmp_path)) == 0
    entries = Inbox(node.config.inbox_path).list_messages(limit=None)
    assert len(entries) == 1
    assert entries[0]["in_reply_to"] == {"id": "orig-1", "excerpt": "abcd"}


def test_denied_dm_with_bad_sent_at_records_as_unknown(tmp_path):
    # The protocol accepts any number for sent_at, including a
    # boolean, NaN and infinity; the store cannot reload those, so
    # the node normalizes them to an unknown send time instead.
    # NaN and infinity cannot come through the strict wire encoder,
    # so those two arrive as hand-built JSON, the way a modified
    # client would send them.
    node = make_node(tmp_path)
    _enable_allowlist(node)
    sender = RNS.Identity()
    for index, bad in enumerate((True, float("nan"), float("inf"))):
        data = json.dumps(
            {
                "type": "message",
                "text": "hi",
                "kind": "message",
                "sent_at": bad,
                "id": f"req-bad-{index}",
            }
        ).encode("utf-8")
        result = call_handler(node._handle_message, data, sender)
        assert result["ok"] is False
        assert result["error"]["code"] == "not_allowed"
    requests = load_dm_requests(node.config.dm_requests_path)
    assert len(requests) == 3
    assert all(entry.sent_at == 0.0 for entry in requests)


def test_from_dict_rejects_unrepresentable_sent_at():
    # A huge integer survives JSON but not float(): the loader must
    # raise DmRequestsError, never OverflowError.
    with pytest.raises(DmRequestsError):
        DmRequest.from_dict(make_request().to_dict() | {"sent_at": 10**400})


def test_cli_accept_refuses_blocked_sender(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    now = time.time()
    record_dm_request(
        config.dm_requests_path,
        make_request("req-1", sender=BOB, received_at=now - 60),
        now=now,
    )
    save_blocklist(config.blocklist_path, BlockList(blocked=frozenset({BOB})))
    assert run_cli("messages", "accept-request", "req-1", "--data-dir", str(data_dir)) == 1
    assert "blocked" in capsys.readouterr().err
    # Nothing was allow-listed, delivered or removed.
    assert BOB not in load_allowlist(config.allowlist_path).allow
    assert Inbox(config.inbox_path).list_messages(limit=None) == []
    assert len(load_dm_requests(config.dm_requests_path)) == 1


def test_cli_accept_refuses_unreadable_blocklist(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    now = time.time()
    record_dm_request(
        config.dm_requests_path,
        make_request("req-1", sender=BOB, received_at=now - 60),
        now=now,
    )
    (config.blocklist_path).write_text("{not json", encoding="utf-8")
    assert run_cli("messages", "accept-request", "req-1", "--data-dir", str(data_dir)) == 1
    assert "block-list is unusable" in capsys.readouterr().err
    assert len(load_dm_requests(config.dm_requests_path)) == 1


def test_cli_accept_failed_append_keeps_request_for_retry(tmp_path, capsys, monkeypatch):
    # Appending to the inbox fails on the first accept: no seen
    # marker may be left behind, so the retry delivers the held copy.
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    now = time.time()
    record_dm_request(
        config.dm_requests_path,
        make_request("req-1", sender=BOB, received_at=now - 60),
        now=now,
    )
    calls = []
    original_append = cli.Inbox.append

    def flaky(self, record, **kwargs):
        if not calls:
            calls.append(1)
            raise InboxError("disk full")
        return original_append(self, record, **kwargs)

    monkeypatch.setattr(cli.Inbox, "append", flaky)
    assert run_cli("messages", "accept-request", "req-1", "--data-dir", str(data_dir)) == 1
    assert "could not deliver" in capsys.readouterr().err
    assert len(load_dm_requests(config.dm_requests_path)) == 1
    assert Mailbox(config.mailbox_path).is_seen("req-1") is False
    assert Inbox(config.inbox_path).list_messages(limit=None) == []
    assert run_cli("messages", "accept-request", "req-1", "--data-dir", str(data_dir)) == 0
    entries = Inbox(config.inbox_path).list_messages(limit=None)
    assert len(entries) == 1
    assert entries[0]["id"] == "req-1"
    assert Mailbox(config.mailbox_path).is_seen("req-1") is True
    assert load_dm_requests(config.dm_requests_path) == []


def test_cli_accept_failed_persist_keeps_request_for_retry(tmp_path, capsys, monkeypatch):
    # The inbox persist fails on the first accept: the append must
    # raise instead of reporting success, so no seen marker is left
    # behind and the retry delivers the held copy.
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    now = time.time()
    record_dm_request(
        config.dm_requests_path,
        make_request("req-1", sender=BOB, received_at=now - 60),
        now=now,
    )
    calls = []
    original_save = cli.Inbox._save

    def flaky_save(self, *, strict=False):
        if not calls:
            calls.append(1)
            raise OSError("disk full")
        return original_save(self, strict=strict)

    monkeypatch.setattr(cli.Inbox, "_save", flaky_save)
    assert run_cli("messages", "accept-request", "req-1", "--data-dir", str(data_dir)) == 1
    assert "could not deliver" in capsys.readouterr().err
    assert len(load_dm_requests(config.dm_requests_path)) == 1
    assert Mailbox(config.mailbox_path).is_seen("req-1") is False
    assert Inbox(config.inbox_path).list_messages(limit=None) == []
    assert run_cli("messages", "accept-request", "req-1", "--data-dir", str(data_dir)) == 0
    entries = Inbox(config.inbox_path).list_messages(limit=None)
    assert len(entries) == 1
    assert entries[0]["id"] == "req-1"
    assert Mailbox(config.mailbox_path).is_seen("req-1") is True
    assert load_dm_requests(config.dm_requests_path) == []


def test_accept_dm_request_helper_shares_cli_semantics(tmp_path):
    # The shared helper the CLI and the console both call: the same
    # accept the CLI tests cover, driven directly, with the delivered
    # inbox record returned for the console feed.
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    now = time.time()
    record_dm_request(
        config.dm_requests_path,
        make_request("req-1", sender=BOB, received_at=now - 60),
        now=now,
    )
    acceptance = accept_dm_request(
        dm_requests_path=config.dm_requests_path,
        allowlist_path=config.allowlist_path,
        blocklist_path=config.blocklist_path,
        inbox=Inbox(config.inbox_path),
        mailbox=Mailbox(config.mailbox_path),
        sender_name="bob",
        ref="req-1",
    )
    assert acceptance.delivered is True
    assert acceptance.allowlisted is True
    assert acceptance.skipped is None
    assert acceptance.record is not None
    assert acceptance.record["id"] == "req-1"
    assert acceptance.record["from"] == "bob"
    assert BOB in load_allowlist(config.allowlist_path).allow
    assert [entry["id"] for entry in Inbox(config.inbox_path).list_messages()] == [
        "req-1"
    ]
    assert load_dm_requests(config.dm_requests_path) == []


def test_accept_dm_request_helper_rejects_unknown(tmp_path):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    with pytest.raises(DmRequestsError, match="no DM request"):
        accept_dm_request(
            dm_requests_path=config.dm_requests_path,
            allowlist_path=config.allowlist_path,
            blocklist_path=config.blocklist_path,
            inbox=Inbox(config.inbox_path),
            mailbox=Mailbox(config.mailbox_path),
            sender_name="bob",
            ref="nope",
        )


def test_accept_dm_request_helper_retries_after_failed_persist_on_shared_inbox(
    tmp_path, monkeypatch
):
    # The console reuses the node's Inbox across retries: a failed
    # strict persist must not leave a phantom copy behind for find to
    # return, or the retry would mark the id seen and drop the request
    # for a message that never reached disk.
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    now = time.time()
    record_dm_request(
        config.dm_requests_path,
        make_request("req-1", sender=BOB, received_at=now - 60),
        now=now,
    )
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)
    original_save = inbox._save
    calls = []

    def flaky_save(*args, **kwargs):
        if not calls:
            calls.append(1)
            raise OSError("disk full")
        return original_save(*args, **kwargs)

    monkeypatch.setattr(inbox, "_save", flaky_save)
    with pytest.raises(DmRequestsError, match="could not deliver"):
        accept_dm_request(
            dm_requests_path=config.dm_requests_path,
            allowlist_path=config.allowlist_path,
            blocklist_path=config.blocklist_path,
            inbox=inbox,
            mailbox=mailbox,
            sender_name="bob",
            ref="req-1",
        )
    assert inbox.find("req-1", peer=BOB) is None
    assert len(load_dm_requests(config.dm_requests_path)) == 1
    acceptance = accept_dm_request(
        dm_requests_path=config.dm_requests_path,
        allowlist_path=config.allowlist_path,
        blocklist_path=config.blocklist_path,
        inbox=inbox,
        mailbox=mailbox,
        sender_name="bob",
        ref="req-1",
    )
    assert acceptance.delivered is True
    assert [entry["id"] for entry in inbox.list_messages()] == ["req-1"]


def test_accept_ignores_memory_only_copy_from_failed_live_append(
    tmp_path, monkeypatch
):
    # A live append whose persist failed leaves the copy in memory
    # only. If that same message is later held as a request (the
    # sender was de-allow-listed between deliveries), the accept
    # must not trust the cached copy: it re-checks the disk and
    # stores the message for real.
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    now = time.time()
    record_dm_request(
        config.dm_requests_path,
        make_request("req-1", sender=BOB, received_at=now - 60),
        now=now,
    )
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)
    original_save = inbox._save
    calls = []

    def failing_save(*args, **kwargs):
        if not calls:
            calls.append(1)
            return False
        return original_save(*args, **kwargs)

    monkeypatch.setattr(inbox, "_save", failing_save)
    inbox.append(
        {
            "from": "bob",
            "destination": BOB,
            "text": "hello, let me in",
            "kind": "message",
            "sent_at": now - 61,
            "received_at": now - 60,
            "id": "req-1",
            "expires_at": None,
            "direction": "in",
        }
    )
    assert inbox.find("req-1", peer=BOB) is not None
    assert Inbox(config.inbox_path).list_messages(limit=None) == []
    monkeypatch.setattr(inbox, "_save", original_save)
    acceptance = accept_dm_request(
        dm_requests_path=config.dm_requests_path,
        allowlist_path=config.allowlist_path,
        blocklist_path=config.blocklist_path,
        inbox=inbox,
        mailbox=mailbox,
        sender_name="bob",
        ref="req-1",
    )
    assert acceptance.delivered is True
    assert [entry["id"] for entry in Inbox(config.inbox_path).list_messages()] == [
        "req-1"
    ]
    assert mailbox.is_seen("req-1") is True
    assert load_dm_requests(config.dm_requests_path) == []


def test_retract_of_held_request_blocks_accept(tmp_path, capsys):
    # A sender whose DM is held may still retract it: the signed
    # retract records a marker even though the sender is outside
    # the allow-list, and accepting the request then refuses to
    # deliver the withdrawn text.
    received = []
    node = make_node(tmp_path, on_message=received.append)
    _enable_allowlist(node)
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    result = call_handler(
        node._handle_message,
        encode_message("take this back later", message_id="req-1"),
        sender,
    )
    assert result["ok"] is False
    assert len(load_dm_requests(node.config.dm_requests_path)) == 1
    response = call_handler(
        node._handle_retract,
        _signed_retract(sender, "req-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert node.mailbox.is_retracted(sender_hash, "req-1") is True
    assert run_cli("messages", "accept-request", "req-1", "--data-dir", str(tmp_path)) == 0
    out = capsys.readouterr().out
    assert "not delivered" in out
    assert "retracted" in out
    assert sender_hash in load_allowlist(node.config.allowlist_path).allow
    assert Inbox(node.config.inbox_path).list_messages(limit=None) == []
    assert load_dm_requests(node.config.dm_requests_path) == []


def test_retract_of_unknown_id_from_unlisted_sender_still_denied(tmp_path):
    # The held-request exception is exact: a retract for an id with
    # no held request from this sender is denied as before, so an
    # unlisted peer cannot write markers for arbitrary ids.
    node = make_node(tmp_path)
    _enable_allowlist(node)
    sender = RNS.Identity()
    response = call_handler(
        node._handle_retract,
        _signed_retract(sender, "no-such-id", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_allowed"
    assert node.mailbox.is_retracted(destination_hash(sender).hex(), "no-such-id") is False
