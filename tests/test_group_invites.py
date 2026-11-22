"""Group invite links + request-to-join (issue #466)."""

import base64
import time
import types

import pytest
import RNS

from tau.config import TauConfig
from tau.group_invites import (
    InviteError,
    invite_status,
    invite_token_group_label,
    list_join_requests,
    load_invites,
    mint_invite,
    mint_invite_token,
    parse_invite_link,
    record_join_request,
    resolve_join_request,
    revoke_invites,
    verify_invite_link,
)
from tau.node import TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import APP_NAME, ASPECT, decode, encode, encode_message

JOINER = "carol"


def _invite_node(tmp_path, *, name="creator"):
    """A node with a destination stand-in and a fresh identity."""
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    identity = RNS.Identity()
    node.identity = identity
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    node.destination = types.SimpleNamespace(hexhash=own_hash)
    return node, identity, own_hash


def _peer_identity(name):
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    return identity, Peer(
        destination_hash=destination,
        name=name,
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )


def _group(node, own_hash, *others):
    return node.groups.create(
        "crew",
        [own_hash, *[peer.hexhash for peer in others]],
        created_by=own_hash,
    )


# --- token mint / parse / verify -------------------------------------------
# (id-keyed tokens for secret groups first, then name-keyed tokens)


def _secret_group(node, identity, own_hash, *others):
    return node.groups.create(
        "crew",
        [own_hash, *[peer.hexhash for peer in others]],
        created_by=own_hash,
        secret=True,
        identity=identity,
    )


def test_id_token_round_trip_carries_no_name():
    identity = RNS.Identity()
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    group_id = "ab" * 16
    link = mint_invite_token(
        identity, group_id=group_id, creator_hash=own_hash, mode="closed"
    )
    assert link.startswith("tau://invite/")
    token = parse_invite_link(link)
    assert token["group_id"] == group_id
    assert "group" not in token
    # The group name appears nowhere in the token at all.
    assert b"crew" not in base64.urlsafe_b64decode(
        link[len("tau://invite/") :]
    )
    verified = verify_invite_link(
        link,
        identity=identity,
        invites={"groups": {"crew": {"mode": "closed", "generation": 0}}},
        resolve_group_id=lambda gid: "crew" if gid == group_id else None,
    )
    assert verified["group"] == "crew"
    assert verified["group_id"] == group_id
    assert verified["mode"] == "closed"


def test_mint_token_needs_exactly_one_group_handle():
    identity = RNS.Identity()
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    with pytest.raises(InviteError):
        mint_invite_token(identity, creator_hash=own_hash)
    with pytest.raises(InviteError):
        mint_invite_token(
            identity,
            group_name="crew",
            group_id="ab" * 16,
            creator_hash=own_hash,
        )


def test_parse_rejects_token_with_both_or_neither_group_fields():
    identity = RNS.Identity()
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    for extra in ({"group": "crew", "group_id": "ab" * 16}, {}):
        payload = {
            "v": 1,
            "type": "group-invite",
            "creator": own_hash,
            "mode": "open",
            "generation": 0,
            "issued_at": 1000.0,
            **extra,
        }
        signature = identity.sign(encode(payload))
        token = dict(payload)
        token["sig"] = base64.b64encode(signature).decode("ascii")
        link = "tau://invite/" + base64.urlsafe_b64encode(
            encode(token)
        ).decode("ascii")
        with pytest.raises(InviteError):
            parse_invite_link(link)
    # A malformed group id fails too.
    with pytest.raises(InviteError):
        mint_invite_token(
            identity, group_id="not-an-id", creator_hash=own_hash
        )


def test_verify_id_token_needs_resolver():
    identity = RNS.Identity()
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    group_id = "ab" * 16
    link = mint_invite_token(
        identity, group_id=group_id, creator_hash=own_hash, mode="open"
    )
    invites = {"groups": {"crew": {"mode": "open", "generation": 0}}}
    # Without a resolver there is nothing to check the stored
    # mode/generation against.
    with pytest.raises(InviteError):
        verify_invite_link(link, identity=identity, invites=invites)
    # And a resolver that knows no such id fails the same way.
    with pytest.raises(InviteError):
        verify_invite_link(
            link,
            identity=identity,
            invites=invites,
            resolve_group_id=lambda gid: None,
        )


def test_revoke_invalidates_id_token(tmp_path):
    node, identity, own_hash = _invite_node(tmp_path)
    group_id = "ab" * 16
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name="crew",
        creator_hash=own_hash,
        mode="open",
        group_id=group_id,
        group_created_at=1000.0,
    )
    invites = load_invites(node.config.invites_path)
    assert verify_invite_link(
        link,
        identity=identity,
        invites=invites,
        resolve_group_id=lambda gid: "crew" if gid == group_id else None,
    )["group_id"] == group_id
    revoke_invites(node.config.invites_path, group_name="crew")
    with pytest.raises(InviteError):
        verify_invite_link(
            link,
            identity=identity,
            invites=load_invites(node.config.invites_path),
            resolve_group_id=lambda gid: "crew" if gid == group_id else None,
        )


# --- name-keyed tokens ------------------------------------------------------


def test_mint_parse_verify_round_trip():
    identity = RNS.Identity()
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    group_name = "crew"
    link = mint_invite_token(
        identity, group_name=group_name, creator_hash=own_hash, mode="closed"
    )
    assert link.startswith("tau://invite/")
    token = parse_invite_link(link)
    assert token["group"] == group_name
    assert token["mode"] == "closed"
    verified = verify_invite_link(
        link,
        identity=identity,
        invites={"groups": {group_name: {"mode": "closed", "generation": 0}}},
    )
    assert verified["group"] == group_name
    assert verified["mode"] == "closed"


def test_verify_rejects_tampered_mode_and_wrong_creator():
    identity = RNS.Identity()
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    group_name = "crew"
    link = mint_invite_token(
        identity, group_name=group_name, creator_hash=own_hash, mode="open"
    )
    token = parse_invite_link(link)
    # Tamper with the mode after signing: the signature must fail.
    tampered = dict(token)
    tampered["mode"] = "closed"
    raw = base64.urlsafe_b64encode(encode(tampered)).decode("ascii")
    with pytest.raises(InviteError):
        verify_invite_link(
            "tau://invite/" + raw,
            identity=identity,
            invites={"groups": {group_name: {"mode": "closed", "generation": 0}}},
        )
    # A different identity cannot verify the creator's token either.
    other = RNS.Identity()
    with pytest.raises(InviteError):
        verify_invite_link(
            link,
            identity=other,
            invites={"groups": {group_name: {"mode": "open", "generation": 0}}},
        )
    # And a token for an unknown group fails even with the right signer.
    with pytest.raises(InviteError):
        verify_invite_link(link, identity=identity, invites={"groups": {}})


def test_parse_rejects_garbage_links():
    for bad in ("", "not a link", "tau://invite/", "tau://invite/!!!"):
        with pytest.raises(InviteError):
            parse_invite_link(bad)


def test_revoke_invalidates_older_tokens(tmp_path):
    node, identity, own_hash = _invite_node(tmp_path)
    group_name = "crew"
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group_name,
        creator_hash=own_hash,
        mode="open",
    )
    assert invite_status(node.config.invites_path, group_name=group_name)[
        "generation"
    ] == 0
    assert verify_invite_link(
        link, identity=identity, invites=load_invites(node.config.invites_path)
    )["group"] == group_name
    generation = revoke_invites(node.config.invites_path, group_name=group_name)
    assert generation == 1
    with pytest.raises(InviteError):
        verify_invite_link(
            link,
            identity=identity,
            invites=load_invites(node.config.invites_path),
        )
    # Minting again after the revoke works with the new generation.
    fresh = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group_name,
        creator_hash=own_hash,
        mode="open",
    )
    assert verify_invite_link(
        fresh, identity=identity, invites=load_invites(node.config.invites_path)
    )["generation"] == 1


def test_invite_token_group_label():
    identity = RNS.Identity()
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    name_link = mint_invite_token(
        identity, group_name="crew", creator_hash=own_hash
    )
    id_link = mint_invite_token(
        identity, group_id="ab" * 16, creator_hash=own_hash
    )
    assert invite_token_group_label(parse_invite_link(name_link)) == "crew"
    assert (
        invite_token_group_label(parse_invite_link(id_link)) == "ab" * 16
    )


def test_mint_invite_records_alias_and_revoke_drops_it(tmp_path):
    node, identity, own_hash = _invite_node(tmp_path)
    group_id = "ab" * 16
    mint_invite(
        node.config.invites_path,
        identity,
        group_name="crew",
        creator_hash=own_hash,
        mode="open",
        group_id=group_id,
        group_created_at=1000.0,
    )
    invites = load_invites(node.config.invites_path)
    assert invites["aliases"][group_id] == {
        "name": "crew",
        "creator": own_hash,
        "created_at": 1000.0,
    }
    # A name-keyed mint records no alias.
    mint_invite(
        node.config.invites_path,
        identity,
        group_name="other",
        creator_hash=own_hash,
        mode="open",
    )
    assert set(load_invites(node.config.invites_path)["aliases"]) == {
        group_id
    }
    revoke_invites(node.config.invites_path, group_name="crew")
    assert load_invites(node.config.invites_path)["aliases"] == {}
    # And an id token without a birth time cannot be minted: the
    # alias it would need has nothing to record.
    with pytest.raises(InviteError):
        mint_invite(
            node.config.invites_path,
            identity,
            group_name="crew",
            creator_hash=own_hash,
            mode="open",
            group_id="cd" * 16,
        )


def test_mint_stores_link_for_status_and_revoke_drops_it(tmp_path):
    node, identity, own_hash = _invite_node(tmp_path)
    group_name = "crew"
    assert (
        invite_status(node.config.invites_path, group_name=group_name) is None
    )
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group_name,
        creator_hash=own_hash,
        mode="closed",
    )
    # The console shows the current link from the stored record, so
    # it must be the token just minted — not a fresh mint per view.
    status = invite_status(node.config.invites_path, group_name=group_name)
    assert status is not None
    assert status["link"] == link
    assert status["mode"] == "closed"
    assert status["generation"] == 0
    # A revoke drops the link with the generation bump: the old token
    # no longer verifies and nothing keeps advertising it.
    revoke_invites(node.config.invites_path, group_name=group_name)
    status = invite_status(node.config.invites_path, group_name=group_name)
    assert status is not None
    assert status["link"] is None
    assert status["generation"] == 1
    # Minting again stores the fresh link.
    fresh = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group_name,
        creator_hash=own_hash,
        mode="closed",
    )
    assert (
        invite_status(node.config.invites_path, group_name=group_name)["link"]
        == fresh
    )


# --- join-request store ------------------------------------------------------


def test_join_request_store_dedupes_and_resolves(tmp_path):
    path = tmp_path / "join-requests.json"
    group_name = "crew"
    peer = "1" * 32
    first = record_join_request(path, group_name, peer, now=1000.0)
    second = record_join_request(path, group_name, peer, now=2000.0)
    assert second.requested_at == 2000.0
    assert first.peer == second.peer
    listed = list_join_requests(path, group_name=group_name, now=2000.0)
    assert len(listed) == 1
    assert listed[0].peer == peer
    # Expired requests are pruned from listings.
    assert list_join_requests(path, group_name=group_name, now=2000.0 + 8 * 86400) == []
    assert resolve_join_request(path, group_name, peer) is True
    assert resolve_join_request(path, group_name, peer) is False
    assert list_join_requests(path, group_name=group_name) == []


# --- node: live join requests -----------------------------------------------


def test_open_group_join_adds_member(tmp_path, monkeypatch):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    carol_identity, carol = _peer_identity(JOINER)
    node.registry.upsert(bob.destination_hash, bob.public_key, name="bob", now=1.0)
    node.registry.upsert(
        carol.destination_hash, carol.public_key, name=JOINER, now=1.0
    )
    group = _group(node, own_hash, bob)
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode="open",
    )
    welcomes = []
    monkeypatch.setattr(
        node,
        "send_group_message",
        lambda group_arg, text, **kwargs: welcomes.append((group_arg, text)),
    )
    response = node._handle_message(
        "/tau/message",
        encode_message(link, kind="group-join-request", message_id="m-1"),
        b"req-1",
        carol_identity,
        1000.0,
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert payload["status"] == "added"
    assert carol.hexhash in payload["group"]["members"]
    assert payload["group"]["id"] != group["id"]
    # The roster changed to a successor group.
    assert node.groups.get(payload["group"]["id"]) is not None
    assert carol.hexhash in node.groups.get(payload["group"]["id"])["members"]
    # A welcome group message went out to the new roster.
    assert len(welcomes) == 1
    assert welcomes[0][0]["id"] == payload["group"]["id"]
    assert JOINER in welcomes[0][1]
    # The join request was never stored as a chat message: its id
    # was never marked seen by the storage path.
    assert not node.mailbox.is_seen("m-1")
    # A repeat request from the new member is idempotent.
    again = decode(
        node._handle_message(
            "/tau/message",
            encode_message(link, kind="group-join-request", message_id="m-2"),
            b"req-2",
            carol_identity,
            1001.0,
        )
    )
    assert again["status"] == "already_member"


def test_closed_group_join_queues_request(tmp_path, monkeypatch):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    carol_identity, carol = _peer_identity(JOINER)
    group = _group(node, own_hash, bob)
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode="closed",
    )
    monkeypatch.setattr(
        node,
        "send_group_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("no welcome until approval")
        ),
    )
    response = decode(
        node._handle_message(
            "/tau/message",
            encode_message(link, kind="group-join-request", message_id="m-1"),
            b"req-1",
            carol_identity,
            1000.0,
        )
    )
    assert response["ok"] is True
    assert response["status"] == "pending"
    assert carol.hexhash not in node.groups.get(group["id"])["members"]
    pending = node.list_join_requests(group["name"])
    assert len(pending) == 1
    assert pending[0]["peer"] == carol.hexhash
    # Approving admits the peer and welcomes them.
    welcomes = []
    monkeypatch.setattr(
        node,
        "send_group_message",
        lambda group_arg, text, **kwargs: welcomes.append((group_arg, text)),
    )
    outcome = node.approve_join_request(group["name"], carol.hexhash)
    assert outcome["status"] == "added"
    assert carol.hexhash in outcome["group"]["members"]
    assert node.list_join_requests(group["name"]) == []
    assert len(welcomes) == 1
    # Approving twice fails: there is no pending request any more.
    with pytest.raises(ValueError, match="no pending join request"):
        node.approve_join_request(group["name"], carol.hexhash)


def test_approve_join_request_refuses_banned_peer(tmp_path, monkeypatch):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    _, carol = _peer_identity(JOINER)
    group = _group(node, own_hash, bob)
    # Carol is banned before she ever asks: the ban bars the approve
    # path, so an invite link cannot bring her back in.
    node.groups.ban(group["id"], carol.hexhash, created_by=own_hash)
    record_join_request(
        node.config.join_requests_path,
        group["name"],
        carol.hexhash,
        now=time.time(),
    )
    with pytest.raises(ValueError, match="is banned"):
        node.approve_join_request(group["name"], carol.hexhash)
    # The request is still pending — the creator can deny it, or
    # unban Carol and approve her afterwards.
    assert len(node.list_join_requests(group["name"])) == 1
    welcomes = []
    monkeypatch.setattr(
        node,
        "send_group_message",
        lambda group_arg, text, **kwargs: welcomes.append((group_arg, text)),
    )
    node.groups.unban(group["id"], carol.hexhash, created_by=own_hash)
    outcome = node.approve_join_request(group["name"], carol.hexhash)
    assert outcome["status"] == "added"
    assert carol.hexhash in outcome["group"]["members"]
    assert len(welcomes) == 1


def test_deny_join_request(tmp_path, monkeypatch):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    carol_identity, carol = _peer_identity(JOINER)
    group = _group(node, own_hash, bob)
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode="closed",
    )
    monkeypatch.setattr(node, "send_group_message", lambda *a, **k: None)
    decode(
        node._handle_message(
            "/tau/message",
            encode_message(link, kind="group-join-request", message_id="m-1"),
            b"req-1",
            carol_identity,
            1000.0,
        )
    )
    outcome = node.deny_join_request(group["name"], carol.hexhash)
    assert outcome["status"] == "denied"
    assert carol.hexhash not in node.groups.get(group["id"])["members"]
    assert node.list_join_requests(group["name"]) == []


def test_invalid_join_request_is_rejected_not_stored(tmp_path):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    carol_identity, _ = _peer_identity(JOINER)
    group = _group(node, own_hash, bob)
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode="open",
    )
    # Revoke the token after minting: the request must fail.
    revoke_invites(node.config.invites_path, group_name=group["name"])
    response = decode(
        node._handle_message(
            "/tau/message",
            encode_message(link, kind="group-join-request", message_id="m-1"),
            b"req-1",
            carol_identity,
            1000.0,
        )
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_invite"
    assert node.groups.get(group["id"])["members"] == group["members"]


def test_relayed_join_request_is_decided_not_stored(tmp_path, monkeypatch):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    carol_identity, carol = _peer_identity(JOINER)
    group = _group(node, own_hash, bob)
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode="open",
    )
    monkeypatch.setattr(node, "send_group_message", lambda *a, **k: None)
    delivered = node._deliver_collected(
        {
            "from": carol.hexhash,
            "text": link,
            "kind": "group-join-request",
            "id": "m-1",
            "sent_at": 1000.0,
        }
    )
    assert delivered is True
    # The roster changed to a successor group containing the joiner.
    assert any(
        carol.hexhash in g["members"] for g in node.groups.list_groups()
    )
    assert not node.mailbox.is_seen("m-1")


# --- secret groups: id-keyed invite links ----------------------------------


def test_secret_group_join_via_id_link(tmp_path, monkeypatch):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    carol_identity, carol = _peer_identity(JOINER)
    node.registry.upsert(bob.destination_hash, bob.public_key, name="bob", now=1.0)
    node.registry.upsert(
        carol.destination_hash, carol.public_key, name=JOINER, now=1.0
    )
    group = _secret_group(node, identity, own_hash, bob)
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode="open",
        group_id=group["id"],
        group_created_at=group["created_at"],
    )
    # The minted link names the group by id only — no name anywhere.
    token = parse_invite_link(link)
    assert token["group_id"] == group["id"]
    assert "group" not in token
    welcomes = []
    monkeypatch.setattr(
        node,
        "send_group_message",
        lambda group_arg, text, **kwargs: welcomes.append((group_arg, text)),
    )
    response = decode(
        node._handle_message(
            "/tau/message",
            encode_message(link, kind="group-join-request", message_id="m-1"),
            b"req-1",
            carol_identity,
            1000.0,
        )
    )
    assert response["ok"] is True
    assert response["status"] == "added"
    assert carol.hexhash in response["group"]["members"]
    # The successor is still a secret group with a fresh opaque id.
    successor = node.groups.get(response["group"]["id"])
    assert successor is not None
    assert successor.get("secret") is True
    assert successor["id"] != group["id"]
    assert len(welcomes) == 1
    assert not node.mailbox.is_seen("m-1")


def test_id_link_dies_on_rename_and_revoke_under_new_name(
    tmp_path, monkeypatch
):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    carol_identity, carol = _peer_identity(JOINER)
    node.registry.upsert(
        carol.destination_hash, carol.public_key, name=JOINER, now=1.0
    )
    group = _secret_group(node, identity, own_hash, bob)
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode="open",
        group_id=group["id"],
        group_created_at=group["created_at"],
    )
    # Rename the secret group, freeing its name, then create an
    # unrelated group under the old name.
    renamed = node.groups.rename(
        group["id"], "team", created_by=own_hash, identity=identity
    )
    node.groups.create("crew", [own_hash, bob.hexhash], created_by=own_hash)
    monkeypatch.setattr(node, "send_group_message", lambda *a, **k: None)

    def attempt(message_id):
        return decode(
            node._handle_message(
                "/tau/message",
                encode_message(
                    link, kind="group-join-request", message_id=message_id
                ),
                b"req-1",
                carol_identity,
                1000.0,
            )
        )

    # A rename invalidates outstanding invites, exactly like a
    # name token: the old link joins neither the renamed group
    # nor the group that reused the old name.
    response = attempt("m-1")
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_invite"
    assert carol.hexhash not in node.groups.find("crew")["members"]
    assert carol.hexhash not in node.groups.get(renamed["id"])["members"]
    # Revoking under the new name leaves it rejected too — the
    # revoke bumps only the new name's record, so the refusal
    # must come from the rename itself, not the generation.
    revoke_invites(node.config.invites_path, group_name="team")
    response = attempt("m-2")
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_invite"
    # A fresh link minted after the rename works.
    fresh = mint_invite(
        node.config.invites_path,
        identity,
        group_name="team",
        creator_hash=own_hash,
        mode="open",
        group_id=renamed["id"],
        group_created_at=renamed["created_at"],
    )
    response = decode(
        node._handle_message(
            "/tau/message",
            encode_message(
                fresh, kind="group-join-request", message_id="m-3"
            ),
            b"req-3",
            carol_identity,
            1001.0,
        )
    )
    assert response["ok"] is True
    assert response["status"] == "added"
    assert response["group"]["name"] == "team"
    assert carol.hexhash in response["group"]["members"]


def test_id_link_survives_retired_record_eviction(tmp_path, monkeypatch):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    _, dave = _peer_identity("dave")
    carol_identity, carol = _peer_identity(JOINER)
    node.registry.upsert(
        carol.destination_hash, carol.public_key, name=JOINER, now=1.0
    )
    # The retired cache is capped; shrink the cap so unrelated
    # rotations evict the link's retired record in this test.
    monkeypatch.setattr("tau.groups.MAX_RETIRED_GROUPS", 2)
    group = _secret_group(node, identity, own_hash, bob)
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode="open",
        group_id=group["id"],
        group_created_at=group["created_at"],
    )
    # A roster change retires the id the link names.
    node.groups.update_members(
        group["id"],
        [own_hash, bob.hexhash, dave.hexhash],
        created_by=own_hash,
        identity=identity,
    )
    assert node.groups.get_retired(group["id"]) is not None
    # Three unrelated rotations push the retired record out.
    for name in ("u1", "u2", "u3"):
        other = node.groups.create(
            name, [own_hash, bob.hexhash], created_by=own_hash
        )
        node.groups.update_members(
            other["id"],
            [own_hash, dave.hexhash],
            created_by=own_hash,
        )
    assert node.groups.get_retired(group["id"]) is None
    monkeypatch.setattr(node, "send_group_message", lambda *a, **k: None)
    response = decode(
        node._handle_message(
            "/tau/message",
            encode_message(link, kind="group-join-request", message_id="m-1"),
            b"req-1",
            carol_identity,
            1000.0,
        )
    )
    assert response["ok"] is True
    assert response["status"] == "added"
    assert carol.hexhash in response["group"]["members"]


def test_id_link_survives_roster_rotation(tmp_path, monkeypatch):
    node, identity, own_hash = _invite_node(tmp_path)
    _, bob = _peer_identity("bob")
    _, dave = _peer_identity("dave")
    carol_identity, carol = _peer_identity(JOINER)
    node.registry.upsert(
        carol.destination_hash, carol.public_key, name=JOINER, now=1.0
    )
    group = _secret_group(node, identity, own_hash, bob)
    link = mint_invite(
        node.config.invites_path,
        identity,
        group_name=group["name"],
        creator_hash=own_hash,
        mode="open",
        group_id=group["id"],
        group_created_at=group["created_at"],
    )
    # A roster change rotates the secret group's id before Carol
    # ever uses the link: the token names the retired id.
    node.groups.update_members(
        group["id"],
        [own_hash, bob.hexhash, dave.hexhash],
        created_by=own_hash,
        identity=identity,
    )
    assert node.groups.get(group["id"]) is None
    monkeypatch.setattr(node, "send_group_message", lambda *a, **k: None)
    response = decode(
        node._handle_message(
            "/tau/message",
            encode_message(link, kind="group-join-request", message_id="m-1"),
            b"req-1",
            carol_identity,
            1000.0,
        )
    )
    assert response["ok"] is True
    assert response["status"] == "added"
    assert carol.hexhash in response["group"]["members"]
