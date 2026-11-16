import type { Peer, TauGroup } from "@/lib/types";

// One peer/group target picker shared by the message feed's forward
// action and the Tan post card's share action, so a change to the
// picker (or its empty state) reaches both. The verb tunes the chip
// titles and the picker's accessible label — "Forward to" for the
// feed, "Share with" for a post card — and `className` carries the
// caller's layout: the feed right-aligns outbound pickers, the card
// does not.
export function TargetPicker({
  peers,
  groups,
  pending = false,
  onSelectPeer,
  onSelectGroup,
  verb,
  className = "",
}: {
  peers?: Peer[];
  groups?: TauGroup[];
  pending?: boolean;
  onSelectPeer?: (destination: string) => void;
  onSelectGroup?: (id: string) => void;
  verb: "Forward to" | "Share with";
  className?: string;
}) {
  return (
    <div
      className={`flex flex-wrap gap-1 ${className}`}
      role="group"
      aria-label={`${verb} a peer or group`}
    >
      {(peers ?? []).length === 0 && (groups ?? []).length === 0 ? (
        <span className="font-mono text-[10px] text-mist/60">
          no known peers or groups yet
        </span>
      ) : (
        <>
          {(peers ?? []).map((peer) => (
            <button
              key={peer.destination}
              type="button"
              title={`${verb} ${peer.alias ?? peer.name}`}
              disabled={pending}
              onClick={() => onSelectPeer?.(peer.destination)}
              className="rounded-lg border border-line/80 bg-void/60 px-2 py-1 font-mono text-[11px] text-zinc-200 transition hover:border-signal/35 hover:bg-void"
            >
              {peer.alias ?? peer.name}
            </button>
          ))}
          {(groups ?? []).map((group) => (
            <button
              key={group.id}
              type="button"
              title={`${verb} group ${group.name}`}
              disabled={pending}
              onClick={() => onSelectGroup?.(group.id)}
              className="rounded-lg border border-signal/30 bg-signal/10 px-2 py-1 font-mono text-[11px] text-signal transition hover:border-signal/50 hover:bg-signal/15"
            >
              #{group.name}
            </button>
          ))}
        </>
      )}
    </div>
  );
}
