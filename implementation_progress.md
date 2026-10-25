# Bitswap 1.2.0 Implementation Progress Checklist

This document tracks the implementation status of the `dart-libp2p-bitswap` module against the Bitswap 1.2.0 specification (`bitswap_spec.txt`).

## 1. Protocol Version & ID

-   [x] **Protocol ID:** Uses `/ipfs/bitswap/1.2.0`.
    *   _Location:_ `BitswapNetworkService.protocolId`

## 2. Wire Format (Protobuf `Message` for 1.2.0)

-   **`Message.Wantlist`**
    -   [x] `WantType` enum (`Block`, `Have`) defined in proto.
    -   [x] `Entry.block` (CID bytes) defined and used.
    -   [x] `Entry.priority` (int32) defined and used.
    -   [x] `Entry.cancel` (bool) defined and used.
    -   [x] `Entry.wantType` (WantType) defined and used.
    -   [x] `Entry.sendDontHave` (bool) defined and used.
    -   [x] `entries` (repeated Entry) defined and used.
    -   [x] `full` (bool) defined and used.
-   **`Message.Block`**
    -   [x] `prefix` (bytes) defined; parsed by `cidFromPrefixAndData`, built by `encodeCidPrefix` (`lib/src/cid_prefix.dart`).
    -   [x] `data` (bytes) defined and used.
-   **`Message.BlockPresence`**
    -   [x] `BlockPresenceType` enum (`Have`, `DontHave`) defined in proto.
    -   [x] `cid` (bytes) defined and used.
    -   [x] `type` (BlockPresenceType) defined and used.
-   **`Message` (Top-Level Fields)**
    -   [x] `wantlist` (Wantlist) defined and used.
    -   [x] `payload` (repeated Block) defined and used.
    -   [x] `blockPresences` (repeated BlockPresence) defined and used.
    -   [ ] `pendingBytes` (int32):
        -   [x] Defined in proto.
        -   [ ] **TODO:** Implement setting `pendingBytes` in outgoing messages.
        -   [ ] **TODO:** Implement reading and acting upon `pendingBytes` in incoming messages.

## 3. Interaction Pattern (1.2.0)

-   **Client (C) Wants Data from Server (S):**
    -   [x] **Client sends want message:** `BitswapClient.wantBlocks` via `BitswapNetworkService.sendMessage`.
    -   [x] **Client sends complete wantlist or update:** `full` sent on first contact (`BitswapClient`), honoured in `BitswapServer.handleWantlist`.
    -   [x] **Client reuses stream:** `BitswapNetworkService` keeps one outgoing stream per peer, with sends serialised in order.
    -   [ ] **Client specifies `wantType` (Block/Have) & `sendDontHave`:**
        -   [x] Proto fields exist and are processed on receipt.
        -   [ ] **TODO:** Allow `Bitswap.wantBlocks` to specify `wantType` and `sendDontHave` per CID for outgoing wants. (Currently defaults to `Block` and `sendDontHave=false`).
-   **Server (S) Responds:**
    -   [x] **Server responds on stream, reuses stream:** `BitswapNetworkService.onIncomingStream` reads multiple messages per stream; replies go out on our own outgoing stream.
    -   [ ] **Server response to `Have` request:**
        -   [x] Responds with `Have` `BlockPresence` if block is present.
        -   [ ] **TODO (Optimization):** Consider responding with the block itself if it's very small, as per spec.
    -   [x] **Server response to `Have` (block not present, `sendDontHave` is true):** Responds with `DontHave` `BlockPresence`.
    -   [ ] **Server MAY include `pendingBytes`:** (Covered in section 2 `pendingBytes` TODOs).
    -   [ ] **Server SHOULD respect priority:**
        -   [x] `priority` is stored per peer in `PeerLedger`.
        -   [ ] **TODO:** Serve in priority order — tracked in bead bsw-0u2.7.
-   **Client (C) Cancels Want:**
    -   [x] **Client sends Cancel message:** sent on block arrival, on `cancelWants`, and when the last `getBlock` waiter gives up (`BitswapClient`).

## 4. Other Spec Points

-   [x] **Message-based protocol:** Implemented.
-   [x] **Varint length-prefixing:** Implemented in `BitswapNetworkService` using `dcid` varint utilities.
-   [ ] **Block Sizes (<= 2MiB recommended):**
    -   [ ] **Note:** No explicit checks in Bitswap logic; relies on block store and network layers. Consider if explicit checks/warnings are needed.
-   [x] **Protocol Message Sizes (<= 4MiB):** Inbound messages over 4 MiB reset the stream; responses are packed into messages of at most 4 MiB (`packResponse`).

## 5. General Implementation Details

-   [x] **CID Reconstruction:** `cidFromPrefixAndData` (CIDv0 and CIDv1), used by `BitswapClient.handleBlock`.
-   [x] **Peer discovery:** `BitswapClient` uses peers that spoke Bitswap to us plus connected peers advertising the protocol.
-   [x] **Peer Disconnection Handling:** `Bitswap` subscribes to `EvtPeerConnectednessChanged` on `host.eventBus` and clears client, server and stream state.

## Open work

Remaining work is tracked in the beads issue tracker under epic `bsw-0u2` (`bd ready`, `bd show bsw-0u2`), not in this file.
