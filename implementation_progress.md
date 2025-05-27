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
    -   [x] `prefix` (bytes) defined; parsed in `_handleReceivedBlock`, constructed in `_handleWantlist`.
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
    -   [x] **Client sends want message:** `BitswapNetworkService.sendMessage` and `Bitswap.wantBlocks`.
    -   [x] **Client sends complete wantlist or update:** `Message_Wantlist.full` handled in `_handleWantlist`.
    -   [x] **Client reuses stream:** `BitswapNetworkService` reuses streams.
    -   [ ] **Client specifies `wantType` (Block/Have) & `sendDontHave`:**
        -   [x] Proto fields exist and are processed on receipt.
        -   [ ] **TODO:** Allow `Bitswap.wantBlocks` to specify `wantType` and `sendDontHave` per CID for outgoing wants. (Currently defaults to `Block` and `sendDontHave=false`).
-   **Server (S) Responds:**
    -   [x] **Server responds on stream, reuses stream:** `_onIncomingStream` handles messages; `BitswapNetworkService` reuses outgoing streams.
    -   [ ] **Server response to `Have` request:**
        -   [x] Responds with `Have` `BlockPresence` if block is present.
        -   [ ] **TODO (Optimization):** Consider responding with the block itself if it's very small, as per spec.
    -   [x] **Server response to `Have` (block not present, `sendDontHave` is true):** Responds with `DontHave` `BlockPresence`.
    -   [ ] **Server MAY include `pendingBytes`:** (Covered in section 2 `pendingBytes` TODOs).
    -   [ ] **Server SHOULD respect priority:**
        -   [x] `priority` field exists in `WantlistEntry` and is stored by `WantManager`.
        -   [ ] **TODO:** Implement active prioritization of responses in `_handleWantlist` based on `priority`.
-   **Client (C) Cancels Want:**
    -   [x] **Client sends Cancel message:** `_sendCancelMessages` called in `_handleReceivedBlock`.

## 4. Other Spec Points

-   [x] **Message-based protocol:** Implemented.
-   [x] **Varint length-prefixing:** Implemented in `BitswapNetworkService` using `dart_cid` utilities.
-   [ ] **Block Sizes (<= 2MiB recommended):**
    -   [ ] **Note:** No explicit checks in Bitswap logic; relies on block store and network layers. Consider if explicit checks/warnings are needed.
-   [ ] **Protocol Message Sizes (<= 4MiB):**
    -   [ ] **Note:** No explicit checks for outgoing/incoming message sizes against this limit. Consider for robustness.

## 5. General Implementation Details

-   [x] **CID Reconstruction:** Implemented in `_handleReceivedBlock`.
-   [x] **Robust `_getBitswapPeers()`:** Implemented in `Bitswap.dart` using host network/peerstore.
-   [x] **Peer Disconnection Handling:** Basic framework implemented in `Bitswap.dart` using `EvtPeerConnectednessChanged` to clean up state.

## Summary of Key Pending Implementation TODOs:

1.  **`pendingBytes` Field:**
    *   Implement setting `pendingBytes` in outgoing messages.
    *   Implement reading and acting upon `pendingBytes` from incoming messages.
2.  **Client-side `wantBlocks` Flexibility:**
    *   Allow `Bitswap.wantBlocks` to specify `wantType` (Block/Have) and `sendDontHave` per CID.
3.  **Server-side Response to `Have` Requests (Optimization):**
    *   Consider responding with the block itself if it's very small when handling a `Have` request.
4.  **Server-side Prioritization:**
    *   Implement logic in `_handleWantlist` to respect the `priority` field from incoming wantlist entries.
5.  **Message/Block Size Limit Considerations:**
    *   Evaluate if explicit checks or warnings for block sizes (<=2MiB) or total message sizes (<=4MiB) are needed for robustness.
6.  **Refine Peer Disconnection/Stream Error Handling:**
    *   `BitswapNetworkService`: More robustly listen for stream close/error events to remove streams from `_activeStreams` (currently only removed on send error or explicit disconnect).
    *   `Bitswap`: Consider re-evaluation of local wants if a peer disconnects who was the sole provider for certain CIDs.
7.  **General TODOs from code:**
    *   `Bitswap._handleWantlist`: Stop in-progress sending efforts on CANCEL.
    *   `Bitswap.wantBlocks`: Potentially queue wantlist or try discovery if no peers initially.
    *   `BitswapNetworkService.start`: Potentially discover existing peers.
    *   `BitswapNetworkService._onIncomingStream`: Handle cases where `upstreamFrameData` might contain more than one Bitswap message.
    *   `WantManager.blockReceived`: Enhance notifications.
    *   `WantManager.receivedHave`: Potentially prioritize fetching.
    *   `WantManager.receivedDontHave`: Deprioritize/stop asking peer.
