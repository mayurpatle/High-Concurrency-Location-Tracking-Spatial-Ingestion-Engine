package com.geopulse.common.lock;

/**
 * A held lock, plus the fencing token that makes it enforceable.
 *
 * The token is the point. A lock alone cannot stop a write that's already in
 * flight from a stalled holder — the resource must reject stale tokens.
 */
public record FencedLock(String resource, String ownerId, long fenceToken) {}