package com.geopulse.processing.kafka;

import com.geopulse.processing.service.CellActivityTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * Persists in-memory per-cell counters before a partition changes hands.
 *
 * THE PROBLEM THIS SOLVES: the partition guarantee gives exclusion at an
 * INSTANT, not ownership over TIME. When a rebalance moves a partition, the old
 * owner's counters are stranded — lost on the next deploy, and the new owner
 * starts from zero.
 *
 * ⭐ Any in-memory aggregate keyed by partition needs a revocation hook, or it
 * leaks state on every deploy, every scale event, every rebalance.
 */


@Component
@RequiredArgsConstructor
@Slf4j
public class ActivityFlushRebalanceListener implements ConsumerAwareRebalanceListener {

    private final CellActivityTracker tracker;

    /**
     * Runs BEFORE offsets are committed and before the new owner starts — the
     * only correct moment to persist in-memory state for a partition we are
     * about to lose.
     *
     * Note the ordering matters: onPartitionsRevokedAfterCommit would still work
     * for counters (they're independent of offsets), but "before commit" is the
     * right habit — for state that must be consistent WITH the committed
     * offset, after-commit is already too late.
     */
    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer,
                                                Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            tracker.flushPartition(tp.partition());
        }
        // Rare enough to log. With CooperativeStickyAssignor this is usually a
        // SUBSET of our partitions, not all of them — only what actually moved.
        log.info("Flushed activity counters for revoked partitions: {}", partitions);
    }

    /**
     * Nothing to restore. Counters are additive deltas in Redis, so a new owner
     * starting from an empty local map is correct — its increments simply add
     * to whatever the previous owner flushed.
     *
     * ⭐ Additive state needs no handoff. That's not an accident; it's why we
     * chose INCRBY over SET.
     */
    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer,
                                     Collection<TopicPartition> partitions) {
        // intentionally empty
    }
}