package com.geopulse.query.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Caching for the read path.
 *
 * WHAT WE DELIBERATELY DO NOT CACHE:
 *   /drivers/nearby          — already 2 round trips to an in-memory store
 *                              (~2-5ms). A cache saves milliseconds and costs
 *                              staleness on data whose entire value IS freshness.
 *   /drivers/{id}/location   — same reasoning, plus it's a single HGETALL.
 *
 * WHAT WE DO CACHE:
 *   cell density — many callers (Project 3's pricing engine polls hundreds of
 *   cells repeatedly) asking the SAME question, and the answer TOLERATES
 *   staleness: a 5-second-old supply count is fine for deciding "is this area
 *   busy?"
 *
 * THE RULE: cache what is expensive-or-repeated AND tolerates staleness.
 * Not "cache reads."
 */
@Configuration
@EnableCaching
public class CacheConfig {

    public static final String CELL_COUNT_CACHE = "cellCount";

    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager manager = new CaffeineCacheManager(CELL_COUNT_CACHE);

        manager.setCaffeine(Caffeine.newBuilder()
                // Hard ceiling. A city has hundreds of active cells; 10k is
                // generous. Bounded memory matters more than hit rate — an
                // unbounded cache is just a memory leak with good PR.
                .maximumSize(10_000)

                // THUNDERING HERD MITIGATION: stale-while-revalidate.
                // After 5s an entry is "refreshable" — the next request gets
                // the STALE value IMMEDIATELY while an async reload runs.
                // Without this, a popular entry expiring means every concurrent
                // request misses at the same instant and stampedes Redis.
                // The cache wouldn't reduce load; it would SYNCHRONIZE it.
                //.refreshAfterWrite(Duration.ofSeconds(5))
                // No refreshAfterWrite: that needs a CacheLoader, which
                // CaffeineCacheManager doesn't provide for @Cacheable methods.
                // A short TTL keeps the data fresh enough for a density figure.
                // trade of no red after write is mentioned in the design doc
                .expireAfterWrite(Duration.ofSeconds(5))



                .recordStats());   // exposed via Micrometer in Phase 6

        // Required for refreshAfterWrite: tells Caffeine to reload
        // asynchronously rather than blocking the requesting thread.
        //manager.setAsyncCacheMode(false);
        return manager;
    }
}