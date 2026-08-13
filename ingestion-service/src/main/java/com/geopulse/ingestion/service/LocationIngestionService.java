package com.geopulse.ingestion.service;

import com.geopulse.common.dto.LocationPingRequest;
import com.geopulse.common.model.LocationPing;
import com.geopulse.common.spatial.H3IndexService;
import com.geopulse.ingestion.kafka.LocationPingProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * The ingestion SEAM.
 *
 * Why does this exist if it barely does anything yet? Because it's the
 * boundary where the HTTP world ends and our domain begins. The controller
 * knows about HTTP (status codes, headers); this class knows nothing about
 * HTTP. That separation means:
 *   - we can add gRPC/WebSocket ingestion later and REUSE this untouched
 *   - we can unit-test ingestion logic without spinning up a web server
 *
 * Right now it translates DTO -> domain. In Session 1.2 the H3 enrichment
 * lands here; in 1.3 the Kafka publish replaces the log line.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class LocationIngestionService {

    private final H3IndexService h3IndexService  ;

    private final LocationPingProducer locationPingProducer  ;



    /**
     * Accept one validated ping.
     *
     * The DTO arrives already validated (Spring's @Valid ran before we were
     * called), so this method may TRUST its input — no re-checking.
     */

    /**
     * Max GPS error radius (metres) we'll trust. Externalized to config so it's
     * tunable without a rebuild — thresholds like this ALWAYS want to be tunable,
     * because the right value is discovered in production, not guessed at design time.
     */
    @Value("${geopulse.ingestion.max-accuracy-metres:100.0}")
    private double maxAccuracyMetres ;




    public void ingest(LocationPingRequest request) {

        // ---- QUALITY GATE ----
        // A ping with a huge error radius (tunnel, urban canyon, bad fix) is worse
        // than no ping: it would overwrite a good position with a vague one.
        //
        // We DROP it rather than reject it with a 400. The client did nothing wrong —
        // its JSON is perfectly valid — so making it retry would be pointless churn.
        // It still gets a 202. This is "accept and discard", and it's safe precisely
        // because location data is SELF-HEALING: a better fix arrives in ~4 seconds.
        if (request.accuracy() != null && request.accuracy() > maxAccuracyMetres) {
            // TODO(Phase 6): increment a `pings.dropped{reason="low_accuracy"}` counter.
            // Counting > logging on the hot path.
            return;
        }


        // ---- SPATIAL ENRICHMENT ----
        // "Index space at the edge." Two cells from ONE conversion:
        //   storage cell (res 9)   -> computed from lat/lng
        //   partition cell (res 7) -> walked UP the H3 hierarchy from it
        // Deriving the parent (rather than a 2nd lat/lng conversion) is cheaper
        // AND guarantees the two can never disagree.
        String storageCell   = h3IndexService.toStorageCell(request.lat(), request.lng());
        String partitionCell = h3IndexService.toPartitionCell(storageCell);

        // TODO(1.2): enrich with H3 cell ID  -> index space at the edge  - done

        // Translate: untrusted DTO -> trusted domain object.
        // This is the ONE place the boundary is crossed.
        LocationPing ping = LocationPing.from(
                request.driverId(),
                request.lat(),
                request.lng(),
                request.timestamp(),
                request.speed(),
                request.heading(),
                request.accuracy(),
                storageCell,
                partitionCell
        );


        // TODO(1.3): publish to Kafka        -> kafkaTemplate.send(TOPIC, key, ping) - done

        locationPingProducer.publish(ping);


        // Placeholder so we can SEE the pipeline work end-to-end today.
        // NOTE: this log is a deliberate stand-in and MUST die in 1.3.
        // Logging on the hot path at 250k/s would be a self-inflicted DoS —
        // disk I/O per request is exactly the blocking work we're avoiding.
        log.info("Ingested ping: driver={} lat={} lng={} h3={} partitionCell={}",
                ping.driverId(), ping.lat(), ping.lng(), ping.h3Cell(), ping.h3PartitionCell());

    }
}