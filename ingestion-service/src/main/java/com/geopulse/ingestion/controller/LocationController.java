package com.geopulse.ingestion.controller;

import com.geopulse.common.dto.LocationPingRequest;
import com.geopulse.ingestion.service.LocationIngestionService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * The write path's front door.
 *
 * What this endpoint does:   validate -> translate -> publish -> 202
 * What it MUST NEVER do:     DB writes, business logic, calls to other services.
 *
 * Anything done here is done 250,000 times a second. The edge stays thin.
 */
@RestController
@RequestMapping("/v1/locations")
@RequiredArgsConstructor   // Lombok: constructor injection for the final field below.
// Constructor injection (not @Autowired on a field) because it
// makes the dependency explicit, allows `final`, and lets us
// build the class in a plain unit test with `new`.
public class LocationController {

    private final LocationIngestionService ingestionService;

    /**
     * POST /v1/locations — a single driver ping.
     *
     * @Valid is what actually ENFORCES the annotations on the DTO. Without it,
     * every @NotNull/@DecimalMin is inert documentation. If validation fails,
     * Spring throws MethodArgumentNotValidException BEFORE this method body
     * runs — invalid data never reaches our service layer. (We shape that
     * exception into a clean 400 response in Part 3.)
     *
     * Returns 202 ACCEPTED, not 200 OK. This is a deliberate contract:
     * 200 would claim "stored and durable" — a lie, since we've only handed the
     * ping to Kafka and the consumer hasn't processed it yet. 202 honestly says
     * "I've taken responsibility for this; processing is in flight."
     *
     * Body is empty (Void). At 250k/s, serializing a response body for every
     * ping is pure waste — the status code IS the response.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> ingestLocation(@Valid @RequestBody LocationPingRequest request) {
        ingestionService.ingest(request);
        return ResponseEntity.accepted().build();
    }

    /**
     * POST /v1/locations/batch — many pings in one request.
     *
     * Two reasons this exists:
     *  1. A driver app that lost connectivity buffers pings and flushes them
     *     on reconnect. Without a batch endpoint that's N separate HTTP calls.
     *  2. Throughput: HTTP overhead (headers, TLS, connection handling) is paid
     *     PER REQUEST. Batching 50 pings into one request amortizes that cost
     *     ~50x. At our scale this is not a micro-optimization; it's the
     *     difference between needing 20 ingestion pods and needing 4.
     *
     * @Valid on a List<T> validates EVERY element. Note the all-or-nothing
     * semantics: one bad ping rejects the whole batch with a 400. That's a real
     * tradeoff — see the note below.
     *
     * @Size caps the batch so a client can't send us a 1-million-element array
     * and blow up our heap. Never trust a client-controlled collection size.
     */
    @PostMapping("/batch")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> ingestLocationBatch(
            @RequestBody @Valid @Size(max = 100, message = "batch size must not exceed 100")
            List<@Valid LocationPingRequest> requests) {

        requests.forEach(ingestionService::ingest);
        return ResponseEntity.accepted().build();
    }
}