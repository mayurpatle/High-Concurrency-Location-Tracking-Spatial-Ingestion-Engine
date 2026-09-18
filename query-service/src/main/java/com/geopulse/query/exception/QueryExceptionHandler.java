package com.geopulse.query.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class QueryExceptionHandler {

    /**
     * 503 SERVICE_UNAVAILABLE, not 200-with-empty and not 404.
     *
     * 503 tells the caller "retry later" — it's a transient, our-fault status.
     * A 404 would mean "definitely not there"; an empty 200 would mean
     * "definitely nothing nearby". Both are lies during an outage.
     *
     * Retry-After lets a well-behaved client back off sensibly instead of
     * hammering us while the breaker is open.
     */
    @ExceptionHandler(SpatialUnavailableException.class)
    public ResponseEntity<Map<String, Object>> handleUnavailable(SpatialUnavailableException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "10")   // matches the breaker's open window
                .body(Map.of(
                        "status", 503,
                        "error", "Spatial store temporarily unavailable",
                        "timestamp", Instant.now().toString()));
    }
}