package com.geopulse.ingestion.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Global error handling for the ingestion edge.
 *
 * Two goals:
 *  1. USEFUL errors — tell the client exactly which field was wrong.
 *  2. NO LEAKS — never expose stack traces or internal class names. This
 *     endpoint faces the public internet; error messages are an attack surface.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * Thrown when @Valid fails on the DTO — i.e. the client sent structurally
     * valid JSON but semantically invalid data (lat=200, missing driverId...).
     *
     * We return a field -> message map so the client can fix the exact problem.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {

        Map<String, String> fieldErrors = new HashMap<>();
        ex.getBindingResult().getFieldErrors().forEach(err ->
                fieldErrors.put(err.getField(), err.getDefaultMessage()));

        // NOTE: we do NOT log at ERROR here. Bad client input is EXPECTED traffic,
        // not a system fault — and at 250k/s, logging every rejected ping would
        // flood the log pipeline (and hand an attacker a cheap way to fill our disks).
        // Validation failures get COUNTED as a metric in Phase 6, not logged.
        return ResponseEntity.badRequest().body(Map.of(
                "status",    HttpStatus.BAD_REQUEST.value(),
                "error",     "Validation failed",
                "fields",    fieldErrors,
                "timestamp", Instant.now().toString()
        ));
    }

    /**
     * Thrown when the body isn't parseable at all — malformed JSON, wrong
     * content type, a string where a number belongs.
     * We give a generic message: don't echo the parser's internals back.
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "status",    HttpStatus.BAD_REQUEST.value(),
                "error",     "Malformed request body",
                "timestamp", Instant.now().toString()
        ));
    }

    /**
     * The catch-all. Anything unexpected = OUR bug, not the client's.
     *
     * Here we DO log at ERROR (with the stack trace, server-side, for us)
     * but return a bland 500 to the client. This is the split that matters:
     * full detail in our logs, zero detail on the wire.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleUnexpected(Exception ex) {
        log.error("Unexpected error in ingestion", ex);   // full trace -> our logs
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status",    HttpStatus.INTERNAL_SERVER_ERROR.value(),
                "error",     "Internal server error",     // <- bland, on purpose
                "timestamp", Instant.now().toString()
        ));
    }
}