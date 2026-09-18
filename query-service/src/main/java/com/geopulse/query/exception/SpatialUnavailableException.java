package com.geopulse.query.exception;

/**
 * The spatial store couldn't answer. Distinct from "the answer is empty."
 */
public class SpatialUnavailableException extends RuntimeException {
    public SpatialUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}