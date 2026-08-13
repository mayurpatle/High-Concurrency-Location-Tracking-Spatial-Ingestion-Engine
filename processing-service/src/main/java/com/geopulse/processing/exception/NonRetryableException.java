package com.geopulse.processing.exception;

/**
 * Marks a failure that will NEVER succeed on retry — a bad message, not a
 * bad moment. Malformed payload, missing required field, unparseable data.
 *
 * Throwing this tells the error handler: skip retries, go straight to the
 * dead-letter topic. Retrying a permanently-broken message is the poison pill
 * that stalls a partition forever.
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}