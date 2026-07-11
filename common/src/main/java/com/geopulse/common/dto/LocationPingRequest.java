package com.geopulse.common.dto;

import jakarta.validation.constraints.*;

/**
 * The DTO: what a driver's phone sends us over HTTP.
 *
 * This is our PUBLIC CONTRACT with the driver app. It is deliberately
 * separate from the internal LocationPing domain model so that our API
 * and our Kafka message format can evolve independently.
 *
 * A Java `record` gives us an immutable carrier with a constructor,
 * accessors, equals/hashCode, and toString — for free. Perfect for a DTO.
 */
public record LocationPingRequest(

        // @NotBlank: rejects null, "", and "   ". A ping without a driver is meaningless.
        @NotBlank(message = "driverId is required")
        String driverId,

        // Latitude is bounded [-90, 90]. We use Double (not double) ON PURPOSE:
        // a primitive double would silently default to 0.0 when the field is
        // MISSING from the JSON — and 0.0 is a VALID latitude (the equator!).
        // So a missing field would sail through validation as a real location
        // in the Gulf of Guinea. The wrapper type stays null, and @NotNull catches it.
        @NotNull(message = "lat is required")
        @DecimalMin(value = "-90.0",  message = "lat must be >= -90")
        @DecimalMax(value = "90.0",   message = "lat must be <= 90")
        Double lat,

        // Longitude is bounded [-180, 180]. Same wrapper-type reasoning.
        @NotNull(message = "lng is required")
        @DecimalMin(value = "-180.0", message = "lng must be >= -180")
        @DecimalMax(value = "180.0",  message = "lng must be <= 180")
        Double lng,

        // Epoch millis, captured ON THE DEVICE (not on the server).
        // Why the device? Because the phone may buffer pings while offline and
        // flush them later — the moment of MEASUREMENT is what we care about,
        // not the moment of arrival. This also makes (driverId + timestamp)
        // a natural idempotency key for dedupe in Phase 2.
        @NotNull(message = "timestamp is required")
        @Positive(message = "timestamp must be a positive epoch-millis value")
        Long timestamp,

        // ---- Optional telemetry: no @NotNull. Absent is legal. ----

        // Metres/second. Negative speed is nonsense; we reject it if present.
        @PositiveOrZero(message = "speed cannot be negative")
        Double speed,

        // Compass bearing in degrees: 0 = north, clockwise, [0, 360).
        @DecimalMin(value = "0.0",   message = "heading must be >= 0")
        @DecimalMax(value = "360.0", message = "heading must be <= 360")
        Double heading,

        // GPS accuracy radius in metres. Useful later: a ping with 500m accuracy
        // (urban canyon, tunnel) is far less trustworthy than one with 5m.
        @PositiveOrZero(message = "accuracy cannot be negative")
        Double accuracy
) {}