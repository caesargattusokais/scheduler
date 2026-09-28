package dev.scheduler.persistence;

import java.time.Instant;

public record RuntimeConfigRow(String key, String value, String updatedBy, Instant updatedAt) {}