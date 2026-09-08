package dev.scheduler.core;
import java.time.Instant;

/** DAG 定义头(spec §1.1)。 */
public record Dag(Long id, String name, String description, String cron,
    boolean enabled, boolean paused, Instant createdAt, Instant updatedAt) {}
