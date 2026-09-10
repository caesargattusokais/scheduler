package dev.scheduler.persistence;
import java.time.Instant;
import java.util.List;
public record WorkerRegistration(String id, List<String> refs, Instant lastSeen, String status) {}