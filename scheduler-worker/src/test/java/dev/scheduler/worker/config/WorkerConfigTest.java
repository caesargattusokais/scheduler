package dev.scheduler.worker.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class WorkerConfigTest {
  @Test void defaultWorkerId_isWorkerAtHostnameColonPid() {
    String id = WorkerConfig.defaultWorkerId();
    assertTrue(id.startsWith("worker@"), "默认 id 前缀 vote@ : " + id);
    assertTrue(id.contains(":"), "应含 ':pid' 使同机多 worker 唯一 : " + id);
  }
}