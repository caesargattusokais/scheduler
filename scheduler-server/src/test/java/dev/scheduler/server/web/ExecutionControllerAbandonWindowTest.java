package dev.scheduler.server.web;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.server.security.CurrentOperator;
import dev.scheduler.server.service.AuditRecorder;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;

/**
 * E2-B 强制放弃的活性门必须与 Reconciler 同源(spec §12:积压/卡死判据复用同一活体窗)。
 *
 * <p>动机:门与 Reconciler 曾各写各的 30s。运维调大 {@code scheduler.worker.stale-after-seconds} 后,
 * 一个心跳略慢(35s 前)的 worker 对 Reconciler 仍算 ALIVE(不回收),而门按 30s 判其已死 → 放行强制放弃,
 * 与仍在跑的 worker 撞成双跑。这正是活性门存在的意义所在。
 */
class ExecutionControllerAbandonWindowTest {

  /** 配置窗内属主仍 ALIVE → 必须 409 拒绝,且判定用的是配置值而非类内常量。 */
  @Test void abandon_usesConfiguredLivenessWindow() {
    ExecutionRepository executions = mock(ExecutionRepository.class);
    ShardRepository shards = mock(ShardRepository.class);
    WorkerRepository workers = mock(WorkerRepository.class);
    AuditRecorder auditor = mock(AuditRecorder.class);
    CurrentOperator current = mock(CurrentOperator.class);
    Shard running = new Shard(7L, 1L, 0, null, ExecutionStatus.RUNNING, 1, "w", null, null,
        false, false, null, null, null);
    when(shards.findShard(7L)).thenReturn(Optional.of(running));
    when(workers.isAlive("w", 60)).thenReturn(true); // 配置窗(60s)内 → 属主仍 ALIVE
    ExecutionController c = new ExecutionController(executions, shards, workers,
        mock(JdbcTemplate.class), auditor, current, 60);

    assertThrows(ResponseStatusException.class, () -> c.abandon(7L),
        "配置窗内属主仍 ALIVE → 409 拒绝(不得按 30s 判死放行)");
    verify(workers).isAlive("w", 60);
  }
}
