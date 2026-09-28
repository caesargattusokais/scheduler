package dev.scheduler.server.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.scheduler.core.Execution;
import dev.scheduler.core.Shard;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.OutboundNotification;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.Webhook;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * NotificationTaskMatcher 纯单测:任务维度白名单/黑名单 × 任务级与 DAG 级命中。
 * 语义与 Webhook.scopeMode 对应:ALL 恒命中;INCLUDE 仅选中;EXCLUDE 取反;解析不出 taskId 放行不误杀。
 */
class NotificationTaskMatcherTest {
  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

  private ExecutionRepository executions;
  private ShardRepository shards;
  private DagRepository dags;
  private NotificationTaskMatcher matcher;

  @BeforeEach
  void setUp() {
    executions = mock(ExecutionRepository.class);
    shards = mock(ShardRepository.class);
    dags = mock(DagRepository.class);
    matcher = new NotificationTaskMatcher(executions, shards, dags);
  }

  private static Webhook hook(String scope, List<Long> tasks, List<Long> dagIds) {
    return new Webhook(1, "https://h/x", null, List.of(), true, 5, 1000, T0, scope, tasks, dagIds);
  }

  /** execution 目标:targetId = 执行行 id,taskId 由 execution 解析。 */
  private static OutboundNotification execNotif(long execId) {
    return new OutboundNotification(1, "execution.completed", null, "execution", execId, "{}",
        OutboundNotification.STATUS_PENDING, 0, null, null, T0, null);
  }

  /** shard 目标:targetId = 分片 id,经 shard→execution→taskId 两级解析。 */
  private static OutboundNotification shardNotif(long shardId) {
    return new OutboundNotification(1, "execution.completed", null, "shard", shardId, "{}",
        OutboundNotification.STATUS_PENDING, 0, null, null, T0, null);
  }

  /** 执行力不从心:执行行已回收(无 task join)→ 解析失败放行。 */
  private static OutboundNotification unresolvableNotif(String targetType) {
    return new OutboundNotification(1, "execution.completed", null, targetType, 999L, "{}",
        OutboundNotification.STATUS_PENDING, 0, null, null, T0, null);
  }

  @Test
  void all_alwaysHits() {
    when(executions.findById(anyLong())).thenReturn(Optional.of(Execution.ofDue(11, "k", 1)));
    var n = execNotif(5);
    assertTrue(matcher.matches(hook("ALL", List.of(), List.of()), n));
    // INCLUDE 语义不适用于 ALL:即便选集空也恒命中。
    assertTrue(matcher.matches(hook("ALL", List.of(99L), List.of()), n));
  }

  @Test
  void include_taskDirectHit_andMiss() {
    when(executions.findById(5L)).thenReturn(Optional.of(Execution.ofDue(11, "k", 1)));
    var n = execNotif(5);
    // 选中 task 11 → 命中。
    assertTrue(matcher.matches(hook("INCLUDE", List.of(11L), List.of()), n));
    // 选中的是不相关 task 22 → 不命中。
    assertFalse(matcher.matches(hook("INCLUDE", List.of(22L), List.of()), n));
  }

  @Test
  void include_dagAncestorHit() {
    when(executions.findById(5L)).thenReturn(Optional.of(Execution.ofDue(11, "k", 1)));
    var n = execNotif(5);
    // task 11 属于 dag 3;选中 dag 3 → 命中。
    when(dags.findDagIdsContainingTask(11L)).thenReturn(List.of(3L));
    assertTrue(matcher.matches(hook("INCLUDE", List.of(), List.of(3L)), n));
    // 选中 dag 9(不含 task 11)→ 不命中。
    when(dags.findDagIdsContainingTask(11L)).thenReturn(List.of(3L));
    assertFalse(matcher.matches(hook("INCLUDE", List.of(), List.of(9L)), n));
  }

  @Test
  void include_dag_noReverse_returnsEmpty_miss() {
    when(executions.findById(5L)).thenReturn(Optional.of(Execution.ofDue(11, "k", 1)));
    var n = execNotif(5);
    when(dags.findDagIdsContainingTask(11L)).thenReturn(List.of());
    assertFalse(matcher.matches(hook("INCLUDE", List.of(), List.of(3L)), n));
  }

  @Test
  void exclude_directMiss_toHit() {
    when(executions.findById(5L)).thenReturn(Optional.of(Execution.ofDue(11, "k", 1)));
    var n = execNotif(5);
    // 黑名单选中 task 11 → 排除。
    assertFalse(matcher.matches(hook("EXCLUDE", List.of(11L), List.of()), n));
    // 黑名单选的是 task 22 → 放行。
    assertTrue(matcher.matches(hook("EXCLUDE", List.of(22L), List.of()), n));
  }

  @Test
  void shard_resolvesViaExecution_taskId() {
    when(shards.findShard(7L)).thenReturn(Optional.of(Shard.ofDue(5L, 0)));
    when(executions.findById(5L)).thenReturn(Optional.of(Execution.ofDue(11, "k", 1)));
    var n = shardNotif(7);
    assertTrue(matcher.matches(hook("INCLUDE", List.of(11L), List.of()), n));
    assertFalse(matcher.matches(hook("INCLUDE", List.of(22L), List.of()), n));
  }

  @Test
  void unresolvable_taskId_doesNotSilenceAlarm() {
    // execution 目标但执行行不存在 → taskIdOf null → 放行(不因 join 失败误杀告警)。
    when(executions.findById(999L)).thenReturn(Optional.empty());
    assertTrue(matcher.matches(hook("INCLUDE", List.of(22L), List.of()), unresolvableNotif("execution")));
    // shard 目标但分片不存在 → 放行。
    when(shards.findShard(999L)).thenReturn(Optional.empty());
    assertTrue(matcher.matches(hook("INCLUDE", List.of(22L), List.of()), unresolvableNotif("shard")));
    // 无目标(如 operator 级告警)→ 放行。
    var noTarget = new OutboundNotification(1, "system.alert", null, null, null, "{}",
        OutboundNotification.STATUS_PENDING, 0, null, null, T0, null);
    assertTrue(matcher.matches(hook("INCLUDE", List.of(22L), List.of()), noTarget));
  }

  @Test
  void include_unknownTargetType_doesNotSilence() {
    // 目标类型不落进 execution/shard 分支(如 dag)→ taskIdOf null → 放行。
    var n = unresolvableNotif("dag");
    assertTrue(matcher.matches(hook("INCLUDE", List.of(22L), List.of()), n));
  }
}