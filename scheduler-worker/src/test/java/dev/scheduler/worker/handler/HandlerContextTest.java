package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class HandlerContextTest {
  @Test void touch_delegatesToProgress() {
    var p = new ProgressSignal();
    var ctx = new HandlerContext(1, 2, 0, 1, null, null, "w", new CancellationToken(false), p, 1);
    ctx.touch();
    assertTrue(p.consume(), "touch 委托置位 progress");
  }

  @Test void touch_noProgress_isNoop() {
    var ctx = HandlerContext.of(1, 2, 0, 1, null, null); // progress=null
    ctx.touch(); // 不得 NPE
    assertNull(ctx.progress(), "of(...) 便捷构造的 progress 为 null");
  }

  @Test void attempt_defaultsToFirstDelivery_viaOf() {
    var ctx = HandlerContext.of(1, 2, 0, 1, null, null);
    assertEquals(1, ctx.attempt(), "of(...) 便捷构造默认首次投递 attempt=1");
    assertFalse(ctx.redelivery(), "attempt=1 不应标记为重投递");
  }

  @Test void redelivery_trueWhenAttemptGreaterThanOne() {
    var ctx = new HandlerContext(1, 2, 0, 1, null, null, "w", new CancellationToken(false), null, 2);
    assertEquals(2, ctx.attempt());
    assertTrue(ctx.redelivery(), "attempt=2 应标记为重投递");
  }

  @Test void idempotencyKey_isStableExecutionIdColonShardIndex() {
    var first = new HandlerContext(42, 7, 3, 5, null, null, "w", new CancellationToken(false), null, 1);
    var retry = new HandlerContext(42, 7, 3, 5, null, null, "w", new CancellationToken(false), null, 9);
    assertEquals("42:3", first.idempotencyKey(), "幂等键 = executionId:shardIndex");
    assertEquals(first.idempotencyKey(), retry.idempotencyKey(), "幂等键跨 attempt 不变");
  }
}
