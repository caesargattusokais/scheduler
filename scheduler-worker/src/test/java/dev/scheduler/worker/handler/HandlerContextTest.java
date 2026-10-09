package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class HandlerContextTest {
  @Test void touch_delegatesToProgress() {
    var p = new ProgressSignal();
    var ctx = new HandlerContext(1, 2, 0, 1, null, null, "w", new CancellationToken(false), p);
    ctx.touch();
    assertTrue(p.consume(), "touch 委托置位 progress");
  }

  @Test void touch_noProgress_isNoop() {
    var ctx = HandlerContext.of(1, 2, 0, 1, null, null); // progress=null
    ctx.touch(); // 不得 NPE
    assertNull(ctx.progress(), "of(...) 便捷构造的 progress 为 null");
  }
}
