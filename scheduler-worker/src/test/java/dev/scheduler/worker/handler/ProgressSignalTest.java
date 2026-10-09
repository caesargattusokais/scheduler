package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class ProgressSignalTest {
  @Test void touchThenConsume_returnsTrueThenFalse() {
    var p = new ProgressSignal();
    assertFalse(p.consume(), "未 touch → false");
    p.touch();
    assertTrue(p.consume(), "touch 后首 consume → true");
    assertFalse(p.consume(), "consume 清零 → 再 consume false");
  }

  @Test void multipleTouches_singleConsumeCoalesces() {
    var p = new ProgressSignal();
    p.touch(); p.touch(); p.touch();
    assertTrue(p.consume(), "多次 touch 幂等合并");
    assertFalse(p.consume(), "已 flush → false");
  }
}
