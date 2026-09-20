package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.OperatorRole;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcOperatorRepositoryTest extends AbstractPostgresTest {
  private OperatorRepository operators;

  @BeforeEach
  void clean() {
    jdbc.execute("TRUNCATE app_operator RESTART IDENTITY CASCADE");
    operators = new JdbcOperatorRepository(jdbc);
  }

  @Test
  void roleOf_unknownName_empty() {
    assertTrue(operators.roleOf("no-such").isEmpty());
  }

  @Test
  void upsert_thenRoleOf_activeOperatorRollsRoles() {
    operators.upsert("alice", OperatorRole.ADMIN, true);
    assertEquals(OperatorRole.ADMIN, operators.roleOf("alice").orElseThrow());

    // 同 name upsert 幂等更新角色
    operators.upsert("alice", OperatorRole.OPERATOR, true);
    assertEquals(OperatorRole.OPERATOR, operators.roleOf("alice").orElseThrow());
  }

  @Test
  void roleOf_withoutName_onlyMatchesName() {
    operators.upsert("bob", OperatorRole.OPERATOR, true);
    assertTrue(operators.roleOf("alice").isEmpty());
  }

  @Test
  void deactivate_makesRoleOfEmpty_butRowRetained() {
    operators.upsert("carol", OperatorRole.ADMIN, true);
    operators.deactivate("carol");
    assertTrue(operators.roleOf("carol").isEmpty(), "停用后被 roleOf 视为不存在,不可再执行写操作");
    var row = jdbc.queryForMap("SELECT name, role, active FROM app_operator WHERE name='carol'");
    assertFalse((Boolean) row.get("active"), "行应保留(维系已写审计可读性),仅 active=false");
  }

  @Test
  void list_ordersByName_andShowsActive() {
    operators.upsert("zoe", OperatorRole.OPERATOR, true);
    operators.upsert("alice", OperatorRole.ADMIN, true);
    operators.upsert("bob", OperatorRole.OPERATOR, false);
    var all = operators.list();
    assertEquals(3, all.size());
    assertEquals("alice", all.get(0).name());
    assertEquals("bob", all.get(1).name());
    assertEquals("zoe", all.get(2).name());
    assertEquals(OperatorRole.ADMIN, all.get(0).role());
    assertFalse(all.get(1).active(), "停用的 bob 在目录中仍出现但 active=false");
  }
}