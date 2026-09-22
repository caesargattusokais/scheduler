package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.OperatorRole;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcOperatorRepositoryTest extends AbstractPostgresTest {
  private OperatorRepository operators;

  @BeforeEach
  void clean() {
    // app_password_history 无 FK,不受 app_operator CASCADE 连带,须显式清以隔离用例。
    jdbc.execute("TRUNCATE app_operator, app_password_history RESTART IDENTITY CASCADE");
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

  @Test
  void passwordHash_setThenGet_activeRequired() {
    assertTrue(operators.activePasswordHash("alice").isEmpty(), "未登记 → empty");
    operators.upsert("alice", OperatorRole.ADMIN, true);
    assertTrue(operators.activePasswordHash("alice").isEmpty(), "无密 → empty");

    operators.setPassword("alice", "bcrypt-hash");
    assertEquals("bcrypt-hash", operators.activePasswordHash("alice").orElseThrow());

    operators.deactivate("alice");
    assertTrue(operators.activePasswordHash("alice").isEmpty(), "停用 → 即使有哈希也不返回");
  }

  @Test
  void namesWithoutPassword_listsOnlyNullHash() {
    assertTrue(operators.namesWithoutPassword().isEmpty());
    operators.upsert("alice", OperatorRole.ADMIN, true);
    operators.upsert("bob", OperatorRole.OPERATOR, true);
    operators.upsert("carol", OperatorRole.OPERATOR, true);
    assertEquals(List.of("alice", "bob", "carol"), operators.namesWithoutPassword());

    // setPassword 后 alice 不再无密;bob/carol 仍无密
    operators.setPassword("alice", "bcrypt-hash");
    assertEquals(List.of("bob", "carol"), operators.namesWithoutPassword());
  }

  @Test
  void mustChangePassword_defaultFalse_thenSetGet() {
    operators.upsert("alice", OperatorRole.ADMIN, true);
    assertEquals(false, operators.mustChangePassword("alice").orElseThrow(), "默认(迁移常量)false");

    operators.setMustChangePassword("alice", true); // 共享默认引导置位
    assertTrue(operators.mustChangePassword("alice").orElse(false), "置位应可回读");

    operators.setMustChangePassword("alice", false); // 人类选定口令清除
    assertFalse(operators.mustChangePassword("alice").orElse(true), "清除应可回读");
  }

  @Test
  void passwordHistory_pushAndRead_newestFirst() {
    operators.upsert("alice", OperatorRole.ADMIN, true);
    operators.pushPasswordHistory("alice", "hash-1", 5);
    operators.pushPasswordHistory("alice", "hash-2", 5);
    operators.pushPasswordHistory("alice", "hash-3", 5);
    assertEquals(List.of("hash-3", "hash-2", "hash-1"), operators.passwordHistoryHashes("alice"),
        "历史按设定先后倒序,最新在前");
  }

  @Test
  void passwordHistory_pushTrimsToKeepNewest() {
    operators.upsert("alice", OperatorRole.ADMIN, true);
    for (int i = 1; i <= 6; i++) operators.pushPasswordHistory("alice", "hash-" + i, 3);
    assertEquals(List.of("hash-6", "hash-5", "hash-4"), operators.passwordHistoryHashes("alice"),
        "只保留最新 keep=3 条,旧的淘汰");
  }

  @Test
  void passwordHistory_scopedPerOperator() {
    operators.upsert("alice", OperatorRole.ADMIN, true);
    operators.upsert("bob", OperatorRole.OPERATOR, true);
    operators.pushPasswordHistory("alice", "alice-hash", 5);
    assertEquals(List.of("alice-hash"), operators.passwordHistoryHashes("alice"));
    assertTrue(operators.passwordHistoryHashes("bob").isEmpty(), "历史按操作者隔离,互不串扰");
  }
}