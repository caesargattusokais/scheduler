package dev.scheduler.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.OperatorEntry;
import dev.scheduler.core.OperatorRole;
import dev.scheduler.persistence.AuthRepository;
import dev.scheduler.persistence.OperatorRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** 纯单测(不连 DB):BCrypt 前缀、长度校验、存在性校验、revokeAllForOperator 调用、bootstrap 仅作用于无密者。 */
class OperatorPasswordServiceTest {

  /** 记录写入的假 OperatorRepository。 */
  private static final class FakeOperators implements OperatorRepository {
    final List<OperatorEntry> entries = new ArrayList<>();
    final List<String> hashed = new ArrayList<>();
    final List<String> mustChangeSet = new ArrayList<>();
    final List<String> mustChangeCleared = new ArrayList<>();
    Boolean mustChange = false;
    List<String> passwordless = new ArrayList<>();

    @Override public Optional<OperatorRole> roleOf(String name) {
      return entries.stream().filter(e -> e.name().equals(name) && e.active())
          .map(OperatorEntry::role).findFirst();
    }

    @Override public List<OperatorEntry> list() { return List.copyOf(entries); }

    @Override public void upsert(String name, OperatorRole role, boolean active) {
      entries.removeIf(e -> e.name().equals(name));
      entries.add(new OperatorEntry(name, role, active));
    }

    @Override public void deactivate(String name) { }

    @Override public Optional<String> activePasswordHash(String name) { return Optional.empty(); }

    @Override public void setPassword(String name, String bcryptHash) { hashed.add(name + "=" + bcryptHash); }

    @Override public void setMustChangePassword(String name, boolean v) {
      (v ? mustChangeSet : mustChangeCleared).add(name);
      mustChange = v;
    }

    @Override public Optional<Boolean> mustChangePassword(String name) { return Optional.ofNullable(mustChange); }

    @Override public List<String> namesWithoutPassword() { return passwordless; }
  }

  /** 记录撤销调用的假 AuthRepository。 */
  private static final class FakeAuth implements AuthRepository {
    final List<String> revokedAll = new ArrayList<>();

    @Override public Instant now() { return Instant.now(); }

    @Override public Optional<Session> resolve(String rawToken) { return Optional.empty(); }

    @Override public void create(String rawToken, String operator, java.time.Duration ttl) { }

    @Override public void create(String rawToken, String operator, java.time.Duration ttl, java.time.Instant createdAt) { }

    @Override public void revoke(String rawToken) { }

    @Override public void revokeAllForOperator(String operator) { revokedAll.add(operator); }

    @Override public Optional<Instant> lockedUntil(String name) { return Optional.empty(); }

    @Override public void recordFailure(String name, int maxLockSeconds) { }

    @Override public void resetLockout(String name) { }
  }

  @Test
  void setPassword_forcesBcryptHashAndRevokesSessions() {
    FakeOperators ops = new FakeOperators();
    ops.upsert("alice", OperatorRole.ADMIN, true);
    FakeAuth auth = new FakeAuth();
    OperatorPasswordService svc = new OperatorPasswordService(ops, auth);

    svc.setPassword("alice", "secret-pass");

    assertEquals(1, ops.hashed.size());
    String stored = ops.hashed.get(0);
    assertTrue(stored.startsWith("alice=$2"), "应写入 BCrypt 哈希($2 前缀),got: " + stored);
    assertEquals(List.of("alice"), auth.revokedAll, "改密后应撤销该操作者全部会话");
  }

  @Test
  void setPassword_shortPassword_rejected() {
    FakeOperators ops = new FakeOperators();
    ops.upsert("alice", OperatorRole.ADMIN, true);
    FakeAuth auth = new FakeAuth();
    OperatorPasswordService svc = new OperatorPasswordService(ops, auth);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> svc.setPassword("alice", "short"));
    assertTrue(e.getMessage().contains("8"), e.getMessage());
    assertTrue(ops.hashed.isEmpty(), "过短密码不得落库");
    assertTrue(auth.revokedAll.isEmpty(), "过短密码不得撤销会话");
  }

  @Test
  void setPassword_unknownOperator_rejected() {
    FakeOperators ops = new FakeOperators(); // 空目录,alice 未登记
    FakeAuth auth = new FakeAuth();
    OperatorPasswordService svc = new OperatorPasswordService(ops, auth);

    assertThrows(IllegalArgumentException.class, () -> svc.setPassword("alice", "secret-pass"));
    assertTrue(ops.hashed.isEmpty(), "未知名不得静默落库(避免打错名后看似成功但没改到)");
    assertTrue(auth.revokedAll.isEmpty());
  }

  @Test
  void bootstrap_appliesDefaultToPasswordlessOnly() {
    FakeOperators ops = new FakeOperators();
    ops.upsert("alice", OperatorRole.ADMIN, true);
    ops.upsert("bob", OperatorRole.OPERATOR, true);
    // 仅 bob 当前无密 → 引导应只作用于 bob
    ops.passwordless = List.of("bob");
    FakeAuth auth = new FakeAuth();
    OperatorPasswordService svc = new OperatorPasswordService(ops, auth);

    svc.bootstrap("bob", "boot-pass");

    assertEquals(1, ops.hashed.size());
    assertTrue(ops.hashed.get(0).startsWith("bob=$2"), "bootstrap 也应编码为 BCrypt,got: " + ops.hashed.get(0));
    assertEquals(List.of("bob"), ops.mustChangeSet, "共享默认口令 bootstrap 应置 must_change_password=true");
    // 已有口令的操作者(alice 不在 passwordless)走 bootstrap 应 no-op
    svc.bootstrap("alice", "boot-pass");
    assertEquals(1, ops.hashed.size(), "已有口令的操作者 bootstrap 应跳过");
  }

  /** ADMIN 设密(人类选定口令)→ 清除必须改密标。 */
  @Test
  void setPassword_clearsMustChangeFlag() {
    FakeOperators ops = new FakeOperators();
    ops.upsert("alice", OperatorRole.ADMIN, true);
    FakeAuth auth = new FakeAuth();
    OperatorPasswordService svc = new OperatorPasswordService(ops, auth);

    svc.setPassword("alice", "secret-pass");

    assertEquals(List.of("alice"), ops.mustChangeCleared, "人类选定口令后应清除必须改密标");
  }

  @Test
  void deactivate_revokesAllSessionsForOperator() {
    FakeOperators ops = new FakeOperators();
    ops.upsert("carol", OperatorRole.ADMIN, true);
    FakeAuth auth = new FakeAuth();
    OperatorPasswordService svc = new OperatorPasswordService(ops, auth);

    svc.deactivate("carol");

    assertEquals(List.of("carol"), auth.revokedAll, "停用应撤销该操作者全部会话");
  }
}