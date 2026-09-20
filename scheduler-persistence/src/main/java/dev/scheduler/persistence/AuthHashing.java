package dev.scheduler.persistence;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** 共享 SHA-256 工具:会话令牌散列等。持久层与 server 集成测试(播种会话)同源复用。 */
public final class AuthHashing {
  private AuthHashing() {}

  /** SHA-256(input) 十六进制文本(64-char lowercase);同 PG encode(digest(...),'hex')。 */
  public static String sha256(String input) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
    return HexFormat.of().formatHex(md.digest(input.getBytes(StandardCharsets.UTF_8)));
  }
}