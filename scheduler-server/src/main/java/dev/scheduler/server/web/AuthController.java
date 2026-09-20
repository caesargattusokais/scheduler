package dev.scheduler.server.web;

import dev.scheduler.core.OperatorEntry;
import dev.scheduler.persistence.OperatorRepository;
import dev.scheduler.server.security.CurrentOperator;
import dev.scheduler.server.service.AuthService;
import dev.scheduler.server.service.AuthService.LoginResult;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 认证端点:登录(Set-Cookie 会话 + 退避)/ 当前操作者 / 登出(撤销 + 清 cookie)。 */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {
  private final AuthService auth;
  private final OperatorRepository operators;
  private final CurrentOperator current;
  private final boolean secureCookies;

  public AuthController(AuthService auth, OperatorRepository operators, CurrentOperator current,
                        @Value("${scheduler.auth.secure-cookies:false}") boolean secureCookies) {
    this.auth = auth;
    this.operators = operators;
    this.current = current;
    this.secureCookies = secureCookies;
  }

  /** POST /login {name,password} → 成功 200 + Set-Cookie(session) + {operator,expiresAt};失败/锁定 401。 */
  @PostMapping("/login")
  public ResponseEntity<?> login(@RequestBody Map<String, String> body, HttpServletResponse res) {
    String name = body == null ? null : body.get("name");
    String password = body == null ? null : body.get("password");
    if (name == null || name.isBlank()) {
      throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "name is required");
    }
    Optional<LoginResult> r = auth.login(name.trim(), password);
    if (r.isEmpty()) {
      throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "authentication failed");
    }
    LoginResult lr = r.get();
    setCookie(res, lr.token(), secureCookies);
    OperatorEntry entry = entryOf(lr.operator());
    Map<String, Object> body2 = new LinkedHashMap<>();
    body2.put("operator", entry);
    body2.put("expiresAt", lr.expiresAt().toString());
    return ResponseEntity.ok(body2);
  }

  /** GET /me → 200 {operator} 当有已解析会话;无 cookie/无效 → 401。 */
  @GetMapping("/me")
  public ResponseEntity<?> me() {
    String who = current.get();
    if (who == null) {
      throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "not authenticated");
    }
    return ResponseEntity.ok(Map.of("operator", entryOf(who)));
  }

  /** POST /logout → 撤销当前 cookie 会话并清 cookie(幂等;无会话也 200)。 */
  @PostMapping("/logout")
  public ResponseEntity<?> logout(HttpServletRequest req, HttpServletResponse res) {
    Cookie c = cookie(req, "session");
    if (c == null || c.getValue() == null || c.getValue().isBlank()) {
      return ResponseEntity.ok(Map.of());
    }
    auth.logout(c.getValue());
    // 清除 cookie 须按 secure 分支对齐登录 cookie:Secure 属性开启时浏览器要求删除 cookie 也为 Secure 才覆盖。
    res.addHeader("Set-Cookie", "session=; Path=/api; Max-Age=0; HttpOnly; SameSite=Strict"
        + (secureCookies ? "; Secure" : ""));
    return ResponseEntity.ok(Map.of());
  }

  /** 目录条目;登录/me 均须为已登记活跃操作者(login 已验证),取 null 仅防御。 */
  private OperatorEntry entryOf(String who) {
    return operators.list().stream()
        .filter(e -> e.name().equals(who))
        .findFirst()
        .orElse(null);
  }

  private static Cookie cookie(HttpServletRequest req, String name) {
    Cookie[] cs = req.getCookies();
    if (cs == null) return null;
    for (Cookie c : cs) {
      if (c.getName().equals(name)) return c;
    }
    return null;
  }

  /** 会话 cookie:HttpOnly + SameSite=Strict + Path=/api;Secure 仅当属性开启(测试/MockMvc 默认 false)。 */
  public static void setCookie(HttpServletResponse res, String token, boolean secure) {
    res.addHeader("Set-Cookie",
        "session=" + token + "; Path=/api; HttpOnly; SameSite=Strict" + (secure ? "; Secure" : ""));
  }
}