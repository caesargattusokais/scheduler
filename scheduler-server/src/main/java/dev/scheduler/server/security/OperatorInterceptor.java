package dev.scheduler.server.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.OperatorRole;
import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.OperatorRepository;
import dev.scheduler.server.service.AuditRecorder;
import dev.scheduler.server.service.AuthService;
import dev.scheduler.server.service.AuthService.ResolveResult;
import dev.scheduler.server.web.AuthController;
import dev.scheduler.server.web.ApiExceptionHandler.ApiError;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * 写端点操作者授权 + 会话 cookie 身份解析(替代旧 X-Operator 自报头信任)。
 * <p>对 /api/v1/** :
 * <ul>
 *   <li>每个请求先 resolve {@code session} cookie → {@link CurrentOperator} + 剩余 TTL&lt;半程时轮换(回写新 cookie)。</li>
 *   <li>{@code /api/v1/auth/**} → 开放(login/logout/me 自带身份语义),但已 resolve/轮换。</li>
 *   <li>操作者管理路径 {@code /api/v1/operators/**} → 全部方法需 ADMIN;</li>
 *   <li>敏感写端点(见 {@link #ADMIN_WRITES})→ 需 ADMIN;</li>
 *   <li>其余写方法(POST/PUT/DELETE)→ 需登录且活跃的任意操作者(OPERATOR 即可);</li>
 *   <li>读端点(GET 等)→ 保持开放(who 已设时供 /me 等读取)。</li>
 * </ul>
 * 拒绝时记 {@code access.denied} 审计(operator 取已解析身份,否则 anonymous)。
 * 请求完成清 {@link CurrentOperator}(ThreadLocal 防泄漏)。由于写端点已由本拦截器校验身份,
 * 进入控制器时 {@code CurrentOperator.get()} 即为可信登记操作者,控制器审计以此为准。
 */
@Component
public class OperatorInterceptor implements HandlerInterceptor {
  private static final String PREFIX = "/api/v1";
  private static final String AUTH_PREFIX = "/api/v1/auth/";
  private static final String COOKIE = "session";
  private static final String OPERATORS = "/api/v1/operators/**";

  /** 提权到 ADMIN 的敏感写端点:删除 / 取消 / 触发 DAG / 归档审计。其余写端点 OPERATOR 即可。 */
  private static final List<String[]> ADMIN_WRITES = List.of(
      new String[]{"DELETE", "/api/v1/tasks/{id}"},
      new String[]{"POST", "/api/v1/executions/{id}/cancel"},
      new String[]{"POST", "/api/v1/dags/{id}/trigger"},
      new String[]{"POST", "/api/v1/audits/archive"});

  private final OperatorRepository operators;
  private final AuditRecorder auditor;
  private final ObjectMapper json;
  private final AuthService auth;
  private final CurrentOperator current;
  private final boolean secureCookies;
  private final AntPathMatcher path = new AntPathMatcher();

  public OperatorInterceptor(OperatorRepository operators, AuditRecorder auditor, ObjectMapper json,
                             AuthService auth, CurrentOperator current,
                             @Value("${scheduler.auth.secure-cookies:false}") boolean secureCookies) {
    this.operators = operators;
    this.auditor = auditor;
    this.json = json;
    this.auth = auth;
    this.current = current;
    this.secureCookies = secureCookies;
  }

  @Override
  public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws IOException {
    String uri = req.getRequestURI();
    if (!uri.startsWith(PREFIX)) return true;
    boolean isAuth = uri.startsWith(AUTH_PREFIX);
    String who = resolveIdentity(req, res); // cookie → CurrentOperator + 轮换;无效 → null
    if (isAuth) return true;                // auth/(login/me/logout) 开放门禁;身份已由 resolve 消费
    String method = req.getMethod();
    boolean management = path.match(OPERATORS, uri);
    if (!management && !isWrite(method)) return true; // 读端点开放(who 已设时已入 CurrentOperator)

    OperatorRole required = management
        ? OperatorRole.ADMIN
        : (requiresAdmin(uri, method) ? OperatorRole.ADMIN : OperatorRole.OPERATOR);

    if (who == null) {
      return deny(req, res, 401, required, "authentication required");
    }
    Optional<OperatorRole> role = operators.roleOf(who);
    if (role.isEmpty() || role.get().rank() < required.rank()) {
      return deny(req, res, 403, required, "insufficient role for write (needs " + required.name() + ")");
    }
    return true;
  }

  @Override
  public void afterCompletion(HttpServletRequest req, HttpServletResponse res,
                              Object handler, Exception ex) {
    current.clear();
  }

  /** 解析 cookie 会话 → 设置 CurrentOperator;余量不足半程时轮换并回写新 cookie。无效 → null(匿名)。 */
  private String resolveIdentity(HttpServletRequest req, HttpServletResponse res) throws IOException {
    String token = cookieToken(req);
    if (token == null) return null;
    ResolveResult r = auth.resolve(token);
    if (r == null || r.operator() == null) return null; // 无效/过期/强制重登 → 匿名
    current.set(r.operator());
    if (r.rotatedToken() != null) {
      AuthController.setCookie(res, r.rotatedToken(), secureCookies);
    }
    return r.operator();
  }

  /** 敏感写到 ADMIN 的判定:命中任一 (method, pathPattern) 规则。 */
  private boolean requiresAdmin(String uri, String method) {
    return ADMIN_WRITES.stream()
        .anyMatch(r -> method.equals(r[0]) && path.match(r[1], uri));
  }

  private static boolean isWrite(String method) {
    return method.equals("POST") || method.equals("PUT") || method.equals("DELETE");
  }

  private static String cookieToken(HttpServletRequest req) {
    Cookie[] cs = req.getCookies();
    if (cs == null) return null;
    for (Cookie c : cs) {
      if (COOKIE.equals(c.getName())) {
        String v = c.getValue();
        return (v == null || v.isBlank()) ? null : v.trim();
      }
    }
    return null;
  }

  /** 写 401/403 JSON 并记 access.denied 审计;返回 false 终止继续处理。 */
  private boolean deny(HttpServletRequest req, HttpServletResponse res, int status, OperatorRole required,
                       String reason) throws IOException {
    String who = current.get();
    auditor.record(who == null ? "anonymous" : who,
        "access.denied", targetFrom(req.getRequestURI()), 0L,
        Map.of("path", req.getRequestURI(), "method", req.getMethod(),
            "reason", reason, "required", required.name()));
    res.setStatus(status);
    res.setContentType("application/json");
    res.getWriter().write(json.writeValueAsString(new ApiError(reason)));
    return false;
  }

  /** access.denied 的归属目标类型:按 path 前缀取最接近的资源;失败兜底 DAG(仅作审计归类,不承载资源语义)。 */
  private static TargetType targetFrom(String uri) {
    if (uri.contains("/tasks")) return TargetType.TASK;
    if (uri.contains("/executions")) return TargetType.EXECUTION;
    if (uri.contains("/dags")) return TargetType.DAG;
    return TargetType.DAG;
  }
}