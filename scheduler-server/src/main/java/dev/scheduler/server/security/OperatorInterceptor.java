package dev.scheduler.server.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.OperatorRole;
import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.OperatorRepository;
import dev.scheduler.server.service.AuditRecorder;
import dev.scheduler.server.web.ApiExceptionHandler.ApiError;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * 写端点操作者授权(已声明身份上的白名单 + 角色;非密码/令牌式认证)。
 * <p>对 /api/v1/** :
 * <ul>
 *   <li>操作者管理路径 /api/v1/operators/** → 全部方法需 ADMIN。</li>
 *   <li>敏感写端点(见 {@link #ADMIN_WRITES})→ 需 ADMIN;</li>
 *   <li>其余写方法(POST/PUT/DELETE)→ 需登录且活跃的任意操作者(OPERATOR 即可);</li>
 *   <li>读端点(GET 等)→ 保持开放(审计只读不受此限制)。</li>
 * </ul>
 * 每次拒绝记一条 {@code access.denied} 审计(自称操作者 + 被拒 path/method/reason),自动进取证链。
 * 由于写端点已由本拦截器校验身份,进入控制器时 X-Operator 即为可信的登记操作者。
 */
@Component
public class OperatorInterceptor implements HandlerInterceptor {
  private static final String PREFIX = "/api/v1";
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
  private final AntPathMatcher path = new AntPathMatcher();

  public OperatorInterceptor(OperatorRepository operators, AuditRecorder auditor, ObjectMapper json) {
    this.operators = operators;
    this.auditor = auditor;
    this.json = json;
  }

  @Override
  public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws IOException {
    String uri = req.getRequestURI();
    if (!uri.startsWith(PREFIX)) return true;
    String method = req.getMethod();
    boolean management = path.match(OPERATORS, uri);
    if (!management && !isWrite(method)) return true; // 读端点开放

    OperatorRole required = management
        ? OperatorRole.ADMIN
        : (requiresAdmin(uri, method) ? OperatorRole.ADMIN : OperatorRole.OPERATOR);

    String who = nonBlank(req.getHeader("X-Operator"));
    if (who == null) {
      return deny(req, res, 401, required, "X-Operator required for write");
    }
    Optional<OperatorRole> role = operators.roleOf(who);
    if (role.isEmpty() || role.get().rank() < required.rank()) {
      return deny(req, res, 403, required, "insufficient role for write (needs " + required.name() + ")");
    }
    return true;
  }

  /** 敏感写到 ADMIN 的判定:命中任一 (method, pathPattern) 规则。 */
  private boolean requiresAdmin(String uri, String method) {
    return ADMIN_WRITES.stream()
        .anyMatch(r -> method.equals(r[0]) && path.match(r[1], uri));
  }

  private static boolean isWrite(String method) {
    return method.equals("POST") || method.equals("PUT") || method.equals("DELETE");
  }

  private static String nonBlank(String s) {
    return (s == null || s.isBlank()) ? null : s.trim();
  }

  /** 写 401/403 JSON 并记 access.denied 审计;返回 false 终止继续处理。 */
  private boolean deny(HttpServletRequest req, HttpServletResponse res, int status, OperatorRole required,
                       String reason) throws IOException {
    auditor.record(nonBlank(req.getHeader("X-Operator")) == null ? "anonymous" : req.getHeader("X-Operator").trim(),
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