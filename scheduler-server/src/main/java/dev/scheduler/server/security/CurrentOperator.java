package dev.scheduler.server.security;

import org.springframework.stereotype.Component;

/**
 * 当前请求操作者(ThreadLocal):由 {@link OperatorInterceptor} resolve cookie 会话后写入;
 * 控制器读它以记 audit operator 与 /auth/me 响应。请求终止后须 {@link #clear()}。null = 未认证。
 */
@Component
public class CurrentOperator {
  private final ThreadLocal<String> holder = new ThreadLocal<>();

  public void set(String operator) { holder.set(operator); }

  /** null = 未认证(无有效会话 cookie)。 */
  public String get() { return holder.get(); }

  public void clear() { holder.remove(); }
}