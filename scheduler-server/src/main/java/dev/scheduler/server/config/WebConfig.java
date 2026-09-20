package dev.scheduler.server.config;

import dev.scheduler.server.security.OperatorInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** 注册写端点操作者拦截器到 /api/v1/**(含 MockMvc 集成测试)。 */
@Configuration
public class WebConfig implements WebMvcConfigurer {
  private final OperatorInterceptor operatorInterceptor;

  public WebConfig(OperatorInterceptor operatorInterceptor) {
    this.operatorInterceptor = operatorInterceptor;
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(operatorInterceptor).addPathPatterns("/api/v1/**");
  }
}