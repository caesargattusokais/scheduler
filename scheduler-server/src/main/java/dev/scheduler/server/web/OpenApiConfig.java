package dev.scheduler.server.web;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 4b OpenAPI 元信息与安全声明。文档本体(路径/参数/schema)由 springdoc 自动扫描 REST controller 生成,
 * 无需任何 @Operation 注解侵入;本类只装配项目 Title/版本/描述,并把会话认证声明为 HttpOnly cookie
 * (session)安全方案——Swagger UI 据此提示:写端点须先 POST /api/v1/auth/login 取得该 cookie。
 */
@Configuration
public class OpenApiConfig {

  /** 认证 cookie 名,与 {@code security.OperatorInterceptor} {@code COOKIE="session"} 一致。 */
  static final String SESSION_COOKIE = "session";

  @Bean
  OpenAPI schedulerOpenApi() {
    return new OpenAPI()
        .info(new Info()
            .title("Scheduler 调度器 API")
            .version("v1")
            .description("多分片执行调度平台:任务定义、执行/DAG/DLQ 控制面、审计与企业治理。"
                + "写端点需先 POST /api/v1/auth/login 取得 session HttpOnly cookie;审计读与文档开放。"))
        .addSecurityItem(new SecurityRequirement().addList("sessionAuth"))
        .components(new Components().addSecuritySchemes("sessionAuth",
            new SecurityScheme()
                .type(SecurityScheme.Type.APIKEY)
                .in(SecurityScheme.In.COOKIE)
                .name(SESSION_COOKIE)));
  }
}