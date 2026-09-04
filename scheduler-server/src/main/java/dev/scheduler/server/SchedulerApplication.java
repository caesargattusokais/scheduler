package dev.scheduler.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** 调度节点引导入口:装配仓库 / handler 注册表 / 选主 / 触发 / 执行并启动固定周期循环。 */
@SpringBootApplication
@EnableScheduling
public class SchedulerApplication {

  public static void main(String[] args) {
    SpringApplication.run(SchedulerApplication.class, args);
  }
}
