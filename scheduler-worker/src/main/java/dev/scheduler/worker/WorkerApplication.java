package dev.scheduler.worker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** 执行节点引导入口:装配 handler/executor/retry + 心跳注册。扫描 dev.scheduler.worker.*。 */
@SpringBootApplication
@EnableScheduling
public class WorkerApplication {
  public static void main(String[] args) {
    SpringApplication.run(WorkerApplication.class, args);
  }
}