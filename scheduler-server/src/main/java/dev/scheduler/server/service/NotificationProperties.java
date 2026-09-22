package dev.scheduler.server.service;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 通知底座配置 {@code scheduler.notifications.*}:webhooks 列表(默认空 → 投递禁用)。
 * 每个 webhook:url、签名 secret、kinds(空 = 订阅全部)、enabled、maxAttempts(默认 5,含首次的发送总次数)、
 * backoffMs(默认 1000,指数退避基数)。V1 webhook 端点走配置文件(DB 表留待后续)。
 */
@Component
@ConfigurationProperties(prefix = "scheduler.notifications")
public class NotificationProperties {
  private List<Webhook> webhooks = new ArrayList<>();

  public List<Webhook> getWebhooks() { return webhooks; }
  public void setWebhooks(List<Webhook> webhooks) { this.webhooks = webhooks; }

  public static class Webhook {
    private String url = "";
    private String secret = "";
    private boolean enabled = true;
    private List<String> kinds = new ArrayList<>();
    private int maxAttempts = 5;
    private long backoffMs = 1000;

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public String getSecret() { return secret; }
    public void setSecret(String secret) { this.secret = secret; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public List<String> getKinds() { return kinds; }
    public void setKinds(List<String> kinds) { this.kinds = kinds; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getBackoffMs() { return backoffMs; }
    public void setBackoffMs(long backoffMs) { this.backoffMs = backoffMs; }
  }
}