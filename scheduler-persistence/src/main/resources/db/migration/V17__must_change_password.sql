-- 操作者强认证 · 首登强制改密:must_change_password=true 表示该口令由共享引导默认(scheduler.operators.default-password)
-- 置位、尚无人类选定过,登录/me 暴露后前端强制改密。任一人类选定路径(自助 change-password、ADMIN /operators/{name}/password)清除。
ALTER TABLE app_operator ADD COLUMN must_change_password boolean NOT NULL DEFAULT false;