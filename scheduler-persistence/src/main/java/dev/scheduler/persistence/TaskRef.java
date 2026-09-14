package dev.scheduler.persistence;

/** DLQ 行补充的任务标识(只读投影):分片所属任务的任务名 + handlerRef。经 execution→task 关联解析。 */
public record TaskRef(String name, String handlerRef) {}