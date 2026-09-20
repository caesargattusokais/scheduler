package dev.scheduler.core;

/** 操作者目录条目(与 app_operator 一行对应)。 */
public record OperatorEntry(String name, OperatorRole role, boolean active) {}