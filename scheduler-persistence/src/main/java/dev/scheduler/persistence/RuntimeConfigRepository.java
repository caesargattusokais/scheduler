package dev.scheduler.persistence;

import java.util.List;
import java.util.Optional;

public interface RuntimeConfigRepository {
  Optional<RuntimeConfigRow> find(String key);
  List<RuntimeConfigRow> findAll();
  void upsert(String key, String value, String operator);
}