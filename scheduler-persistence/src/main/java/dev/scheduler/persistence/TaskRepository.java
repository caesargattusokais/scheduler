package dev.scheduler.persistence;
import dev.scheduler.core.Task;
import java.util.List;
import java.util.Optional;

public interface TaskRepository {
  Task create(Task t);
  Optional<Task> findById(long id);
  List<Task> findCronEnabled();
  List<Task> findAll();
  void setPaused(long id, boolean paused);
}
