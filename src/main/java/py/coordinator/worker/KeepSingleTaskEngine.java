
package py.coordinator.worker;

import java.util.List;
import py.coordinator.task.SingleTask;


public interface KeepSingleTaskEngine {

  public boolean putTask(SingleTask singleTask);

  public void start();

  public void stop();

  public void process(List<SingleTask> singleTasks);

  public void freeTask(SingleTask singleTask);


  public int getTaskCount();
}
