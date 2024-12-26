
package py.coordinator.task;


public abstract class SingleTask<T> implements Comparable<SingleTask<T>> {

  public abstract Long getRequestId();

  public abstract T getCompareKey();

  public abstract Long getVolumeId();
}
