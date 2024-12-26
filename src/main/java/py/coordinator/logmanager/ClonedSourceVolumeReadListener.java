
package py.coordinator.logmanager;


public interface ClonedSourceVolumeReadListener {


  void done();


  boolean isDone();


  void failed(Throwable throwable);

  public enum ClonedProcessStatus {
    Step_Begin,
    Step_ReadDone,
    Step_WriteDone,
    Step_Close,
  }
}
