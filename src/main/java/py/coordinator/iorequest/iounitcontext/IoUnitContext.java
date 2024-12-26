

package py.coordinator.iorequest.iounitcontext;

import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;


public interface IoUnitContext extends Comparable<IoUnitContext> {

  public IoRequestType getRequestType();

  public IoUnit getIoUnit();


  public void done();

  public boolean hasDone();

  public int getSegIndex();

  public long getPageIndexInSegment();

  public IoRequest getIoRequest();

  public void releaseReference(boolean isParticularlyWrite);
}
