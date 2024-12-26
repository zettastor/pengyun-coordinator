

package py.coordinator.iorequest.iounit;

import py.buffer.PyBuffer;


public interface IoUnit {

  public long getOffset();

  public int getLength();

  public PyBuffer getPyBuffer();

  public void setPyBuffer(PyBuffer pyBuffer);

  public int getSegIndex();

  public boolean isSuccess();

  public void setSuccess(boolean success);

  public long getPageIndexInSegment();

  public void releaseReference();
}
