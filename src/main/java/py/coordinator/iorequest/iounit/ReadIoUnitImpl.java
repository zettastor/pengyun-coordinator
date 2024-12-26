
package py.coordinator.iorequest.iounit;

import py.buffer.PyBuffer;


public class ReadIoUnitImpl extends BaseIoUnit {

  private PyBuffer pyBuffer;

  public ReadIoUnitImpl(int segIndex, long pageIndex, long offset, int length) {
    super(segIndex, pageIndex, offset, length);
  }

  @Override
  public String toString() {
    return "ReadIOUnitImpl [super=" + super.toString() + "]";
  }

  @Override
  public void releaseReference() {
    if (pyBuffer != null) {
      pyBuffer.releaseReference();
      pyBuffer = null;
    }
  }

  public PyBuffer getPyBuffer() {
    return pyBuffer;
  }

  public void setPyBuffer(PyBuffer pyBuffer) {
    this.pyBuffer = pyBuffer;
  }
}
