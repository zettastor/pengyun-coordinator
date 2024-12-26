
package py.coordinator.lib;


public class DiscardInfo {


  public long offset;
  public int length;

  public DiscardInfo(long offset, int length) {
    this.offset = offset;
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }
}
