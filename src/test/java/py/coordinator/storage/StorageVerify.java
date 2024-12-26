
package py.coordinator.storage;

/**
 * xx.
 */
public interface StorageVerify {

  public void verify();

  public void startWrite();

  public void stopWrite();

  public int getErrorCount();
}
