

package py.coordinator.lib;

import py.coordinator.iorequest.iorequest.IoRequest;
import py.drivercontainer.exception.ProcessPositionException;
import py.exception.StorageException;


public interface IoConvertor {

  public void processWriteRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws ProcessPositionException, StorageException;

  public void processReadRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws ProcessPositionException, StorageException;

  public void processDiscardRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws ProcessPositionException, StorageException;

  public void fireWrite(Long requestUuid) throws StorageException;

  public void fireRead(Long requestUuid) throws StorageException;

  public void fireDiscard(Long requestUuid) throws StorageException;

}
