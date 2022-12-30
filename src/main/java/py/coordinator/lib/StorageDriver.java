/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.coordinator.lib;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.NotImplementedException;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.exception.StorageException;
import py.storage.Storage;


public abstract class StorageDriver extends Storage {

  public StorageDriver(String identifier) {
    super(identifier);
  }

 
  @Override
  public void read(long pos, byte[] dstBuf, int off, int len) throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  @Override
  public void read(long pos, ByteBuffer buffer) throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  @Override
  public void write(long pos, byte[] buf, int off, int len) throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  @Override
  public void write(long pos, ByteBuffer buffer) throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

 
  public void asyncRead(long pos, byte[] dstBuf, int off, int len, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public void asyncRead(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public void asyncWrite(long pos, byte[] buf, int off, int len, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public void asyncWrite(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  
  @Override
  public long size() {
    throw new NotImplementedException("this is a StorageDriver");
  }

  @Override
  public void close() throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public void open(Long volumeId, int snapshotId) throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public AtomicLong getRequestVolumeId() {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public void pause() {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public void restart() {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public boolean isPause() {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public boolean hasIoRequest() {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public void sendStartOnlineMigrationRequestToAllDatanode() {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public boolean checkAllDatanodeNotifySuccessfullyOrResend() {
    throw new NotImplementedException("this is a StorageDriver");
  }

  

  public void getTickets(int ticketCount) throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public void accumulateIoRequest(Long requestUuid, IoRequest ioRequest) throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }

  public void submitIoRequests(Long requestUuid) throws StorageException {
    throw new NotImplementedException("this is a StorageDriver");
  }
}
