/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
