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

package py.coordinator;

import java.nio.ByteBuffer;
import org.apache.commons.lang.NotImplementedException;
import py.coordinator.calculator.LogicalToPhysicalCalculator;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.exception.StorageException;
import py.storage.Storage;


public abstract class HighSpeedStorage extends Storage {

  public HighSpeedStorage(String identifier) {
    super(identifier);
  }

  public abstract void submit(IoUnitContextPacket callback) throws StorageException;

  public LogicalToPhysicalCalculator getLogicalToPhysicalCalculator() {
    throw new NotImplementedException("this is a HighSpeedStorage");
  }

  @Override
  public void read(long pos, byte[] dstBuf, int off, int len) throws StorageException {
    throw new NotImplementedException("this is a HighSpeedStorage");
  }

  @Override
  public void read(long pos, ByteBuffer buffer) throws StorageException {
    throw new NotImplementedException("this is a HighSpeedStorage");
  }

  @Override
  public void write(long pos, byte[] buf, int off, int len) throws StorageException {
    throw new NotImplementedException("this is a HighSpeedStorage");
  }

  @Override
  public void write(long pos, ByteBuffer buffer) throws StorageException {
    throw new NotImplementedException("this is a HighSpeedStorage");
  }

  @Override
  public long size() {
    throw new NotImplementedException("this is a HighSpeedStorage");
  }
}
