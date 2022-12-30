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
