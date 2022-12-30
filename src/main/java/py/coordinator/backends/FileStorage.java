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

package py.coordinator.backends;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.lib.AsyncIoCallBack;
import py.coordinator.lib.StorageDriver;
import py.exception.StorageException;
import py.thrift.datanode.service.ReadRequestUnit;
import py.thrift.datanode.service.ReadResponseUnit;
import py.thrift.datanode.service.WriteRequestUnit;
import py.thrift.datanode.service.WriteResponseUnit;
import py.thrift.share.ReadUnitResultThrift;


public class FileStorage extends StorageDriver {

  private static final Logger logger = LoggerFactory.getLogger(FileStorage.class);
  private RandomAccessFile file;
  private String filePath;
  private long fileSize;



  public FileStorage(String identifier) {
    super(identifier);
  }

  /**
   * read are synchronous.
   */
  public void read(long pos, byte[] buf, int off, int len) throws StorageException {
    logger.debug("read at {} length is {}", pos, len);
    try {
      file.seek(pos);
      file.readFully(buf, off, len);
      logger.debug("data is {}", Arrays.toString(buf));
    } catch (Exception e) {
      logger.error("failed to write to the storage", e);
      throw new StorageException(e);
    } finally {
      logger.info("nothing need to do here");
    }
  }

  @Override
  public void read(long pos, ByteBuffer buffer) throws StorageException {
    throw new StorageException("no implemented");
  }



  public List<ReadResponseUnit> read(List<ReadRequestUnit> readRequestUnits)
      throws StorageException {
    List<ReadResponseUnit> responseUnits = new ArrayList<>();

    for (ReadRequestUnit unit : readRequestUnits) {
      long offset = unit.getOffset();
      int length = unit.getLength();
      ReadUnitResultThrift result = ReadUnitResultThrift.OK;
      byte[] data = new byte[length];

      read(offset, data, 0, length);
      ReadResponseUnit responseUnit = new ReadResponseUnit(offset, length, result,
          RequestIdBuilder.get());
      responseUnit.setData(data);
      responseUnits.add(responseUnit);
    }

    return responseUnits;
  }

  /**
   * Writes are synchronous.
   */
  public void write(long pos, byte[] buf, int off, int len) throws StorageException {
    logger.debug("write at {} buf:{} offset: {} length: {}", pos, Arrays.toString(buf), off, len);
    try {
      file.seek(pos);
      file.write(buf, off, len);
    } catch (IOException e) {
      logger.error("failed to write to the storage", e);
      throw new StorageException(e);
    } finally {
      logger.info("nothing need to do here");
    }
  }

  @Override
  public void write(long pos, ByteBuffer buffer) throws StorageException {
    throw new StorageException("no implemented");
  }



  public List<WriteResponseUnit> write(List<WriteRequestUnit> writeRequestUnits)
      throws StorageException {
    List<WriteResponseUnit> responseUnits = new ArrayList<>();

    for (WriteRequestUnit unit : writeRequestUnits) {
      write(unit.getOffset(), unit.getData(), 0, unit.getData().length);
    }
    return responseUnits;
  }

  @Override
  public void close() throws StorageException {
    try {
      file.close();
    } catch (Exception e) {
      logger.error("failed to close the storage ", e);
      throw new StorageException(e);
    }
  }


  @Override
  public void open() throws StorageException {
    try {
      file = new RandomAccessFile(filePath.toString(), "rws");
      file.setLength(fileSize);
    } catch (Exception e) {
      logger.error("can't create a backend file for the storage", e);
      throw new StorageException(e);
    }
  }


  @Override
  public void asyncRead(long pos, byte[] dstBuf, int off, int len, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {

  }

  @Override
  public void asyncRead(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {

  }

  @Override
  public void asyncWrite(long pos, byte[] buf, int off, int len, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {

  }

  @Override
  public void asyncWrite(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {

  }

  public void accumulateIoRequest(Long requestUuid, IoRequest ioRequest) throws StorageException {
  }

  public void submitIoRequests(Long requestUuid) throws StorageException {
  }

  @Override
  public long size() {
    return fileSize;
  }



  public void setConfig(String filePath, long fileSize) {

    this.filePath = filePath;
    this.fileSize = fileSize;
  }

}
