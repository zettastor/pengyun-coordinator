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

package py.coordinator.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.exception.StorageException;

/**
 * xx.
 */
public abstract class AbstractStorageVerify implements StorageVerify {

  private static final Logger logger = LoggerFactory.getLogger(AbstractStorageVerify.class);
  private final RelatedOffsetVerifyAlgorithm algorithm;

  public AbstractStorageVerify(int delta) {
    logger.warn("current delta: {}", delta);
    algorithm = new RelatedOffsetVerifyAlgorithm(delta);
  }

  public long alignPosition(long position) {
    return algorithm.alignPosition(position);
  }

  public int alginPosition(int size) {
    return (int) algorithm.alignPosition(size);
  }

  public byte[] generateData(long position, int length) {
    return algorithm.generateData(position, length);
  }

  public void verifyData(long position, byte[] data) throws StorageException {
    algorithm.verifyData(position, data);
  }

}
