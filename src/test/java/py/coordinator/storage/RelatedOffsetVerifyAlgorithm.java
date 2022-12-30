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

import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Utils;
import py.exception.StorageException;

/**
 * xx.
 */
public class RelatedOffsetVerifyAlgorithm {

  private static final Logger logger = LoggerFactory.getLogger(RelatedOffsetVerifyAlgorithm.class);
  private final int delta;
  private final int alignment = 8;

  public RelatedOffsetVerifyAlgorithm(int delta) {
    this.delta = delta;
  }

  public long alignPosition(long position) {
    return (position / alignment) * alignment;
  }


  /**
   * xx.
   */
  public byte[] generateData(long position, int length) {
    if (length % alignment != 0) {
      throw new RuntimeException(
          "the length has to be multiple times of 8" + "offset:" + position + "length:"
              + length);
    }

    long finalOffset = position + delta;
    byte[] src = new byte[length];
    ByteBuffer outputBuf = ByteBuffer.wrap(src);
    int times = length / alignment;
    for (int i = 0; i < times; i++) {
      outputBuf.putLong(finalOffset);
      finalOffset += alignment;
    }

    logger.debug("generateData: {}:{}", position, length);
    return src;
  }

  public void verifyData(long position, byte[] data) throws StorageException {
    verifyData(position, data, 0, data.length);
  }


  /**
   * xx.
   */
  public void verifyData(long position, byte[] data, int offset, int length)
      throws StorageException {
    long tmpPosition = position;
    if (length % alignment != 0) {
      throw new RuntimeException(
          "the length has to be multiple times of 8" + " offset:" + position + "length:"
              + length);
    }

    logger.debug("verifyData: {}:{}", position, length);
    ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);

    for (int i = 0; i < length / alignment; i++) {
      long returnedValue = buffer.getLong();
      if (returnedValue != tmpPosition + delta) {
        logger.warn(
            "Failed to verifyData: {}:{}, the error occur at the offset:{} returned value is {}, "
                + "delta {}",
            position, length, tmpPosition, returnedValue, delta);
        StringBuilder builder = new StringBuilder();

        int offsetInArrayToBuildString = i * 8 - 16;
        if (offsetInArrayToBuildString < 0) {
          offsetInArrayToBuildString = 0;
        }

        Utils.toString(ByteBuffer.wrap(data, offsetInArrayToBuildString, 1024), builder);
        throw new StorageException("something wrong " + builder.toString());
      }

      tmpPosition += 8;
    }
  }
}
