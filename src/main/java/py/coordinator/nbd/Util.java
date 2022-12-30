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

package py.coordinator.nbd;

import io.netty.buffer.ByteBuf;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;
import py.drivercontainer.exception.ProcessPositionException;


public class Util {

  private static final Logger logger = LoggerFactory.getLogger(Util.class);

  public static final int byteArrayToInt(byte[] b) {
    return ((((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16) | ((b[2] & 0xff) << 8) | ((b[3] & 0xff)
        << 0)));
  }


  
  public static final long byteArrayToLong(byte[] b) {
    return ((((long) b[0] & 0xff) << 56) | (((long) b[1] & 0xff) << 48) | (((long) b[2] & 0xff)
        << 40)
        | (((long) b[3] & 0xff) << 32) | (((long) b[4] & 0xff) << 24) | (((long) b[5] & 0xff) << 16)
        | (((long) b[6] & 0xff) << 8) | (((long) b[7] & 0xff) << 0));
  }


  
  public static final long byteArrayInLittleEndianToLong(byte[] b) {
    return ((((long) b[7] & 0xff) << 56) | (((long) b[6] & 0xff) << 48) | (((long) b[5] & 0xff)
        << 40)
        | (((long) b[4] & 0xff) << 32) | (((long) b[3] & 0xff) << 24) | (((long) b[2] & 0xff) << 16)
        | (((long) b[1] & 0xff) << 8) | (((long) b[0] & 0xff) << 0));
  }

  
  public static final byte[] intToByteArray(int i) {
    byte[] b = new byte[4];

    b[0] = (byte) (0xFF & (i >> 24));
    b[1] = (byte) (0xFF & (i >> 16));
    b[2] = (byte) (0xFF & (i >> 8));
    b[3] = (byte) (0xFF & i);

    return b;
  }

  
  public static final byte[] zeros(int size) {
    byte[] b = new byte[size];

    for (int i = 0; i < size; i++) {
      b[i] = 0x00;
    }
    return b;
  }

  
  public static final byte[] longToByteArray(long l) {
    byte[] b = new byte[8];

    b[0] = (byte) (0xFF & (l >> 56));
    b[1] = (byte) (0xFF & (l >> 48));
    b[2] = (byte) (0xFF & (l >> 40));
    b[3] = (byte) (0xFF & (l >> 32));
    b[4] = (byte) (0xFF & (l >> 24));
    b[5] = (byte) (0xFF & (l >> 16));
    b[6] = (byte) (0xFF & (l >> 8));
    b[7] = (byte) (0xFF & l);

    return b;
  }


  
  public static final String bytesToString(byte[] array) {
    StringBuilder sb = new StringBuilder("\n");
    int count = 0;
    for (byte b : array) {
      sb.append(String.format("%x ", b));
      if (++count % 32 == 0) {
        sb.append("\n");
      }
    }
    return sb.toString();
  }


  
  public static final String bytesToString(ByteBuf byteBuf) {
    StringBuilder sb = new StringBuilder("\n");
    int count = 0;

    int readerIndex = byteBuf.readerIndex();
    for (int i = 0; i < byteBuf.readableBytes(); i++) {
      byte b = byteBuf.getByte(readerIndex + i);
      sb.append(String.format("%x ", b));
      if (++count % 32 == 0) {
        sb.append("\n");
      }
    }
    return sb.toString();
  }

  public static int segmentIndex(long pos, long segmentSize) {
    return (int) (pos / segmentSize);
  }

  public static int pageIndex(long pos, long pageSize) {
    return (int) (pos / pageSize);
  }


  
  public static long convertPosition(long currentPos, long pageSize, long segmentSize,
      long segmentCount) {
    long convertedPos = 0L;

    long seqPageIndex = currentPos / pageSize;

    long segmentIndex = seqPageIndex % segmentCount;
    long manageIndexInSegment = seqPageIndex / segmentCount;

    convertedPos =
        (segmentIndex * segmentSize) + (manageIndexInSegment * pageSize) + (currentPos % pageSize);
    return convertedPos;
  }


  
  public static long convertPosition(long currentPos, long pageSize, long segmentSize,
      long segmentCount,
      int pagePackageCount) {

    long pagesPackageSize = pageSize * pagePackageCount;
    long convertedPos;

   
   
   
    long noOverFlowPos = (segmentSize / pagesPackageSize) * segmentCount * pagesPackageSize;
    if (currentPos > noOverFlowPos) {
     
      long sizeOfLastPackage = segmentSize % (pageSize * pagePackageCount);
      int segmentIndex = (int) ((currentPos - noOverFlowPos) / sizeOfLastPackage);
      long offsetInLastPackage = (currentPos - noOverFlowPos) % sizeOfLastPackage;
      convertedPos = segmentIndex * segmentSize + segmentSize / pagesPackageSize * pagesPackageSize
          + offsetInLastPackage;
      return convertedPos;
    } else {
      long seqPageIndex = currentPos / pagesPackageSize;

      long segmentIndex = seqPageIndex % segmentCount;
      long manageIndexInSegment = seqPageIndex / segmentCount;

      convertedPos =
          (segmentIndex * segmentSize) + (manageIndexInSegment * pagesPackageSize) + (currentPos
              % pagesPackageSize);

      return convertedPos;
    }
  }

  
  public static long convertBackPosition(long convertPos, long pageSize, long segmentSize,
      long segmentCount) {
    long originPos = 0L;
    long originSegmentIndex = 0L;
    long originPageIndex = 0L;
    long pageCount = segmentSize / pageSize;

   
    long convertPageIndex = convertPos / pageSize;
    long convertSegmentIndex = convertPageIndex / pageCount;
    long convertPageIndexInSegment = convertPageIndex % pageCount;

   
    long logicPageIndex = (convertPageIndexInSegment * segmentCount) + convertSegmentIndex;
    originSegmentIndex = logicPageIndex / pageCount;
    originPageIndex = logicPageIndex % pageCount;

   
    originPos =
        (originSegmentIndex * segmentSize) + (originPageIndex * pageSize) + (convertPos % pageSize);

    return originPos;
  }

  
  @Deprecated
  public static long convertBackPosition(long convertPos, long pageSize, long segmentSize,
      long segmentCount,
      int pagePackageCount) {
    long packagePageSize = pageSize * pagePackageCount;
    long originPos = 0L;
    long originSegmentIndex = 0L;
    long originPageIndex = 0L;
    long pageCount = segmentSize / packagePageSize;

   
    long convertPageIndex = convertPos / packagePageSize;
    long convertSegmentIndex = convertPageIndex / pageCount;
    long convertPageIndexInSegment = convertPageIndex % pageCount;

   
    long logicPageIndex = (convertPageIndexInSegment * segmentCount) + convertSegmentIndex;
    originSegmentIndex = logicPageIndex / pageCount;
    originPageIndex = logicPageIndex % pageCount;

   
    originPos = (originSegmentIndex * segmentSize) + (originPageIndex * packagePageSize)
        + (convertPos % packagePageSize);

    return originPos;
  }


  
  public static Pair<Integer, Long> processConvertPos(long originPos, long pageSize,
      long segmentSize,
      int pagePackageCount, List<Long> countOfSegments, boolean isConvertBack)
      throws ProcessPositionException {
    Long currentVolumeSize = null;
    Long convertPos = null;
    Long basicOffset = 0L;
    int biggerCount = 0;

    for (Long currentSegmentCount : countOfSegments) {
      currentVolumeSize = currentSegmentCount * segmentSize;
      originPos -= currentVolumeSize;

      if (originPos >= 0) {
        basicOffset += currentVolumeSize;
        biggerCount++;
        continue;
      } else {
        originPos += currentVolumeSize;
        if (isConvertBack) {
          convertPos = convertBackPosition(originPos, pageSize, segmentSize, currentSegmentCount,
              pagePackageCount);
        } else {
          convertPos = convertPosition(originPos, pageSize, segmentSize, currentSegmentCount,
              pagePackageCount);
        }
        convertPos += basicOffset;
        break;
      }
    }
    if (biggerCount == countOfSegments.size()) {
      logger.warn("process offset:{} is bigger than volume size, count of segments: {}",
          (originPos + basicOffset), countOfSegments);
      throw new ProcessPositionException();
    }
    Validate.notNull(convertPos);
    Integer segmentIndex = (int) (convertPos / segmentSize);
    Long offsetInSegment = convertPos % segmentSize;
    Pair<Integer, Long> valuePair = new Pair<Integer, Long>(segmentIndex, offsetInSegment);
    return valuePair;
  }

}
