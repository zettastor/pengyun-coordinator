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

package py.coordinator.base;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Utils;
import py.coordinator.lib.AsyncIoCallBackImpl;
import py.coordinator.lib.Coordinator;
import py.coordinator.lib.StorageDriver;
import py.exception.StorageException;
import py.test.TestUtils.ByteUtils;

/**
 * xx.
 */
public class CoordinatorRandomReadWrite {

  private static final Logger logger = LoggerFactory.getLogger(CoordinatorRandomReadWrite.class);

  private final AtomicLong writeCount = new AtomicLong(0);
  protected StorageDriver coordinator;
  protected int pageSize = 8 * 1024;
  protected boolean isLoopWriteStopped = false;
  protected boolean isLoopReadWriteStopped = false;
  private RangeSet<Long> accessPlanRangeSet;
  private long volumeSize;
  private String errorMsg = "";
  private CountDownLatch loopWriteLatch = null;
  private CountDownLatch readLatch = null;
  private CountDownLatch loopReadWriteLatch = null;
  private long writeReadPausedTorrence = 5000;
  private Semaphore iodepthSemaphore;


  /**
   * xx.
   */
  public CoordinatorRandomReadWrite(long volumeSize, StorageDriver coordinator, int iodepth) {
    accessPlanRangeSet = TreeRangeSet.create();
    this.coordinator = coordinator;
    this.volumeSize = volumeSize;
    this.isLoopWriteStopped = false;
    this.isLoopReadWriteStopped = false;
    iodepthSemaphore = new Semaphore(iodepth);
  }

  public CoordinatorRandomReadWrite(long volumeSize, StorageDriver coordinator) {
    this(volumeSize, coordinator, 8);
  }


  /**
   * xx.
   */
  public static byte[] generateDataAssociatedWithAddress(long offset, int length, int delta)
      throws StorageException {
    if (length % 8 != 0) {
      throw new StorageException(
          "the length has to be multiple times of 8" + "offset:" + offset + "length:" + length);
    }

    long finalOffset = offset + delta;
    byte[] src = new byte[length];
    ByteBuffer outputBuf = ByteBuffer.wrap(src);
    int times = length / 8;
    for (int i = 0; i < times; i++) {
      outputBuf.putLong(finalOffset);
      finalOffset += 8;
    }

    return src;
  }


  /**
   * xx.
   */
  public static void checkDataAssociatedWithAddress(long offset, byte[] src, int length, int delta)
      throws StorageException {
    logger.debug("verifying offset:{} length:{}", offset, length);
    long origOffset = offset;
    if (length % 8 != 0) {
      throw new StorageException(
          "the length has to be multiple times of 8" + " offset:" + offset + "length:" + length);
    }

    int times = length / 8;
    byte[] temp = new byte[8];
    for (int i = 0; i < times; i++) {
      for (int j = 0; j < 8; j++) {
        temp[j] = src[i * 8 + j];
      }

      long returnedValue = ByteUtils.bytesToLong(temp);
      if (returnedValue != offset + delta) {
        logger.warn(
            "Failed to verify offset:{} length:{} the error occur at the offset:{} returned value"
                + " is {}, delta {}",
            origOffset, length, offset, returnedValue, delta);
        StringBuilder builder = new StringBuilder();
        int offsetInArrayToBuildString = i * 8 - 16;
        if (offsetInArrayToBuildString < 0) {
          offsetInArrayToBuildString = 0;
        }

        int len = src.length - offsetInArrayToBuildString;
        len = len < 1024 ? len : 1024;
        Utils.toString(ByteBuffer.wrap(src, offsetInArrayToBuildString, len), builder);
        throw new StorageException("something wrong " + builder.toString());
      }
      offset += 8;
    }
  }

  public StorageDriver getStorage() {
    return coordinator;
  }


  /**
   * xx.
   */
  public Coordinator getCoordinator() {
    if (this.coordinator instanceof Coordinator) {
      return (Coordinator) this.coordinator;
    }

    if (this.coordinator instanceof CoordinatorBaseTest) {
      return ((CoordinatorBaseTest) coordinator).getCoordinator();
    }

    return null;
  }


  /**
   * xx.
   */
  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }


  /**
   * xx.
   */
  public void waitUntilReadDone() {
    logger.debug("stopping read data ");
    if (readLatch != null) {
      try {
        while (true) {
          boolean succ = readLatch.await(1, TimeUnit.SECONDS);
          if (succ) {
            logger.debug("finish await latch");
            break;
          } else {
            if (!getErrorMsg().equals("")) {
              throw new RuntimeException("encounter err no need wait read done: " + getErrorMsg());
            }
            logger.info("read is not done, go on waitting.");
          }
        }
      } catch (InterruptedException e) {
        logger.warn("can not wait for thread stopped");
      }
    }
  }


  /**
   * xx.
   */
  public void stopLoopRandomWrite() {
    if (isLoopWriteStopped) {
      logger.warn("already stopped before !");
      return;
    }
    logger.warn("stopping write data ");
    isLoopWriteStopped = true;
    if (loopWriteLatch != null) {
      try {
        boolean succ = loopWriteLatch.await(600, TimeUnit.SECONDS);
        if (succ) {
          logger.debug("finish await latch");
        } else {
          throw new RuntimeException("timeout for wait write done");
        }
      } catch (InterruptedException e) {
        logger.error("can not wait for thread stoping", e);
      }
    }
  }


  /**
   * xx.
   */
  public void stopLoopRandomReadWrite() {
    logger.debug("reading and writing data is done and the range is {}",
        accessPlanRangeSet.toString());
    isLoopReadWriteStopped = true;
    if (loopReadWriteLatch != null) {
      try {
        loopReadWriteLatch.await();
      } catch (InterruptedException e) {
        logger.warn("can not wait for thread stoping");
      }
    }
    isLoopReadWriteStopped = false;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg(String errorMsg) {
    logger.debug("errorMsg is {}", errorMsg, new Exception());
    this.errorMsg = errorMsg;
  }

  public void setWriteReadPausedTolerance(long mills) {
    this.writeReadPausedTorrence = mills;
  }


  /**
   * xx.
   */
  public String randomWrite(int numWriteThread, long size, final int delta) throws Exception {
    final ConcurrentLinkedQueue<Range<Long>> accessPlan = generateWriteAccessPlan(size);
    final CountDownLatch mainLatch = new CountDownLatch(numWriteThread);
    final CountDownLatch threadLatch = new CountDownLatch(1);
    final long writeTimeBegin = System.currentTimeMillis();
    final AtomicLong totalLen = new AtomicLong(0L);
    final AtomicBoolean hasFailed = new AtomicBoolean(false);
    for (int i = 0; i < numWriteThread; i++) {
      Thread writeThread = new Thread("randomWrite_Thread_" + i) {
        public void run() {
          try {
            threadLatch.await();
            while (true) {
              Range<Long> range = accessPlan.poll();
              if (range == null) {
                logger.debug("one write thread is done");
                return;
              }
              logger.debug("range: {}", range);
              int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
              totalLen.addAndGet(length);
              writeAddressData(range.lowerEndpoint(), length, delta, new AtomicBoolean());
              writeCount.incrementAndGet();
            }
          } catch (Exception e) {
            errorMsg =
                "coordinator:" + coordinator + " random write data failure, Write Total Bytes:"
                    + totalLen.get();
            logger.error(errorMsg, e);
            hasFailed.set(true);
            long count = mainLatch.getCount();
            for (int i = 0; i < count; i++) {
              mainLatch.countDown();
            }
          } finally {
            mainLatch.countDown();
          }
        }
      };
      writeThread.start();
    }

    threadLatch.countDown();
    mainLatch.await();
    long writeTotalLength = totalLen.get();
    long writeDonetime = System.currentTimeMillis();

    logger.debug("writing data is done and the range is {}", accessPlanRangeSet.toString());
    String result =
        "Total bytes: " + writeTotalLength + "Write time: " + (writeDonetime - writeTimeBegin)
            + " rate: " + writeTotalLength / (writeDonetime - writeTimeBegin);
    return result;
  }


  /**
   * xx.
   */
  public void sequentialWrite(long size, final int delta) throws Exception {
    long pageCount = size / pageSize;
    for (int i = 0; i < pageCount; i++) {
      try {
        writeAddressData(i * pageSize, pageSize, delta, new AtomicBoolean());
      } catch (Exception e) {
        errorMsg = "coordinator:" + coordinator + " random write data failure, Write Total Bytes:"
            + i * pageSize;
        logger.error(errorMsg, e);
        throw e;
      }
    }
  }


  /**
   * xx.
   */
  public void startReadThreads(final AtomicBoolean hasFailed, final int delta,
      boolean continuousRead)
      throws Exception {
    if (hasFailed.get()) {
      logger.error("write encountor error!");
      return;
    }
    readLatch = new CountDownLatch(1);
    final AtomicLong totalLen = new AtomicLong(0L);
    Thread readThread = new Thread(() -> {
      logger.warn("start read thread: {}", Thread.currentThread().getId());
      try {
        ConcurrentLinkedQueue<Range<Long>> accessPlan = null;

        long lastReadTime = System.currentTimeMillis();

        accessPlan = generateReadAccessPlan();

        logger.debug("one read plan: {}", accessPlan);
        while (true) {
          Range<Long> range = accessPlan.poll();
          if (range == null) {
            logger.debug("no more ranges to read");
            break;
          }
          int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
          totalLen.addAndGet(length);
          byte[] dst = new byte[length];
          verifyAddressData(range.lowerEndpoint(), length, delta, dst);
          long readPaused = System.currentTimeMillis() - lastReadTime;
          if (readPaused > writeReadPausedTorrence) {
            if (continuousRead) {
              logger.error("read paused too long {}", readPaused);
              errorMsg = "read paused for too long " + readPaused;
              hasFailed.set(true);
            } else {
              logger.warn("read paused too long {}", readPaused);
            }
          } else {
            logger.info("read paused for {}", readPaused);
          }
          lastReadTime = System.currentTimeMillis();
          logger.debug("one read action, offset:{}, length: {}", range.lowerEndpoint(), length);
          Thread.sleep(10);
        }
      } catch (Exception e) {
        errorMsg =
            "coordinator:" + coordinator + " -- read data failure, Write Total Bytes:" + totalLen
                .get();
        logger.error(errorMsg, e);
        hasFailed.set(true);
      } finally {
        readLatch.countDown();
        logger.warn("exit write thread: {}", Thread.currentThread().getId());
      }
    });
    readThread.setName("startReadThreads_Thread");
    readThread.start();
  }


  /**
   * xx.
   */
  public void loopRandomWrite(final long size, final int interval, final AtomicBoolean hasFailed,
      final int delta,
      boolean continuousWrite) throws Exception {
    loopWriteLatch = new CountDownLatch(1);
    final AtomicLong totalLen = new AtomicLong(0L);
    Thread writeThread = new Thread("loopRandomWrite_Thread") {
      public void run() {
        logger.warn("start write thread: {}", Thread.currentThread().getId());
        isLoopWriteStopped = false;
        try {
          ConcurrentLinkedQueue<Range<Long>> accessPlan = null;
          long totalOffset = 0;
          int unit = (int) (size / 16);

          long lastWriteTime = System.currentTimeMillis();
          while (!isLoopWriteStopped && !hasFailed.get()) {
            if (totalOffset + unit > size) {
              logger.warn("write over the volume size {}, again from 0", size);
              totalOffset = 0;
            }

            accessPlan = generateWriteAccessPlan(totalOffset, unit);
            totalOffset += unit;

            logger.debug("one write plan: {}", accessPlan);
            while (true) {
              Range<Long> range = accessPlan.poll();
              if (range == null) {
                logger.debug("no more range to write");
                break;
              }
              int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
              totalLen.addAndGet(length);
              writeAddressData(range.lowerEndpoint(), length, delta, hasFailed);

              writeCount.incrementAndGet();
            }
            Thread.sleep(interval);
          }
        } catch (Exception e) {
          errorMsg = "coordinator:" + coordinator + " -- write data failure, Write Total Bytes:"
              + totalLen.get();
          logger.error(errorMsg, e);
          hasFailed.set(true);
        } finally {
          loopWriteLatch.countDown();
          logger.warn("exit write thread: {}", Thread.currentThread().getId());
        }
      }
    };
    writeThread.start();
    logger.info("wait for write thread running");
    Utils.waitUntilConditionMatches(10, () -> !isLoopWriteStopped);
  }


  /**
   * xx.
   */
  public String randomReadVolume(int numReadThread, final int delta) throws Exception {
    logger.debug("Calling randomReadVolume");
    final ConcurrentLinkedQueue<Range<Long>> accessPlan1 = generateReadAccessPlan();
    final CountDownLatch mainLatch = new CountDownLatch(numReadThread);
    final CountDownLatch threadLatch = new CountDownLatch(1);

    final long beginningTime = System.currentTimeMillis();
    final AtomicLong totalLen = new AtomicLong(0L);
    final AtomicBoolean hasFailed = new AtomicBoolean(false);

    for (int i = 0; i < numReadThread; i++) {
      Thread readThread = new Thread("randomReadVolume_Thread_" + i) {
        public void run() {
          try {
            threadLatch.await();
            while (!hasFailed.get()) {
              Range<Long> range = accessPlan1.poll();
              if (range == null) {
                return;
              }
              int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
              logger.debug("range:{} length:{} ", range, length);
              totalLen.addAndGet(length);
              byte[] dst = new byte[length];
              verifyAddressData(range.lowerEndpoint(), length, delta, dst);
            }
          } catch (Exception e) {
            errorMsg = "coordinator:" + coordinator + " -- read data failure, read Total Bytes:"
                + totalLen.get();
            logger.error(errorMsg, e);
            hasFailed.set(true);
            long count = mainLatch.getCount();
            for (int i = 0; i < count; i++) {
              mainLatch.countDown();
            }
          } finally {
            mainLatch.countDown();
          }
        }
      };
      readThread.start();
    }

    threadLatch.countDown();
    boolean success = mainLatch.await(300, TimeUnit.SECONDS);
    if (!success) {
      logger.error("failed to wait read down");
      throw new Exception();
    }
    long readDonetime = System.currentTimeMillis();
    long readTotalLength = totalLen.get();

    return "Total bytes: " + readTotalLength + "Read time: " + (readDonetime - beginningTime)
        + " rate: "
        + readTotalLength / (readDonetime - beginningTime);
  }

  private ConcurrentLinkedQueue<Range<Long>> generateWriteAccessPlan(long offset, long length)
      throws StorageException {
    int unitLen = pageSize * 10;
    offset -= offset % 8;
    Validate.isTrue(offset >= 0);

    // write first
    Random randomForLen = new Random(System.currentTimeMillis());
    Random randomForPos = new Random(System.currentTimeMillis() + 1000);
    ConcurrentLinkedQueue<Range<Long>> accessPlan = new ConcurrentLinkedQueue<Range<Long>>();
    Range<Long> wholeRange = Range.closed(offset, offset + length - 1);

    long estimateTimes = length / unitLen;
    while (!accessPlanRangeSet.encloses(wholeRange) && estimateTimes > 0) {
      estimateTimes--;
      long tmpOffset = Math.abs(randomForPos.nextLong()) % (length - 1);
      tmpOffset -= tmpOffset % 8;
      Validate.isTrue(tmpOffset >= 0);

      int smallLen = randomForLen.nextInt(unitLen);
      if (tmpOffset + smallLen > length) {
        smallLen = (int) (length - tmpOffset);
      }

      smallLen -= smallLen % 8;
      if (smallLen <= 0) {
        logger.debug("small leng {} : {}", tmpOffset, smallLen);
        continue;
      }

      tmpOffset = tmpOffset + offset;
      Range<Long> range = Range.closedOpen(tmpOffset, tmpOffset + smallLen);
      // add the range to the set
      accessPlanRangeSet.add(range);
      accessPlan.add(range);
    }

    return accessPlan;
  }

  private ConcurrentLinkedQueue<Range<Long>> generateWriteAccessPlan(long length)
      throws StorageException {
    int unitLen = pageSize * 10;

    // write first
    Random randomForLen = new Random(System.currentTimeMillis());
    Random randomForPos = new Random(System.currentTimeMillis() + 1000);
    ConcurrentLinkedQueue<Range<Long>> accessPlan = new ConcurrentLinkedQueue<Range<Long>>();
    Range<Long> wholeRange = Range.closed(0L, length - 1);

    long estimateTimes = length / unitLen;
    while (!accessPlanRangeSet.encloses(wholeRange) && estimateTimes > 0) {
      estimateTimes--;
      long offset = Math.abs(randomForPos.nextLong()) % (length - 1);
      offset -= offset % 8;
      Validate.isTrue(offset >= 0);

      int smallLen = randomForLen.nextInt(unitLen);
      if (offset + smallLen > length) {
        smallLen = (int) (length - offset);
      }

      smallLen -= smallLen % 8;

      if (smallLen <= 0) {
        logger.debug("small leng {} : {}", offset, smallLen);
        continue;
      }
      Range<Long> range = Range.closedOpen(offset, offset + smallLen);
      // add the range to the set
      accessPlanRangeSet.add(range);
      accessPlan.add(range);
    }

    return accessPlan;
  }

  private ConcurrentLinkedQueue<Range<Long>> generateReadAccessPlan() throws StorageException {
    int unitLen = pageSize * 10;
    // write first
    Random randomForLen = new Random(System.currentTimeMillis());
    ConcurrentLinkedQueue<Range<Long>> accessPlan = new ConcurrentLinkedQueue<Range<Long>>();
    Set<Range<Long>> ranges = accessPlanRangeSet.asRanges();

    Iterator<Range<Long>> iterator = ranges.iterator();
    while (iterator.hasNext()) {
      Range<Long> range = iterator.next();

      long offset = range.lowerEndpoint();
      int length = (int) (range.upperEndpoint() - range.lowerEndpoint());

      while (length > 0) {
        int smallLen = randomForLen.nextInt(unitLen);

        if (smallLen > length) {
          smallLen = length;
        }

        smallLen -= smallLen % 8;
        if (smallLen <= 0) {
          break;
        }

        // add the range to the set
        accessPlan.add(Range.closedOpen(offset, offset + smallLen));
        offset += smallLen;
        length -= smallLen;
      }
    }

    return accessPlan;
  }


  /**
   * xx.
   */
  public void writeAddressData(long pos, int length, int delta, AtomicBoolean hasFailed)
      throws Exception {
    byte[] src = generateDataAssociatedWithAddress(pos, length, delta);
    long startTime = System.currentTimeMillis();
    logger.warn("start write offset: {}, length: {}", pos, length);
    coordinator.asyncWrite(pos, src, 0, length, new AsyncIoCallBackImpl(iodepthSemaphore) {
      @Override
      public void ioRequestDone(Long volumeId, boolean result, ByteBuf byteBuf) {
        super.ioRequestDone(volumeId, result, byteBuf);
        long endTime = System.currentTimeMillis();
        long writePaused = endTime - startTime;
        logger.warn("it cost write time: {}, offset: {}, length: {}", writePaused, pos, length);
        if (writePaused > writeReadPausedTorrence) {
          errorMsg = "write paused for too long " + writePaused;
          logger.error(errorMsg);
          hasFailed.set(true);
        } else {
          logger.info("write paused for {}", writePaused);
        }
      }
    });

  }


  /**
   * xx.
   */
  public void verifyAddressData(long pos, int len, int delta, byte[] dst) throws Exception {
    long startTime = System.currentTimeMillis();
    logger.warn("start read offset:{}, length:{}", pos, len);
    try {
      coordinator.asyncRead(pos, dst, 0, len, new AsyncIoCallBackImpl(iodepthSemaphore) {
        @Override
        public void ioRequestDone(Long volumeId, boolean result, ByteBuf byteBuf) {
          super.ioRequestDone(volumeId, result, byteBuf);
          long endTime = System.currentTimeMillis();
          long readPaused = endTime - startTime;
          if (readPaused > writeReadPausedTorrence) {
            logger.error("read paused too long:{}, offset:{}, length:{}", readPaused, pos, len);
            errorMsg = "read paused for too long " + readPaused;
          } else {
            logger.warn("it cost read time:{}, offset:{}, length:{}", readPaused, pos, len);
          }
          try {
            checkDataAssociatedWithAddress(pos, dst, len, delta);
          } catch (StorageException e) {
            logger.error("", e);
            errorMsg = e.getMessage();
          }
        }

      });

    } catch (Exception e) {
      logger.error("caught an exception when sync write", e);
      throw e;
    }

  }

  public long getWriteCount() {
    return writeCount.get();
  }


  /**
   * xx.
   */
  public void close() {
    try {
      this.coordinator.close();
    } catch (Exception e) {
      logger.error("fail to close coordinator.");
    }
  }

}
