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

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.lib.StorageDriver;

/**
 * xx.
 */
public class LoopStorageVerify extends AbstractStorageVerify {

  private static final Logger logger = LoggerFactory.getLogger(LoopStorageVerify.class);
  private static final Random random = new Random();

  private final StorageDriver storage;
  private final int verifyThreadCount;
  private final int writeThreadCount;
  private final int stepSize;
  private final int maxIoSize;
  private final AtomicBoolean stop;
  private int interval;
  private RangeSet<Long> accessPlanRangeSet;
  private AtomicInteger exceptionCounter;
  private CountDownLatch writeLatch;

  public LoopStorageVerify(StorageDriver storage, int writeThreadCount, int verifyThreadCount,
      int stepSize, int maxIoSize) {
    this(storage, writeThreadCount, verifyThreadCount, stepSize, maxIoSize, random.nextInt(256));
  }


  /**
   * xx.
   */
  public LoopStorageVerify(StorageDriver storage, int writeThreadCount, int verifyThreadCount,
      int stepSize, int maxIoSize,
      int delta) {
    super(delta);
    this.writeThreadCount = writeThreadCount;
    this.storage = storage;
    this.verifyThreadCount = verifyThreadCount;
    this.stepSize = stepSize;
    this.maxIoSize = maxIoSize;
    this.stop = new AtomicBoolean(true);
    this.accessPlanRangeSet = TreeRangeSet.create();
    this.exceptionCounter = new AtomicInteger();
  }

  public int getInterval() {
    return interval;
  }

  public void setInterval(int interval) {
    this.interval = interval;
  }

  @Override
  public void startWrite() {
    Validate.isTrue(stop.get());
    stop.set(false);
    exceptionCounter.set(0);
    writeLatch = new CountDownLatch(writeThreadCount);
    for (int i = 0; i < writeThreadCount; i++) {
      Thread thread = new Thread(new WriteData(writeLatch));
      thread.start();
    }
  }

  @Override
  public void stopWrite() {
    if (stop.get()) {
      logger.info("has been stopped");
    }

    stop.set(true);
    try {
      writeLatch.await();
    } catch (InterruptedException e) {
      logger.error("caught exception", e);
    }
    logger.info("write plans: {}", accessPlanRangeSet);
  }


  /**
   * xx.
   */
  public synchronized Queue<Range<Long>> generateWritePlan(long offset, int length) {
    // write first
    Random randomForLen = new Random(System.currentTimeMillis());
    Random randomForPos = new Random(System.currentTimeMillis() + 1000);
    Queue<Range<Long>> accessPlan = new ConcurrentLinkedQueue<Range<Long>>();

    Range<Long> wholeRange = Range.closed(offset, offset + length - 1);
    if (length == 0 || accessPlanRangeSet.encloses(wholeRange)) {
      logger.info("can not generate range: {}:{}", offset, length);
      return accessPlan;
    }

    int processLength = length;
    while (processLength > 0) {
      int currentUnit = Math.min(maxIoSize, processLength);
      long tmpOffset = alignPosition(randomForPos.nextInt(currentUnit));

      int tmpLength = randomForLen.nextInt(currentUnit);
      tmpLength = alginPosition(tmpLength);
      tmpLength = tmpLength > 0 ? tmpLength : 8;

      if (tmpOffset + tmpLength > currentUnit) {
        if (tmpOffset >= tmpLength) {
          tmpOffset = currentUnit - tmpOffset;
        } else {
          tmpLength = currentUnit - tmpLength;
        }
      }

      Validate.isTrue(tmpOffset + tmpLength <= maxIoSize);
      Range<Long> range = Range.closedOpen(offset + tmpOffset, offset + tmpOffset + tmpLength);

      accessPlanRangeSet.add(range);
      accessPlan.add(range);
      processLength -= currentUnit;
      offset += currentUnit;
    }

    return accessPlan;
  }

  @Override
  public void verify() {
    Queue<Range<Long>> accessPlans = generateReadPlan();
    exceptionCounter.set(0);
    logger.debug("verify plans: {}", accessPlans);
    CountDownLatch latch = new CountDownLatch(verifyThreadCount);
    for (int i = 0; i < verifyThreadCount; i++) {
      Thread thread = new Thread(new VerifyData(accessPlans, latch));
      thread.start();
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("can not wait");
    }

  }

  private Queue<Range<Long>> generateReadPlan() {
    // write first
    ConcurrentLinkedQueue<Range<Long>> accessPlan = new ConcurrentLinkedQueue<Range<Long>>();
    Set<Range<Long>> ranges = accessPlanRangeSet.asRanges();

    Iterator<Range<Long>> iterator = ranges.iterator();
    while (iterator.hasNext()) {
      Range<Long> range = iterator.next();

      long offset = range.lowerEndpoint();
      int length = (int) (range.upperEndpoint() - range.lowerEndpoint());

      while (length > 0) {
        int smallLen = Math.min(maxIoSize, length);
        accessPlan.add(Range.closedOpen(offset, offset + smallLen));
        offset += smallLen;
        length -= smallLen;
      }
    }

    return accessPlan;
  }

  @Override
  public int getErrorCount() {
    return exceptionCounter.get();
  }

  public class WriteData implements Runnable {

    private CountDownLatch latch;

    public WriteData(CountDownLatch latch) {
      this.latch = latch;
    }


    /**
     * xx.
     */
    public void run() {
      logger.info("start write thread: {}", Thread.currentThread().getId());
      try {
        long totalOffset = 0;
        long storageSize = storage.size();
        while (!stop.get()) {
          if (totalOffset + stepSize > storageSize) {
            logger.warn("all storage has been written, now again");
            totalOffset = 0;
          }

          Queue<Range<Long>> accessPlan = generateWritePlan(totalOffset, stepSize);
          totalOffset += stepSize;

          logger.debug("one write plan: {}", accessPlan);
          while (true) {
            Range<Long> range = accessPlan.poll();
            if (range == null) {
              logger.debug("no more range to write");
              break;
            }

            logger.debug("write range: {}", range);
            int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
            byte[] data = generateData(range.lowerEndpoint(), length);
            try {
              storage.write(range.lowerEndpoint(), ByteBuffer.wrap(data));
            } catch (Exception e) {
              exceptionCounter.incrementAndGet();
              stop.set(true);
              logger.error("write range:{} failue", range, e);
              break;
            }

            Thread.sleep(interval);
          }
        }
      } catch (Exception e) {
        logger.error("caught an exception", e);
      } finally {
        logger.info("exit write thread: {}", Thread.currentThread().getId());
        latch.countDown();
      }
    }
  }

  public class VerifyData implements Runnable {

    private final Queue<Range<Long>> accessPlans;
    private final CountDownLatch latch;

    private VerifyData(Queue<Range<Long>> accessPlans, CountDownLatch latch) {
      this.accessPlans = accessPlans;
      this.latch = latch;
    }


    /**
     * xx.
     */
    public void run() {
      logger.info("start verify thread: {}", Thread.currentThread().getId());
      try {
        while (true) {
          Range<Long> range = accessPlans.poll();
          if (range == null) {
            return;
          }

          logger.debug("verify range: {}", range);
          int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
          byte[] data = new byte[length];
          try {
            storage.read(range.lowerEndpoint(), ByteBuffer.wrap(data));
            verifyData(range.lowerEndpoint(), data);
          } catch (Exception e) {
            exceptionCounter.incrementAndGet();
            logger.error("range: {} is corrupted", range, e);
          }
        }
      } catch (Exception e) {
        logger.info("caught an exception", e);
      } finally {
        logger.info("exit verify thread: {}", Thread.currentThread().getId());
        latch.countDown();
      }
    }
  }
}
