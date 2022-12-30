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

package py.integ.coordinator;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import py.common.RequestIdBuilder;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.PydNormalRequestHeader;
import py.coordinator.nbd.request.Request;
import py.coordinator.nbd.request.RequestHeader;
import py.exception.StorageException;

/**
 * xx.
 */
public class CoordinatorRandomReadWriteTest extends CoordinatorReadWriteTest {

  private RangeSet<Long> accessPlanRangeSet = TreeRangeSet.create();
  private ConcurrentLinkedQueue<List<Request>> directReadRequestPlan =
      new ConcurrentLinkedQueue<List<Request>>();

  @Test
  public void testRandomReadWriteVolumeMultipleTimes() throws Exception {
    for (int i = 0; i < 10; i++) {
      testRandomReadWriteVolume();
      // testRandomWriteVolume();
      // testRandomReadVolume();
    }
  }

  @Test
  public void testRandomMixedReadWriteVolume() throws Exception {
    logger.info("Start to write");
    String writeResult = randomWrite(15, volumeSize);
    logger.info("Write completed and its write result is " + writeResult);
    logger.info(" Start to execute mixed read and write");
    String mixedResult = randomMixedReadWriteVolume(15);
    logger.info("Mixed read/write completed and its result " + mixedResult);
  }

  @Test
  public void testRandomReadWriteVolume() throws Exception {
    logger.info("Start to write");
    String writeResult = randomWrite(15, volumeSize);
    logger.info("Write completed and its write result is " + writeResult);
    logger.info(" Start to execute reads");
    String readResult = randomReadVolume(15);
    logger.info("Read completed and its result " + readResult);
  }

  @Test
  public void testRandomWriteVolume() throws Exception {
    logger.info("Start to write");
    String writeResult = randomWrite(15, volumeSize);
    logger.info("Write completed and its write result is " + writeResult);

    /* testRandomReadVolume **/
    logger.info(" Start to execute reads");
    String readResult = randomReadVolume(15);
    logger.info("Read completed and its result " + readResult);

  }

  /**
   * set it to testRandomWriteVolume test. public void testRandomReadVolume() throws Exception {
   * logger.info(" Start to execute reads"); String readResult = randomReadVolume(15);
   * logger.info("Read completed and its result " + readResult); }
   */

  private String randomWrite(int numWriteThread, long size) throws Exception {
    final ConcurrentLinkedQueue<Range<Long>> accessPlan = generateWriteAccessPlan(size);
    final CountDownLatch mainLatch = new CountDownLatch(numWriteThread);
    final CountDownLatch threadLatch = new CountDownLatch(1);
    final long writeTimeBegin = System.currentTimeMillis();
    final AtomicLong totalLen = new AtomicLong(0L);
    final AtomicLong totalRequestSent = new AtomicLong(0L);
    final AtomicBoolean hasFailed = new AtomicBoolean(false);
    for (int i = 0; i < numWriteThread; i++) {
      Thread writeThread = new Thread("RandomWrite" + i) {
        @Override
        public void run() {
          try {
            threadLatch.await();
            while (true) {
              Range<Long> range = accessPlan.poll();
              if (range == null) {
                logger.info("one write thread is done");
                return;
              }
              logger.debug("range:" + range);
              int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
              logger.warn("the length is :{} lowerEndpoint is :{}", length, range.lowerEndpoint());
              CoordinatorRandomReadWriteTest
                  .writeAddressData(storage, range.lowerEndpoint(), length);
              logger.debug("sent {} requests, and size of data sent {} ",
                  totalRequestSent.incrementAndGet(), totalLen.addAndGet(length));
            }
          } catch (Exception e) {
            logger.error("Get an exception when random write", e);
            hasFailed.set(true);
            long count = mainLatch.getCount();
            for (int i = 0; i < count; i++) {
              mainLatch.countDown();
            }
            System.exit(1);
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

    logger.debug("writing data is done and the range is " + accessPlanRangeSet.toString());

    if (hasFailed.get()) {
      fail("writing failed");
    }

    String result =
        "Total bytes: " + writeTotalLength + "Write time: " + (writeDonetime - writeTimeBegin)
            + " rate: "
            + writeTotalLength / (writeDonetime - writeTimeBegin);
    return result;
  }

  private String randomReadVolume(int numReadThread) throws Exception {
    final ConcurrentLinkedQueue<Range<Long>> accessPlan1 = generateReadAccessPlan();
    final CountDownLatch mainLatch = new CountDownLatch(numReadThread);
    final CountDownLatch threadLatch = new CountDownLatch(1);

    final long beginningTime = System.currentTimeMillis();
    final AtomicLong totalLen = new AtomicLong(0L);
    final AtomicBoolean hasFailed = new AtomicBoolean(false);

    for (int i = 0; i < numReadThread; i++) {
      Thread readThread = new Thread("RandomRead" + i) {
        @Override
        public void run() {
          try {
            threadLatch.await();
            while (true) {
              Range<Long> range = accessPlan1.poll();
              if (range == null) {
                logger.info("one read thread is done");
                return;
              }
              int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
              logger.debug("range:" + range + " length: " + length);
              totalLen.addAndGet(length);
              byte[] dst = new byte[length];
              CoordinatorReadWriteTest
                  .verifyAddressData(storage, range.lowerEndpoint(), length, dst);
            }
          } catch (Exception e) {
            logger.error("caught an exception", e);
            hasFailed.set(true);
            long count = mainLatch.getCount();
            for (int i = 0; i < count; i++) {
              mainLatch.countDown();
            }
            System.exit(1);
          } finally {
            mainLatch.countDown();
          }
        }
      };
      readThread.start();
    }
    threadLatch.countDown();
    mainLatch.await();
    if (hasFailed.get()) {
      fail("reading failed");
    }
    long readDonetime = System.currentTimeMillis();
    long readTotalLength = totalLen.get();

    return "Total bytes: " + readTotalLength + "Read time: " + (readDonetime - beginningTime)
        + " rate: "
        + readTotalLength / (readDonetime - beginningTime);
  }

  private String randomMixedReadWriteVolume(int numReadWriteThread) throws Exception {
    final ConcurrentLinkedQueue<Range<Long>> accessPlan = generateReadAccessPlan();
    final CountDownLatch mainLatch = new CountDownLatch(numReadWriteThread);
    final CountDownLatch threadLatch = new CountDownLatch(1);
    final long beginningTime = System.currentTimeMillis();
    final AtomicLong totalLen = new AtomicLong(0L);
    final AtomicBoolean hasFailed = new AtomicBoolean(false);

    for (int i = 0; i < numReadWriteThread; i++) {
      Thread readWriteThread = new Thread("MixReadWrite" + i) {
        @Override
        public void run() {
          try {
            threadLatch.await();
            Random randomForReadOrWrite = new Random(System.currentTimeMillis());

            while (true) {
              Range<Long> range = accessPlan.poll();
              if (range == null) {
                logger.info("one thread that executes mixed read/write is done");
                return;
              }

              int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
              logger.debug("range:" + range + " length: " + length);
              if (randomForReadOrWrite.nextBoolean()) {
                // read
                byte[] dst = new byte[length];
                CoordinatorReadWriteTest
                    .verifyAddressData(storage, range.lowerEndpoint(), length, dst);
              } else {
                CoordinatorReadWriteTest.writeAddressData(storage, range.lowerEndpoint(), length);
              }
              totalLen.addAndGet(length);
            }
          } catch (Exception e) {
            logger.error("caught an exception", e);
            hasFailed.set(true);
            long count = mainLatch.getCount();
            for (int i = 0; i < count; i++) {
              mainLatch.countDown();
            }
            System.exit(1);
          } finally {
            mainLatch.countDown();
          }
        }
      };
      readWriteThread.start();
    }
    threadLatch.countDown();
    mainLatch.await();
    if (hasFailed.get()) {
      fail("mixed read/write failed");
    }
    long readDonetime = System.currentTimeMillis();
    return "Total bytes: " + totalLen.get() + "Read time: " + (readDonetime - beginningTime)
        + " rate: "
        + totalLen.get() / (readDonetime - beginningTime) + "\n";
  }

  private ConcurrentLinkedQueue<Range<Long>> generateWriteAccessPlan(long length) {
    int unitLen = cfg.getPageSize() * 10;

    // write first
    Random randomForLen = new Random(System.currentTimeMillis());
    Random randomForPos = new Random(System.currentTimeMillis() + 1000);
    ConcurrentLinkedQueue<Range<Long>> accessPlan = new ConcurrentLinkedQueue<Range<Long>>();
    Range<Long> wholeRange = Range.closed(0L, length - 1);

    long estimateTimes = length / unitLen;
    while (!accessPlanRangeSet.encloses(wholeRange) && estimateTimes > 0) {
      estimateTimes--;
      long offset = Math.abs(randomForPos.nextLong()) % (length - 1);
      offset -= offset % smallLength;
      assertTrue(offset >= 0);

      int smallLen = randomForLen.nextInt(unitLen) + (int) smallLength;
      if (offset + smallLen > length) {
        smallLen = (int) (length - offset);
      }

      /* old */
      //smallLen -= smallLen % 8;
      /* smallLen >= 512 */
      smallLen -= smallLen % smallLength;

      if (smallLen <= 0) {
        logger.warn("small leng" + offset + " : " + smallLen);
        continue;
      }
      logger.warn("get the small leng" + offset + " : " + smallLen);
      Range<Long> range = Range.closedOpen(offset, offset + smallLen);
      // add the range to the set
      accessPlanRangeSet.add(range);
      accessPlan.add(range);
      logger.debug("estimeateTimes is " + estimateTimes);
    }

    logger.warn("accessPlanRangeSet size is " + accessPlanRangeSet.asRanges().size());
    return accessPlan;
  }

  private ConcurrentLinkedQueue<Range<Long>> generateReadAccessPlan() throws StorageException {
    int unitLen = cfg.getPageSize() * 10;
    // write first
    Random randomForLen = new Random(System.currentTimeMillis());
    ConcurrentLinkedQueue<Range<Long>> accessPlan = new ConcurrentLinkedQueue<Range<Long>>();
    Set<Range<Long>> ranges = accessPlanRangeSet.asRanges();
    logger.warn(" the ranges size : {} ", ranges.size());
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

        /* small length 512*/
        //smallLen -= smallLen % 8;
        smallLen -= smallLen % smallLength;
        if (smallLen <= 0) {
          break;
        }

        logger.warn(" generateReadAccessPlan the smallLen is: {} ", smallLen);
        // add the range to the set
        accessPlan.add(Range.closedOpen(offset, offset + smallLen));
        offset += smallLen;
        length -= smallLen;
      }
    }

    return accessPlan;
  }

  private String directReadVolumeWithReadRequestList(int numReadThread) throws Exception {
    final CountDownLatch mainLatch = new CountDownLatch(numReadThread);
    final CountDownLatch threadLatch = new CountDownLatch(1);

    final long beginningTime = System.currentTimeMillis();
    final AtomicLong totalLen = new AtomicLong(0L);
    final AtomicBoolean hasFailed = new AtomicBoolean(false);

    for (int i = 0; i < numReadThread; i++) {
      Thread readThread = new Thread("directlyRead" + i) {
        @Override
        public void run() {
          try {
            threadLatch.await();
            while (true) {
              List<Request> readRequestList = directReadRequestPlan.poll();
              if (readRequestList == null) {
                logger.info("one read thread " + Thread.currentThread().getName() + " is done");
                return;
              }
              long readSize = CoordinatorReadWriteTest
                  .readDataWithRequestListDirectly(storage, readRequestList);
              logger.debug("read " + readRequestList.size() + " requests to data node");
              totalLen.addAndGet(readSize);
            }
          } catch (Exception e) {
            logger.error("caught an exception", e);
            hasFailed.set(true);
            long count = mainLatch.getCount();
            for (int i = 0; i < count; i++) {
              mainLatch.countDown();
            }
            System.exit(1);
          } finally {
            mainLatch.countDown();
          }
        }
      };
      readThread.start();
    }
    threadLatch.countDown();
    mainLatch.await();
    if (hasFailed.get()) {
      fail("reading failed");
    }
    long readDonetime = System.currentTimeMillis();
    long readTotalLength = totalLen.get();

    return "Total bytes: " + readTotalLength + "Read time: " + (readDonetime - beginningTime)
        + " rate: "
        + readTotalLength / (readDonetime - beginningTime);
  }

  private ConcurrentLinkedQueue<List<Request>> generateWriteRequestListPlan(long size) {
    long segmentSize = cfg.getSegmentSize();
    int pageSize = cfg.getPageSize();
    ConcurrentLinkedQueue<Range<Long>> writePlan = this.generateWriteAccessPlan(size);
    ConcurrentHashMap<Long, List<Request>> writeMap = new ConcurrentHashMap<Long, List<Request>>();
    ConcurrentLinkedQueue<List<Request>> writeListQueue =
        new ConcurrentLinkedQueue<List<Request>>();
    // one range is write request list, range length is less than pagesize * 10;
    Random randomPageNumber = new Random(System.currentTimeMillis());
    Random randomSpaceBetweenTwoRequest = new Random(System.currentTimeMillis() + 100);

    Range<Long> range = writePlan.poll();
    while (range != null) {
      logger.debug("range is " + range);
      long offset = range.lowerEndpoint();
      long end = range.upperEndpoint();

      // Random produce request while length not larger than 2 pages for the range length is not
      // than 10 page
      // size;
      while (offset < end) {
        int requestLength = randomPageNumber.nextInt(3) * pageSize;
        if (requestLength == 0) { // create an request not than one page;
          logger.debug("request is not than one page");
          requestLength = randomPageNumber.nextInt(pageSize / 2) + 1;
        }

        // make sure length can be divided by 8
        requestLength -= requestLength % 8;
        if (requestLength < 0) {
          requestLength = 16;
        }

        if (offset + requestLength > end) {
          requestLength = (int) (end - offset);
        }

        // make sure the request is in one segment
        if (offset / segmentSize != (offset + requestLength - 1) / segmentSize) {
          requestLength = (int) ((offset / segmentSize + 1) * segmentSize - offset);
        }

        // make sure the length not 0;
        if (requestLength == 0) {
          continue;
        }

        RequestHeader header = new PydNormalRequestHeader(NbdRequestType.Write,
            RequestIdBuilder.get(), offset,
            requestLength, 1);
        Request request = new Request(header);

        logger.debug("get an request" + request);

        long segIndex = offset / segmentSize;
        List<Request> unitInSeg = writeMap.get(segIndex);
        if (null == unitInSeg) {
          unitInSeg = new ArrayList<Request>();
          writeMap.put(segIndex, unitInSeg);
        }
        unitInSeg.add(request);

        // reach the number request of random, put the list into the delay queue;
        int requestNum = (5 + randomPageNumber.nextInt(25));
        if (unitInSeg.size() > requestNum) {
          writeListQueue.add(unitInSeg);
          writeMap.remove(segIndex);
        }

        offset += requestLength;
        // make a hole between two requests
        offset += randomSpaceBetweenTwoRequest.nextInt(100);

        // make offset can be divided by 8
        offset -= offset % 8;
      }
      range = writePlan.poll();
    }

    for (Entry<Long, List<Request>> entry : writeMap.entrySet()) {
      writeListQueue.add(entry.getValue());
    }

    return writeListQueue;
  }
}