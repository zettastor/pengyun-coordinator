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

package py.coordinator.main;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.lib.Coordinator;
import py.coordinator.logmanager.DelayManagerImpl;
import py.coordinator.logmanager.IoContextManager;
import py.coordinator.logmanager.ReadIoContextManager;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.task.ResendRequest;

/**
 * xx.
 */
public class LogResenderTest {

  private static final Logger logger = LoggerFactory.getLogger(LogResenderTest.class);
  private Coordinator coordinator = mock(Coordinator.class);
  private LogResenderForTest logResender;
  private AtomicInteger resendWriteRequestCount;
  private AtomicInteger resendReadRequestCount;
  @Mock
  private IoContextManager readManager;
  @Mock
  private IoContextManager writeManager;


  /**
   * xx.
   */
  public LogResenderTest() {
    logResender = new LogResenderForTest(coordinator);
    resendWriteRequestCount = new AtomicInteger(0);
    resendReadRequestCount = new AtomicInteger(0);
  }

  @Test
  public void testIncBigNumber() {
    long oneRequest = 4096 * 128 * 1024 * 1024L;
    long maxValue = Long.MAX_VALUE;
    long minValue = Long.MIN_VALUE;

    long modCount = maxValue / oneRequest;
    long nextValue = maxValue + oneRequest;
    long dayCost = modCount / 24 / 60 / 60;
    logger.warn("finally inc count:{}", modCount);
    logger.warn("");
  }

  @Test
  public void testProcessRequest() throws InterruptedException {
    writeManager = mock(WriteIoContextManager.class);
    readManager = mock(ReadIoContextManager.class);
    int returnValue = 1;
    when(readManager.incFailTimes()).thenReturn(returnValue);
    when(writeManager.incFailTimes()).thenReturn(returnValue);

    // ResendRequest resendRequest1 = new ResendRequest(readManager, false);
    // Long delayTime = ResendRequest.RESEND_LOG_TIME_DELAY_UNIT * returnValue;
    // assertTrue(resendRequest1.getDelay() == delayTime);
    // Long newDelay = 1000L; // ms
    // Long littleTime = 50L; // ms
    // resendRequest1.updateDelayWithForce(newDelay);
    // logResender.put(resendRequest1);
    // assertTrue(resendReadRequestCount.get() == 0);
    // Thread.sleep(newDelay + littleTime);
    // assertTrue(resendReadRequestCount.get() == 1);
    //
    // returnValue = 3;
    // when(readManager.incAndGetResendTimes()).thenReturn(returnValue);
    // ResendRequest resendRequest2 = new ResendRequest(readManager, false);
    // delayTime = ResendRequest.RESEND_LOG_TIME_DELAY_UNIT * returnValue;
    // assertTrue(resendRequest2.getDelay() == delayTime);
    // assertTrue(resendReadRequestCount.get() == 1);
    // logResender.put(resendRequest2);
    // Thread.sleep(delayTime + littleTime);
    // assertTrue(resendReadRequestCount.get() == 2);
    //
    // returnValue = 1;
    // when(writeManager.incAndGetResendTimes()).thenReturn(returnValue);
    // delayTime = ResendRequest.RESEND_LOG_TIME_DELAY_UNIT * returnValue;
    // ResendRequest resendRequest3 = new ResendRequest(writeManager, false);
    // assertTrue(resendRequest3.getDelay() == delayTime);
    //
    // resendRequest3.updateDelayWithForce(newDelay);
    // logResender.put(resendRequest3);
    // assertTrue(resendReadRequestCount.get() == 2);
    // assertTrue(resendWriteRequestCount.get() == 0);
    // Thread.sleep(newDelay + littleTime);
    // assertTrue(resendReadRequestCount.get() == 2);
    // assertTrue(resendWriteRequestCount.get() == 1);
    //
    // returnValue = 4;
    // when(writeManager.incAndGetResendTimes()).thenReturn(returnValue);
    // ResendRequest resendRequest4 = new ResendRequest(writeManager, false);
    // delayTime = ResendRequest.RESEND_LOG_TIME_DELAY_UNIT * returnValue;
    // assertTrue(resendRequest4.getDelay() == delayTime);
    // assertTrue(resendReadRequestCount.get() == 2);
    // assertTrue(resendWriteRequestCount.get() == 1);
    // logResender.put(resendRequest4);
    // Thread.sleep(delayTime + littleTime);
    // assertTrue(resendReadRequestCount.get() == 2);
    // assertTrue(resendWriteRequestCount.get() == 2);

    // returnValue = (int) (ResendRequest.MAX_RESEND_LOG_TIME_DELAY / ResendRequest
    // .RESEND_LOG_TIME_DELAY_UNIT);
    // when(writeManager.incAndGetResendTimes()).thenReturn(returnValue);
    // ResendRequest resendRequest5 = new ResendRequest(writeManager, false);
    // delayTime = ResendRequest.RESEND_LOG_TIME_DELAY_UNIT * returnValue;
    // assertTrue(resendRequest5.getDelay() == delayTime);
    // assertTrue(resendReadRequestCount.get() == 2);
    // assertTrue(resendWriteRequestCount.get() == 2);
    // logResender.put(resendRequest5);
    // Thread.sleep(delayTime + littleTime);
    // assertTrue(resendReadRequestCount.get() == 2);
    // assertTrue(resendWriteRequestCount.get() == 3);
  }

  private class LogResenderForTest extends DelayManagerImpl {

    public LogResenderForTest(Coordinator coordinator) {
      super(coordinator, null);
    }

    @Override
    public void processResendRequest(ResendRequest resendRequest) {
      IoContextManager manager = resendRequest.getIoContextManager();
      if (manager instanceof ReadIoContextManager) {
        logger.info("receive a read request");
        resendReadRequestCount.incrementAndGet();
      } else if (manager instanceof WriteIoContextManager) {
        resendWriteRequestCount.incrementAndGet();
        logger.info("receive a write request");
      } else {
        logger.error("unknow request type: {}", resendRequest);
      }
    }
  }
}
