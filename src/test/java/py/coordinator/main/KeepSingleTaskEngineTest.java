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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.Semaphore;
import org.junit.Test;
import py.archive.segment.SegId;
import py.common.RequestIdBuilder;
import py.coordinator.task.GetMembershipTask;
import py.coordinator.task.SingleTask;
import py.coordinator.worker.BaseKeepSingleTaskEngine;
import py.test.TestBase;
import py.volume.VolumeId;

public class KeepSingleTaskEngineTest extends TestBase {

  private static Long requestId = RequestIdBuilder.get();
  private static Semaphore testSemaphore = new Semaphore(3);

  @Test
  public void testKeepSingleTaskEngine() throws InterruptedException {
    TestKeepSingleTaskEngine testEngine = new TestKeepSingleTaskEngine(
        TestKeepSingleTaskEngine.class.getSimpleName());
    testEngine.start();
    VolumeId volumeId = new VolumeId(RequestIdBuilder.get());
    int segIndex = 0;
    SegId segId1 = new SegId(volumeId, segIndex++);
    GetMembershipTask task1 = new GetMembershipTask(volumeId.getId(), segId1, requestId, null);
    SegId segId2 = new SegId(volumeId, segIndex++);
    final GetMembershipTask task2 = new GetMembershipTask(volumeId.getId(), segId2, requestId,
        null);
    SegId segId3 = new SegId(volumeId, segIndex++);
    final GetMembershipTask task3 = new GetMembershipTask(volumeId.getId(), segId3, requestId,
        null);

    testSemaphore.acquire(3);

    assertTrue(testEngine.putTask(task1));
    assertEquals(1, testEngine.getTaskCount());
    assertTrue(testEngine.putTask(task2));
    assertEquals(2, testEngine.getTaskCount());
    assertTrue(testEngine.putTask(task3));
    assertEquals(3, testEngine.getTaskCount());

    for (int i = 0; i < 100; i++) {
      assertFalse(testEngine.putTask(task1));
      assertFalse(testEngine.putTask(task2));
      assertFalse(testEngine.putTask(task3));
      assertEquals(3, testEngine.getTaskCount());
    }

    testSemaphore.release(3);
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      fail();
    }

    assertEquals(0, testEngine.getTaskCount());

    testSemaphore.acquire(3);
    // do it again
    assertTrue(testEngine.putTask(task1));
    assertEquals(1, testEngine.getTaskCount());
    assertTrue(testEngine.putTask(task2));
    assertEquals(2, testEngine.getTaskCount());
    assertTrue(testEngine.putTask(task3));
    assertEquals(3, testEngine.getTaskCount());

    for (int i = 0; i < 100; i++) {
      assertFalse(testEngine.putTask(task1));
      assertFalse(testEngine.putTask(task2));
      assertFalse(testEngine.putTask(task3));
      assertEquals(3, testEngine.getTaskCount());
    }

    testSemaphore.release(2);
    testEngine.stop();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      fail();
    }

    assertEquals(0, testEngine.getTaskCount());
    // releaseBody all semaphore after test
    testSemaphore.release(1);

  }

  class TestKeepSingleTaskEngine extends BaseKeepSingleTaskEngine {

    public TestKeepSingleTaskEngine(String threadName) {
      super(threadName);
    }

    @Override
    public void process(List<SingleTask> singleTasks) {
      for (SingleTask singleTask : singleTasks) {
        try {
          testSemaphore.acquire();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } finally {
          freeTask(singleTask);
          testSemaphore.release();
        }
      }
    }
  } // end class

}
