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

package py.coordinator.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.task.BogusSingleTask;
import py.coordinator.task.SingleTask;


public abstract class BaseKeepSingleTaskEngine implements KeepSingleTaskEngine {

  private static final Logger logger = LoggerFactory.getLogger(BaseKeepSingleTaskEngine.class);

  private static final int NOT_WORKING = 0;
  private static final int WORKING = 1;
  private final Thread workThread;
  private final BlockingQueue<SingleTask> taskQueue;
  private final Map<SingleTask, AtomicInteger> keepSingleMap;
  private boolean stop;



  public BaseKeepSingleTaskEngine(String threadName) {
    this.stop = false;
    this.taskQueue = new LinkedBlockingQueue<>();
    this.keepSingleMap = new ConcurrentHashMap<>();
    this.workThread = new Thread(threadName) {
      @Override
      public void run() {
        try {
          startJob();
        } catch (Throwable t) {
          logger.error("{} thread can not deal with task", threadName, t);
        }
      }
    };
  }

  private void startJob() {
    List<SingleTask> tasks = new ArrayList<>();
    while (!this.stop) {
      tasks.clear();
      // pull from task queue
      if (taskQueue.drainTo(tasks) == 0) {
        try {
          tasks.add(taskQueue.take());
        } catch (InterruptedException e) {
          logger.error("caught an exception when take task from queue:{}", workThread.getName(), e);
          continue;
        }
      }
      for (SingleTask singleTask : tasks) {
        if (singleTask instanceof BogusSingleTask) {
          logger.warn("going to exit:{} now", workThread.getName());
          return;
        }
      }

      try {
        process(tasks);
      } catch (Exception e) {
        logger.error("fail to process tasks:{}", tasks);
      }
    }
  }

  @Override
  public void freeTask(SingleTask singleTask) {

    AtomicInteger referCount = keepSingleMap.get(singleTask);
    boolean isTrue = referCount.compareAndSet(WORKING, NOT_WORKING);
    Validate.isTrue(isTrue, "can not happen here:{}", workThread.getName());
  }

  @Override
  public boolean putTask(SingleTask singleTask) {
    if (!keepSingleMap.containsKey(singleTask)) {
      synchronized (keepSingleMap) {
        if (!keepSingleMap.containsKey(singleTask)) {
          keepSingleMap.put(singleTask, new AtomicInteger(NOT_WORKING));
        }
      }
    }

    AtomicInteger referCount = keepSingleMap.get(singleTask);
    if (referCount.compareAndSet(NOT_WORKING, WORKING)) {
      taskQueue.offer(singleTask);
      return true;
    }
    return false;
  }

  @Override
  public void start() {
    this.workThread.start();
  }

  @Override
  public void stop() {
    this.stop = true;
    taskQueue.offer(new BogusSingleTask());
    try {
      workThread.join(1000);
      keepSingleMap.clear();
    } catch (InterruptedException e) {
      logger.error("try exit {} thread failed", workThread.getName(), e);
    }
  }

  @Override
  public void process(List<SingleTask> singleTask) {

  }

  @Override
  public int getTaskCount() {
    int taskCount = 0;
    for (AtomicInteger integer : keepSingleMap.values()) {
      if (integer.get() == WORKING) {
        taskCount++;
      }
    }
    return taskCount;
  }

}
