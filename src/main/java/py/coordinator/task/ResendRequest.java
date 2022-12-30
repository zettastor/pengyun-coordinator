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

package py.coordinator.task;


import py.common.DelayRequest;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.logmanager.IoContextManager;
import py.drivercontainer.utils.DriverContainerUtils;


public class ResendRequest extends DelayRequest {


  public static final int RESEND_LOG_TIME_DELAY_MAX = 500;
  private final IoContextManager manager;
  private final boolean needUpdateMembership;



  public ResendRequest(IoContextManager logManager, boolean needUpdateMembership) {
    this(logManager, needUpdateMembership,
        DriverContainerUtils.getExponentialBackoffSleepTime(
            CoordinatorConfigSingleton.getInstance().getResendDelayTimeUnitMs(),
            logManager.getFailTimes(),
            RESEND_LOG_TIME_DELAY_MAX));
  }



  public ResendRequest(IoContextManager logManager, boolean needUpdateMembership, long delay) {
    super(delay);
    this.manager = logManager;
    this.needUpdateMembership = needUpdateMembership;
  }

  public IoContextManager getIoContextManager() {
    return manager;
  }

  public boolean needUpdateMembership() {
    return needUpdateMembership;
  }
}
