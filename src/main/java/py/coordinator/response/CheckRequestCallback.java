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

package py.coordinator.response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.instance.InstanceId;
import py.netty.core.AbstractMethodCallback;
import py.proto.Broadcastlog;


public class CheckRequestCallback extends AbstractMethodCallback<Broadcastlog.PbCheckResponse> {

  private static final Logger logger = LoggerFactory.getLogger(CheckRequestCallback.class);
  private final CheckRequestCallbackCollector checkRequestCallbackCollector;
  private final InstanceId passByMeToCheck;

  public CheckRequestCallback(CheckRequestCallbackCollector checkRequestCallbackCollector,
      InstanceId passByMeToCheck) {
    this.checkRequestCallbackCollector = checkRequestCallbackCollector;
    this.passByMeToCheck = passByMeToCheck;
  }

  @Override
  public void complete(Broadcastlog.PbCheckResponse object) {
    checkRequestCallbackCollector.complete(object, passByMeToCheck);
  }

  @Override
  public void fail(Exception e) {
    checkRequestCallbackCollector.fail(e, passByMeToCheck);
  }
}
