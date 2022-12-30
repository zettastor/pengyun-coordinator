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

package py.coordinator.logmanager;

import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.lib.Coordinator;
import py.membership.IoMember;
import py.netty.datanode.PyReadResponse;


public class ReadMethodCallback extends IoMethodCallback<PyReadResponse> {

  private static final Logger logger = LoggerFactory.getLogger(ReadMethodCallback.class);
  private static String className = null;
  private PyReadResponse response;

  public ReadMethodCallback(IoContextManagerForAsyncDatanodeCallback ioContextManager,
      AtomicInteger readCounter, Coordinator coordinator,
      IoMember ioMember) {
    super(ioContextManager, readCounter, coordinator, ioMember);
  }



  public String getClassName() {
    if (className == null) {
      className = getClass().getSimpleName();
    }
    return className;
  }

  @Override
  public void complete(PyReadResponse object) {
    this.response = object;
    try {
      super.complete(object);
    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }
  }

  @Override
  public void fail(Exception e) {
    logger.info("ori:{} caught an exception:{} when reading", ioContextManager.getRequestId(),
        e.getMessage());
    try {
      super.fail(e);
    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }
  }

  @Override
  public PyReadResponse getResponse() {
    return response;
  }

  @Override
  public boolean weakGoodResponse() {
    if (getResponse() != null) {
      return true;
    }

    return false;
  }

}