/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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