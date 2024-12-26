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

import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.pbrequest.RequestBuilder;
import py.membership.IoActionContext;

public interface IoContextManager extends IoContextManagerCommon {

  public void doResult();

  public void doResultForLinkedCloneVolume(ClonedSourceVolumeReadListener li);

  public boolean isExpired();

  public long getExpiredTime();

  public void setExpiredTime(long expiredTime);

  public int incFailTimes();

  public int getFailTimes();

  public int getLogicalSegmentIndex();

  public RequestBuilder<?> getRequestBuilder();

  public void setRequestBuilder(RequestBuilder<?> requestBuilder);

  public IoUnitContextPacket getCallback();

  public boolean needUpdateMembership(int quorumSize, int ioMemberCount);

  public void setIoActionContext(IoActionContext ioActionContext);

  public void resetRequestFailedInfo();

  public void doneDirectly();

  public void resetDelay();

  public long getResponseInterval();

  public void initRequestCount(int requestCount);

  public void releaseReference();
}
