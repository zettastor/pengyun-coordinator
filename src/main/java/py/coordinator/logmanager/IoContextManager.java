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
