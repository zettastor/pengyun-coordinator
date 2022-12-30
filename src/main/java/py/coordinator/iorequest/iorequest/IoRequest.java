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

package py.coordinator.iorequest.iorequest;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.util.List;
import java.util.concurrent.Semaphore;
import py.coordinator.iorequest.iounit.IoUnit;


public interface IoRequest {

  public long getOffset();

  public long getLength();

  public ByteBuf getBody();

  public int getIoSum();

  public void releaseBody();

  public IoRequestType getIoRequestType();

  /**
   * reply to io request initiator.
   */
  public void reply();

  public int decReferenceCount();

  public int incReferenceCount();

  public int getReferenceCount();

  public void add(IoUnit ioUnit);

  public List<IoUnit> getIoUnits();

  public void getTicket(Semaphore ioDepth);

  public void setTicketForRelease(Semaphore ioDepth);

  public Channel getChannel();

  public long getVolumeId();

  public void setVolumeId(Long volumeId);

}
