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

package py.coordinator.iorequest.iounitcontextpacket;

import java.util.List;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.logmanager.ClonedSourceVolumeReadListener;


public class SkipForSourceVolumeIoUnitContextPacket extends IoUnitContextPacketImpl {

  private ClonedSourceVolumeReadListener listener;


  
  public SkipForSourceVolumeIoUnitContextPacket(Long volumeId, Integer snapshotId,
      List<IoUnitContext> ioContexts, int logicalSegIndex, IoRequestType requestType,
      ClonedSourceVolumeReadListener listener) {
    super(volumeId, ioContexts, logicalSegIndex, requestType);
    this.listener = listener;
  }

  @Override
  public void complete() {
    super.complete();
    listener.done();
  }
}
