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


public class ClonedSourceVolumeListenerForReadIO implements ClonedSourceVolumeReadListener {

  private boolean hasDone = false;
  private IoContextManager ioContextManager;
  private ClonedProcessStatus clonedProcessStatus;

  public ClonedSourceVolumeListenerForReadIO(
      IoContextManager ioContextManager) {
    this.ioContextManager = ioContextManager;
    this.clonedProcessStatus = ClonedProcessStatus.Step_Begin;
  }

  @Override
  public void done() {
    switch (clonedProcessStatus) {
      case Step_Begin:
        clonedProcessStatus = ClonedProcessStatus.Step_ReadDone;
        ioContextManager.doResultForLinkedCloneVolume(this);
        break;
      case Step_ReadDone:
        setHasDone();
        break;
      default:
        break;
    }
  }

  private void setHasDone() {
    hasDone = true;
  }

  @Override
  public boolean isDone() {
    return hasDone;
  }

  @Override
  public void failed(Throwable throwable) {

  }
}
