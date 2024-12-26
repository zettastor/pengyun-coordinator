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
