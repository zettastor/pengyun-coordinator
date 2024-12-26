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

package py.coordinator.iorequest.iounitcontextpacket;

import java.util.List;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;


public interface IoUnitContextPacket {

  public IoRequestType getRequestType();

  public int getLogicalSegIndex();

  public void complete();

  public List<IoUnitContext> getIoContext();

  public Long getVolumeId();

  public Integer getSnapshotId();

  public boolean hasSnapshotId();

  public void releaseReference();
}
