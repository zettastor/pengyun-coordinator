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

package py.coordinator.lib;

import com.google.common.collect.Multimap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import py.archive.segment.SegId;
import py.coordinator.calculator.LogicalToPhysicalCalculator;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.instance.InstanceId;
import py.thrift.share.StoragePoolThrift;


public interface VolumeInfoHolder {

  public void init() throws Exception;

  public Long getVolumeId();

  public VolumeInfoRetriever getVolumeInfoRetriever();

  public Multimap<Integer, IoUnitContextPacket> getIoContextWaitingForVolumeCompletion();

  public Map<SegId, Integer> getMapSegIdToIndex();

  public Map<SegId, AtomicLong> getRecordLastIoTime();

  public SpaceSavingVolumeMetadata getVolumeMetadata();

  public LogicalToPhysicalCalculator getLogicalToPhysicalCalculator();


  public void setLogicalToPhysicalCalculator(LogicalToPhysicalCalculator calculator);

  public StoragePoolThrift getStoragePoolThrift();

  public void updateVolumeMetadataWhenSegmentCreated(int startSegmentIndex);

  public void updateVolumeMetadataIfVolumeExtended() throws Exception;


  public void setNotifyDatanodeCount(int notifyDatanodeCount);

  public void decNotifyDatanodeCount();

  public boolean allDatanodeResponse();

  public boolean hasFailedDatanode();

  public List<InstanceId> pullAllFailedDatanodes();

  public void addFailedDatanode(InstanceId failedDatanode);

  public boolean volumeIsAvailable();


  public int getLeftNotifyDatanodeCount();

}
