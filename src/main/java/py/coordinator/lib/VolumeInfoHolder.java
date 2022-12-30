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
