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

package py.coordinator.worker;

import py.coordinator.lib.VolumeInfoHolderManager;
import py.coordinator.utils.NetworkDelayRecorder;
import py.periodic.Worker;
import py.periodic.WorkerFactory;



public class OutputPerformanceWorkerFactory implements WorkerFactory {

  private OutputPerformanceWorker worker;
  private String filePath;
  private Long volumeId;
  private Long storagePoolId;
  private String volumeName;
  private String storagePoolName;
  private VolumeInfoHolderManager volumeInfoHolderManager;

  private NetworkDelayRecorder networkDelayRecorder;
  private String myHostname;
  private int networkDelayMeanOutputIntervalSec;

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new OutputPerformanceWorker(this.volumeInfoHolderManager, networkDelayRecorder);
      worker.setFilePath(filePath);
      worker.setMyHostname(myHostname);
      worker.setNetworkDelayMeanOutputIntervalSec(networkDelayMeanOutputIntervalSec);
    }
    return worker;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public void setStoragePoolName(String storagePoolName) {
    this.storagePoolName = storagePoolName;
  }

  public void setVolumeId(Long volumeId) {
    this.volumeId = volumeId;
  }

  public void setStoragePoolId(Long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public void setVolumeInfoHolderManager(VolumeInfoHolderManager volumeInfoHolderManager) {
    this.volumeInfoHolderManager = volumeInfoHolderManager;
  }

  public NetworkDelayRecorder getNetworkDelayRecorder() {
    return networkDelayRecorder;
  }

  public void setNetworkDelayRecorder(NetworkDelayRecorder networkDelayRecorder) {
    this.networkDelayRecorder = networkDelayRecorder;
  }

  public String getMyHostname() {
    return myHostname;
  }

  public void setMyHostname(String myHostname) {
    this.myHostname = myHostname;
  }

  public int getNetworkDelayMeanOutputIntervalSec() {
    return networkDelayMeanOutputIntervalSec;
  }

  public void setNetworkDelayMeanOutputIntervalSec(int networkDelayMeanOutputIntervalSec) {
    this.networkDelayMeanOutputIntervalSec = networkDelayMeanOutputIntervalSec;
  }
}
