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
