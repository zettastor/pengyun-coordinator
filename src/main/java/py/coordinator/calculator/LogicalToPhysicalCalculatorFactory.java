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

package py.coordinator.calculator;

import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.volumeinfo.VolumeInfoRetriever;


public class LogicalToPhysicalCalculatorFactory {

  private VolumeInfoRetriever volumeInfoRetriever;

  public VolumeInfoRetriever getVolumeInfoRetriever() {
    return volumeInfoRetriever;
  }

  public void setVolumeInfoRetriever(VolumeInfoRetriever volumeInfoRetriever) {
    this.volumeInfoRetriever = volumeInfoRetriever;
  }



  public LogicalToPhysicalCalculator build(Long volumeId) {
    CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();
    String type = cfg.getConvertPosType();
    if (type.equalsIgnoreCase(ConvertPosType.STRIPE.name())) {
      return new StripeLogicalToPhysicalCalculator(volumeInfoRetriever, volumeId,
          cfg.getSegmentSize(), cfg.getPageSize());
    } else if (type.equalsIgnoreCase(ConvertPosType.DIRECT.name())) {
      return new DirectLogicalToPhysicalCalculator(cfg.getSegmentSize(), cfg.getPageSize());
    } else {
      throw new RuntimeException("can not build calculator for this type: " + type);
    }
  }

  public enum ConvertPosType {
    STRIPE, DIRECT
  }
}
