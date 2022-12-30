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
