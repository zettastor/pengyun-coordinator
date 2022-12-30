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

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;
import py.drivercontainer.exception.ProcessPositionException;
import py.exception.StorageException;


public class DirectLogicalToPhysicalCalculator implements LogicalToPhysicalCalculator {

  private static final Logger logger = LoggerFactory
      .getLogger(DirectLogicalToPhysicalCalculator.class);
  private final long segmentSize;
  private final long pageSize;

  public DirectLogicalToPhysicalCalculator(long segmentSize, long pageSize) {
    this.segmentSize = segmentSize;
    this.pageSize = pageSize;
  }

  @Override
  public void updateVolumeInformation() throws StorageException {
    logger.debug("this is a direct logical to physical map");
  }

  @Override
  public Pair<Integer, Long> convertLogicalPositionToPhysical(long position)
      throws ProcessPositionException,
      StorageException {
    return new Pair<Integer, Long>(calculateIndex(position), position % segmentSize);
  }

  @Override
  public Pair<Integer, Long> convertLogicalPositionToPhysical(long position, long timeoutMs)
      throws ProcessPositionException, StorageException {
    return convertLogicalPositionToPhysical(position);
  }

  @Override
  public Pair<Integer, Long> convertPhysicalPositionToLogical(long position)
      throws ProcessPositionException,
      StorageException {
    return new Pair<Integer, Long>(calculateIndex(position), position % segmentSize);
  }

  @Override
  public Pair<Integer, Long> convertPhysicalPositionToLogical(long position, long timeoutMs)
      throws ProcessPositionException, StorageException {
    return convertPhysicalPositionToLogical(position);
  }

  @Override
  public int calculateIndex(long position) {
    return (int) (position / segmentSize);
  }

  @Override
  public void setVolumeLayout(List<Long> volumeLayout) {
    logger.debug("this is a direct logical to physical map");
  }

  @Override
  public long getPageSize() {
    return pageSize;
  }
}
