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
