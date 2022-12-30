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
import py.common.struct.Pair;
import py.drivercontainer.exception.ProcessPositionException;
import py.exception.StorageException;


public interface LogicalToPhysicalCalculator {

  public void updateVolumeInformation() throws StorageException;

  public Pair<Integer, Long> convertLogicalPositionToPhysical(long position)
      throws ProcessPositionException,
      StorageException;

  public Pair<Integer, Long> convertLogicalPositionToPhysical(long position, long timeoutMs)
      throws ProcessPositionException, StorageException;

  public Pair<Integer, Long> convertPhysicalPositionToLogical(long position)
      throws ProcessPositionException,
      StorageException;

  public Pair<Integer, Long> convertPhysicalPositionToLogical(long position, long timeoutMs)
      throws ProcessPositionException, StorageException;

  public int calculateIndex(long position);

  public void setVolumeLayout(List<Long> volumeLayout);

  public long getPageSize();
}
