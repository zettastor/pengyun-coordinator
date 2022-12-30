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

package py.coordinator;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Utils;
import py.volume.VolumeMetadata;

@Deprecated
public class MemStoreProcessImpl {

  protected static final Logger logger = LoggerFactory.getLogger(MemStoreProcessImpl.class);
  private static final long ONE_SECTOR_SIZE = 50L * 1024L * 1024L * 1024L;
  private VolumeMetadata volumeMetadata;
  private FileFastBufferForCoordinator[] memoryStoreBuf;



  public MemStoreProcessImpl(String storagePath, VolumeMetadata volumeMetadata, long totalSizeGB)
      throws Exception {

    try {
      Validate.notNull(volumeMetadata);
      this.volumeMetadata = volumeMetadata;

      Path rootPath = Paths.get(storagePath);
      if (!Files.exists(rootPath, LinkOption.NOFOLLOW_LINKS)) {
        Files.createDirectories(rootPath);
      }
      int fileIndex = 0;
      long totalSize = (long) totalSizeGB * 1024L * 1024L * 1024L;
      int arrayLength = (int) (totalSize / ONE_SECTOR_SIZE) + 1;
      memoryStoreBuf = new FileFastBufferForCoordinator[arrayLength];
      long remainingSize = totalSize;
      while (remainingSize > 0) {
        Path pathToFile = Paths.get(storagePath, "data_storage_"
            + String.valueOf(this.volumeMetadata.getVolumeId()) + "_" + String.valueOf(fileIndex));
        long thisFileSize = 0L;
        if (remainingSize >= ONE_SECTOR_SIZE) {
          thisFileSize = ONE_SECTOR_SIZE;
          remainingSize -= ONE_SECTOR_SIZE;
        } else {
          thisFileSize = remainingSize;
          remainingSize = 0L;
        }
        Files.deleteIfExists(pathToFile);
        if (!Files.exists(pathToFile, LinkOption.NOFOLLOW_LINKS)) {
          Utils.createFile(pathToFile.toFile(), thisFileSize);
        }
        FileFastBufferForCoordinator tmpFileFastBuffer = new FileFastBufferForCoordinator(
            pathToFile.toString(),
            thisFileSize);
        memoryStoreBuf[fileIndex] = tmpFileFastBuffer;
        fileIndex++;
      }

    } catch (Exception e) {
      logger.error("failed to alloc buffer for test", e);
      System.exit(-1);
    }
  }












  private boolean crossSectors(long offset, long length) {
    return ((offset + length) / ONE_SECTOR_SIZE) > (offset / ONE_SECTOR_SIZE);
  }



































































































































  public VolumeMetadata getVolumeMetadata() {
    return volumeMetadata;
  }

  public void setVolumeMetadata(VolumeMetadata volumeMetadata) {
    this.volumeMetadata = volumeMetadata;
  }



  public void close() {
    for (int i = 0; i < memoryStoreBuf.length; i++) {
      memoryStoreBuf[i].close();
    }
  }
}
