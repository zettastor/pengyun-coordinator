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

package py.coordinator.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.exception.StorageException;

/**
 * xx.
 */
public abstract class AbstractStorageVerify implements StorageVerify {

  private static final Logger logger = LoggerFactory.getLogger(AbstractStorageVerify.class);
  private final RelatedOffsetVerifyAlgorithm algorithm;

  public AbstractStorageVerify(int delta) {
    logger.warn("current delta: {}", delta);
    algorithm = new RelatedOffsetVerifyAlgorithm(delta);
  }

  public long alignPosition(long position) {
    return algorithm.alignPosition(position);
  }

  public int alginPosition(int size) {
    return (int) algorithm.alignPosition(size);
  }

  public byte[] generateData(long position, int length) {
    return algorithm.generateData(position, length);
  }

  public void verifyData(long position, byte[] data) throws StorageException {
    algorithm.verifyData(position, data);
  }

}
