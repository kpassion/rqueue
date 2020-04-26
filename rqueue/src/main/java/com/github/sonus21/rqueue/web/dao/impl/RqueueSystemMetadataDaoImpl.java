/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.web.dao.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.models.db.QueueMetadata;
import com.github.sonus21.rqueue.web.dao.RqueueSystemMetaDataDao;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class RqueueSystemMetadataDaoImpl implements RqueueSystemMetaDataDao {
  private RqueueRedisTemplate<QueueMetadata> rqueueRedisTemplate;

  @Autowired
  public RqueueSystemMetadataDaoImpl(RqueueConfig rqueueConfig) {
    this.rqueueRedisTemplate = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  @Override
  public QueueMetadata getQMetadata(String key) {
    return rqueueRedisTemplate.get(key);
  }

  @Override
  public List<QueueMetadata> findAllQMetadata(Collection<String> ids) {
    return rqueueRedisTemplate.mget(ids).stream()
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public void saveQMetadata(QueueMetadata queueMetaData) {
    if (queueMetaData.getId() == null) {
      throw new IllegalArgumentException("id cannot be null " + queueMetaData);
    }
    rqueueRedisTemplate.set(queueMetaData.getId(), queueMetaData);
  }
}
