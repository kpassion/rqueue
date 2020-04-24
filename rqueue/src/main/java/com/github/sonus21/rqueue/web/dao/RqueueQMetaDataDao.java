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

package com.github.sonus21.rqueue.web.dao;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.models.db.QueueMetaData;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class RqueueQMetaDataDao {
  @NonNull private RqueueConfig rqueueConfig;
  private RqueueRedisTemplate<QueueMetaData> rqueueRedisTemplate;

  @PostConstruct
  public void init() {
    this.rqueueRedisTemplate = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  public QueueMetaData get(String key) {
    return rqueueRedisTemplate.get(key);
  }

  public List<QueueMetaData> findAll(Collection<String> ids) {
    return rqueueRedisTemplate.mget(ids).stream()
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  public void save(QueueMetaData queueMetaData) {
    if (queueMetaData.getId() == null) {
      throw new IllegalArgumentException("id cannot be null " + queueMetaData);
    }
    rqueueRedisTemplate.set(queueMetaData.getId(), queueMetaData);
  }
}
