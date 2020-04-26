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

package com.github.sonus21.rqueue.web.service.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

@Service
public class RqueueMessageMetaDataServiceImpl implements RqueueMessageMetadataService {
  private RqueueRedisTemplate<MessageMetadata> template;

  @Autowired
  public RqueueMessageMetaDataServiceImpl(RqueueConfig rqueueConfig) {
    this.template = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  public MessageMetadata get(String id) {
    return template.get(id);
  }

  public List<MessageMetadata> findAll(Collection<String> ids) {
    return template.mget(ids).stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  public void save(MessageMetadata messageMetaData, Duration duration) {
    Assert.notNull(messageMetaData.getId(), "messageMetaData id cannot be null");
    template.set(messageMetaData.getId(), messageMetaData, duration);
  }

  public void deleteMessage(String messageId, Duration duration) {
    String id = QueueUtils.getMetaDataKey(messageId);
    MessageMetadata messageMetaData = get(id);
    if (messageMetaData == null) {
      messageMetaData = new MessageMetadata(messageId);
    }
    messageMetaData.setDeleted(true);
    messageMetaData.setDeletedOn(System.currentTimeMillis());
    template.set(messageMetaData.getId(), messageMetaData, duration);
  }

  public void delete(String messageMetaDataId) {
    template.delete(messageMetaDataId);
  }
}
