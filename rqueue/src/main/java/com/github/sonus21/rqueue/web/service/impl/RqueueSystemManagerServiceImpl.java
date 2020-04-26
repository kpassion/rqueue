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
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.QueueMetadata;
import com.github.sonus21.rqueue.models.event.QueueInitializationEvent;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.web.dao.RqueueSystemMetaDataDao;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

@Service
public class RqueueSystemManagerServiceImpl
    implements RqueueSystemManagerService, ApplicationListener<QueueInitializationEvent> {
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  private final RqueueSystemMetaDataDao systemMetadataDao;

  @Autowired
  public RqueueSystemManagerServiceImpl(
      RqueueRedisTemplate<String> stringRqueueRedisTemplate,
      RqueueSystemMetaDataDao rqueueSystemMetaDataDao) {
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
    this.systemMetadataDao = rqueueSystemMetaDataDao;
  }

  private List<String> queueKeys(QueueMetadata queueMetaData) {
    List<String> keys = new ArrayList<>();
    keys.add(queueMetaData.getName());
    keys.add(QueueUtils.getProcessingQueueName(queueMetaData.getName()));
    if (queueMetaData.isDelayed()) {
      keys.add(QueueUtils.getDelayedQueueName(queueMetaData.getName()));
    }
    if (queueMetaData.hasDeadLetterQueue()) {
      keys.addAll(queueMetaData.getDeadLetterQueues());
    }
    keys.add(QueueUtils.getQueueStatKey(queueMetaData.getName()));
    return keys;
  }

  @Override
  public BaseResponse deleteQueue(String queueName) {
    QueueMetadata queueMetaData =
        systemMetadataDao.getQMetadata(QueueUtils.getMetaDataKey(queueName));
    BaseResponse baseResponse = new BaseResponse();
    if (queueMetaData == null) {
      baseResponse.setCode(1);
      baseResponse.setMessage("Queue not found");
      return baseResponse;
    }
    queueMetaData.setDeletedOn(System.currentTimeMillis());
    queueMetaData.setDeleted(true);
    RedisUtils.executePipeLine(
        stringRqueueRedisTemplate.getRedisTemplate(),
        ((connection, keySerializer, valueSerializer) -> {
          for (String key : queueKeys(queueMetaData)) {
            connection.del(key.getBytes());
          }
          connection.set(
              queueMetaData.getId().getBytes(), valueSerializer.serialize(queueMetaData));
        }));
    baseResponse.setCode(0);
    baseResponse.setMessage("Queue deleted");
    return baseResponse;
  }

  private void updateQueueMetadata(Entry<String, QueueDetail> entry) {
    String queueName = entry.getKey();
    QueueDetail queueDetail = entry.getValue();
    String qMetaId = QueueUtils.getMetaDataKey(queueName);
    QueueMetadata queueMetaData = systemMetadataDao.getQMetadata(qMetaId);
    boolean updated = false;
    boolean created = false;
    if (queueMetaData == null) {
      created = true;
      queueMetaData =
          new QueueMetadata(
              qMetaId,
              queueName,
              queueDetail.getNumRetries(),
              queueDetail.isDelayedQueue(),
              queueDetail.getVisibilityTimeout());
    }
    if (queueDetail.isDlqSet()) {
      updated = queueMetaData.addDeadLetterQueue(queueDetail.getDeadLetterQueueName());
    }
    updated = queueMetaData.updateVisibilityTimeout(queueDetail.getVisibilityTimeout()) || updated;
    updated = queueMetaData.updateIsDelay(queueDetail.isDelayedQueue()) || updated;
    updated = queueMetaData.updateRetryCount(queueDetail.getNumRetries()) || updated;
    if (updated && !created) {
      queueMetaData.updateTime();
    }
    if (updated || created) {
      systemMetadataDao.saveQMetadata(queueMetaData);
    }
  }

  @Override
  public void onApplicationEvent(QueueInitializationEvent event) {
    if (event.isStart()) {
      Set<String> queueNames = event.getQueueDetailMap().keySet();
      if (queueNames.isEmpty()) {
        return;
      }
      String[] queues = new String[queueNames.size()];
      int i = 0;
      for (String queue : queueNames) {
        queues[i++] = queue;
      }
      stringRqueueRedisTemplate.addToSet(QueueUtils.getQueuesKey(), queues);
      for (Entry<String, QueueDetail> entry : event.getQueueDetailMap().entrySet()) {
        updateQueueMetadata(entry);
      }
    }
  }
}
