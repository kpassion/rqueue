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

package com.github.sonus21.rqueue.web.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.QueueMetadata;
import com.github.sonus21.rqueue.models.event.QueueInitializationEvent;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.web.dao.impl.RqueueSystemMetadataDaoImpl;
import com.github.sonus21.rqueue.web.service.impl.RqueueSystemManagerServiceImpl;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueQManagementServiceTest {
  private RqueueRedisTemplate<String> rqueueRedisTemplate = mock(RqueueRedisTemplate.class);
  private RqueueSystemMetadataDaoImpl rqueueQMetaDataDao = mock(RqueueSystemMetadataDaoImpl.class);
  private RqueueSystemManagerServiceImpl rqueueQRegistrationService =
      new RqueueSystemManagerServiceImpl(rqueueRedisTemplate, rqueueQMetaDataDao);
  private String queue1 = "queue-1";
  private String queue2 = "queue-2";
  private String queue3 = "queue-3";
  private String deadLetterQueue2 = "dl-queue-2";
  private String deadLetterQueue3 = "dl-queue-3";
  private QueueDetail queueDetail = new QueueDetail(queue1, Integer.MAX_VALUE, "", false, 900000L);
  private QueueDetail queueDetail2 = new QueueDetail(queue2, 3, deadLetterQueue2, true, 90000L);
  private QueueDetail queueDetail3 = new QueueDetail(queue3, 3, deadLetterQueue3, true, 90000L);
  private Map<String, QueueDetail> queueDetailMap = new HashMap<>();

  @Before
  public void init() {
    queueDetailMap.put(queue1, queueDetail);
    queueDetailMap.put(queue2, queueDetail2);
  }

  @Test
  public void onApplicationEventStop() {
    QueueInitializationEvent queueInitializationEvent =
        new QueueInitializationEvent("Container", queueDetailMap, false);
    rqueueQRegistrationService.onApplicationEvent(queueInitializationEvent);
    verify(rqueueQMetaDataDao, times(0)).getQMetadata(anyString());
    verify(rqueueRedisTemplate, times(0)).addToSet(anyString());
  }

  @Test
  public void onApplicationEventStart() {
    QueueInitializationEvent queueInitializationEvent =
        new QueueInitializationEvent("Container", queueDetailMap, true);
    doAnswer(
            invocation -> {
              QueueMetadata queueMetaData = (QueueMetadata) invocation.getArgument(0);
              if (queue1.equals(queueMetaData.getName())) {
                assertEquals(0, queueMetaData.getDeadLetterQueues().size());
                assertFalse(queueMetaData.isDelayed());
                assertEquals(Integer.MAX_VALUE, queueMetaData.getNumRetry());
                assertEquals(900000L, queueMetaData.getVisibilityTimeout());
              } else if (queue2.equals(queueMetaData.getName())) {
                assertEquals(
                    Collections.singleton(deadLetterQueue2), queueMetaData.getDeadLetterQueues());
                assertTrue(queueMetaData.isDelayed());
                assertEquals(3, queueMetaData.getNumRetry());
                assertEquals(90000L, queueMetaData.getVisibilityTimeout());
              } else {
                fail("Invalid queue name " + queueMetaData.getName());
              }
              return null;
            })
        .when(rqueueQMetaDataDao)
        .saveQMetadata(any());
    rqueueQRegistrationService.onApplicationEvent(queueInitializationEvent);
    verify(rqueueQMetaDataDao, times(2)).getQMetadata(anyString());
    verify(rqueueRedisTemplate, times(1)).addToSet(anyString(), any());
  }

  @Test
  public void onApplicationEventStartUpdate() {
    QueueInitializationEvent queueInitializationEvent =
        new QueueInitializationEvent(
            "Container", Collections.singletonMap(queue3, queueDetail3), true);
    QueueMetadata dbQueueMetaDate =
        new QueueMetadata(QueueUtils.getMetaDataKey(queue3), queue3, -1, false, 1000L);
    doReturn(dbQueueMetaDate).when(rqueueQMetaDataDao).getQMetadata(dbQueueMetaDate.getId());

    doAnswer(
            invocation -> {
              QueueMetadata queueMetaData = (QueueMetadata) invocation.getArguments()[0];
              if (queue3.equals(queueMetaData.getName())) {
                assertEquals(
                    Collections.singleton(deadLetterQueue3), queueMetaData.getDeadLetterQueues());
                assertTrue(queueMetaData.isDelayed());
                assertEquals(3, queueMetaData.getNumRetry());
                assertEquals(90000L, queueMetaData.getVisibilityTimeout());
              } else {
                fail("Invalid queue name " + queueMetaData.getName());
              }
              return null;
            })
        .when(rqueueQMetaDataDao)
        .saveQMetadata(any());
    rqueueQRegistrationService.onApplicationEvent(queueInitializationEvent);
    verify(rqueueQMetaDataDao, times(1)).getQMetadata(anyString());
    verify(rqueueRedisTemplate, times(1)).addToSet(eq("__rq::queues"), any());
  }
}
