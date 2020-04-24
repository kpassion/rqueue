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

package com.github.sonus21.rqueue.models.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class QueueMetaDataTest {

  @Test
  public void addDeadLetterQueue() {
    QueueMetaData queueMetaData = new QueueMetaData();
    Set<String> queues = new HashSet<>();
    queues.add("test-dlq");
    assertTrue(queueMetaData.addDeadLetterQueue("test-dlq"));
    assertEquals(queues, queueMetaData.getDeadLetterQueues());
    assertFalse(queueMetaData.addDeadLetterQueue("test-dlq"));
    assertEquals(queues, queueMetaData.getDeadLetterQueues());
    assertTrue(queueMetaData.addDeadLetterQueue("test2-dlq"));
    queues.add("test2-dlq");
    assertEquals(queues, queueMetaData.getDeadLetterQueues());
  }

  @Test
  public void testVisibilityTimeout() {
    QueueMetaData queueMetaData = new QueueMetaData();
    assertTrue(queueMetaData.updateVisibilityTimeout(100L));
    assertEquals(100L, queueMetaData.getVisibilityTimeout());
    assertFalse(queueMetaData.updateVisibilityTimeout(100L));
    assertEquals(100L, queueMetaData.getVisibilityTimeout());
  }

  @Test
  public void testDelay() {
    QueueMetaData queueMetaData = new QueueMetaData();
    assertFalse(queueMetaData.isDelayed());
    assertTrue(queueMetaData.updateIsDelay(true));
    assertTrue(queueMetaData.isDelayed());
    assertFalse(queueMetaData.updateIsDelay(true));
    assertTrue(queueMetaData.isDelayed());
    assertTrue(queueMetaData.updateIsDelay(false));
    assertFalse(queueMetaData.isDelayed());
  }

  @Test
  public void testConstruction() {
    QueueMetaData queueMetaData = new QueueMetaData("__rq::q", "q", -1, true, 100L);
    assertTrue(queueMetaData.isDelayed());
    assertEquals("__rq::q", queueMetaData.getId());
    assertEquals("q", queueMetaData.getName());
    assertEquals(100L, queueMetaData.getVisibilityTimeout());
    assertEquals(-1, queueMetaData.getNumRetry());
    assertNotNull(queueMetaData.getDeadLetterQueues());
    assertNotNull(queueMetaData.getUpdatedOn());
    assertNotNull(queueMetaData.getCreatedOn());
    assertEquals(queueMetaData.getUpdatedOn(), queueMetaData.getCreatedOn());
  }
}
