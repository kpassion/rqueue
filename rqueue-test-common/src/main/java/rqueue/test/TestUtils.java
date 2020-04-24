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

package rqueue.test;

import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.utils.QueueUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;

@Slf4j
public class TestUtils {
  private static final GenericMessageConverter converter = new GenericMessageConverter();

  private TestUtils() {}

  public static RqueueMessage buildMessage(
      Object object, String queueName, Integer retryCount, Long delay) {
    Message<?> msg = converter.toMessage(object, null);
    return new RqueueMessage(queueName, (String) msg.getPayload(), retryCount, delay);
  }

  public static Map<String, List<RqueueMessage>> getMessageMap(
      String queueName, RqueueMessageTemplate redisTemplate) {
    Map<String, List<RqueueMessage>> queueNameToMessage = new HashMap<>();

    List<RqueueMessage> messages = redisTemplate.readFromList(queueName, 0, -1);
    queueNameToMessage.put(queueName, messages);

    List<RqueueMessage> messagesFromZset =
        redisTemplate.readFromZset(QueueUtils.getDelayedQueueName(queueName), 0, -1);
    queueNameToMessage.put(QueueUtils.getDelayedQueueName(queueName), messagesFromZset);

    List<RqueueMessage> messagesInProcessingQueue =
        redisTemplate.readFromZset(QueueUtils.getProcessingQueueName(queueName), 0, -1);
    queueNameToMessage.put(QueueUtils.getProcessingQueueName(queueName), messagesInProcessingQueue);
    return queueNameToMessage;
  }

  public static void printQueueStats(List<String> queueNames, RqueueMessageTemplate redisTemplate) {
    for (String queueName : queueNames) {
      for (Entry<String, List<RqueueMessage>> entry :
          TestUtils.getMessageMap(queueName, redisTemplate).entrySet()) {
        for (RqueueMessage message : entry.getValue()) {
          log.info("Queue: {} Msg: {}", entry.getKey(), message);
        }
      }
    }
  }
}
