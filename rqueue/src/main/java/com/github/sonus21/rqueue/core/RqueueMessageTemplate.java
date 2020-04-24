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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.models.MessageMoveResult;
import java.util.List;
import org.springframework.data.redis.core.RedisTemplate;

public interface RqueueMessageTemplate {
  RqueueMessage pop(String queueName, long visibilityTimeout);

  void addMessageWithDelay(String queueName, RqueueMessage rqueueMessage);

  void replaceMessage(String zsetName, RqueueMessage src, RqueueMessage tgt);

  Long addMessage(String listName, RqueueMessage rqueueMessage);

  Boolean addToZset(String zsetName, RqueueMessage rqueueMessage, long score);

  List<RqueueMessage> getAllMessages(String queueName);

  MessageMoveResult moveMessageListToList(
      String srcQueueName, String dstQueueName, int numberOfMessage);

  MessageMoveResult moveMessageZsetToList(
      String sourceZset, String destinationList, int maxMessage);

  MessageMoveResult moveMessageListToZset(
      String sourceList, String destinationZset, int maxMessage, long score);

  MessageMoveResult moveMessageZsetToZset(
      String sourceZset, String destinationZset, int maxMessage, long newScore, boolean fixedScore);

  List<RqueueMessage> readFromZset(String name, long start, long end);

  List<RqueueMessage> readFromList(String name, long start, long end);

  RedisTemplate<String, RqueueMessage> getTemplate();

  Long removeElementFromZset(String zsetName, RqueueMessage rqueueMessage);
}
