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

import static com.github.sonus21.rqueue.utils.Constants.ONE_MILLI;
import static com.github.sonus21.rqueue.utils.HttpUtils.readUrl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessageMetaDataService;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.models.db.QueueMetaData;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.MoveMessageRequest;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.MoveMessageResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.dao.RqueueQMetaDataDao;
import com.github.sonus21.rqueue.web.service.RqueueDashboardUtilityService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Service
@Slf4j
public class RqueueDashboardUtilityServiceImpl implements RqueueDashboardUtilityService {
  @NonNull private RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  @NonNull private RqueueQMetaDataDao rqueueQMetaDataDao;
  @NonNull private RqueueWebConfig rqueueWebConfig;
  @NonNull private RqueueMessageTemplate rqueueMessageTemplate;
  @NonNull private RqueueMessageMetaDataService messageMetaDataService;
  private String latestVersion = "NA";
  private long versionFetchTime = 0;

  @Override
  public List<String> getQueues() {
    Set<String> members = stringRqueueRedisTemplate.getMembers(QueueUtils.getQueuesKey());
    if (CollectionUtils.isEmpty(members)) {
      return Collections.emptyList();
    }
    return new ArrayList<>(members);
  }

  @Override
  public List<QueueMetaData> getQueueMetadata(List<String> queues) {
    Collection<String> ids = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queues)) {
      ids = queues.stream().map(QueueUtils::getMetaDataKey).collect(Collectors.toList());
    }
    if (!CollectionUtils.isEmpty(ids)) {
      return rqueueQMetaDataDao.findAll(ids);
    }
    return Collections.emptyList();
  }

  @Override
  public BooleanResponse deleteMessage(String queueName, String id) {
    String qMetaId = QueueUtils.getMetaDataKey(queueName);
    QueueMetaData queueMetaData = rqueueQMetaDataDao.get(qMetaId);
    BooleanResponse booleanResponse = new BooleanResponse();
    if (queueMetaData == null) {
      booleanResponse.setCode(1);
      booleanResponse.setMessage("Queue metadata not found!");
      return booleanResponse;
    }
    messageMetaDataService.deleteMessage(id, Duration.ofSeconds(Constants.SECONDS_IN_A_MONTH));
    booleanResponse.setValue(true);
    return booleanResponse;
  }

  private MoveMessageResponse moveMessageToZset(MoveMessageRequest moveMessageRequest) {
    String src = moveMessageRequest.getSrc();
    String dst = moveMessageRequest.getDst();
    int requestMessageCount = moveMessageRequest.getMessageCount(rqueueWebConfig);
    String newScore = (String) moveMessageRequest.getOthers().get("newScore");
    String isFixedScore = (String) moveMessageRequest.getOthers().get("fixedScore");
    long scoreInMilli = 0;
    boolean fixedScore = false;
    if (newScore != null) {
      scoreInMilli = Long.parseLong(newScore);
    }
    if (isFixedScore != null) {
      fixedScore = Boolean.parseBoolean(isFixedScore);
    }
    MessageMoveResult result;
    if (moveMessageRequest.getSrcType() == DataType.ZSET) {
      result =
          rqueueMessageTemplate.moveMessageZsetToZset(
              src, dst, requestMessageCount, scoreInMilli, fixedScore);
    } else {
      result =
          rqueueMessageTemplate.moveMessageListToZset(src, dst, requestMessageCount, scoreInMilli);
    }
    MoveMessageResponse response = new MoveMessageResponse(result.getNumberOfMessages());
    response.setValue(result.isSuccess());
    return response;
  }

  private MoveMessageResponse moveMessageToList(MoveMessageRequest moveMessageRequest) {
    String src = moveMessageRequest.getSrc();
    String dst = moveMessageRequest.getDst();
    int requestMessageCount = moveMessageRequest.getMessageCount(rqueueWebConfig);
    MessageMoveResult result;
    if (moveMessageRequest.getSrcType() == DataType.ZSET) {
      result = rqueueMessageTemplate.moveMessageZsetToList(src, dst, requestMessageCount);
    } else {
      result = rqueueMessageTemplate.moveMessageListToList(src, dst, requestMessageCount);
    }
    MoveMessageResponse response = new MoveMessageResponse(result.getNumberOfMessages());
    response.setValue(result.isSuccess());
    return response;
  }

  @Override
  public MoveMessageResponse moveMessage(MoveMessageRequest moveMessageRequest) {
    String message = moveMessageRequest.validationMessage();
    if (StringUtils.isEmpty(message)) {
      MoveMessageResponse transferResponse = new MoveMessageResponse();
      transferResponse.setCode(1);
      transferResponse.setMessage(message);
      return transferResponse;
    }
    DataType dstType = moveMessageRequest.getDstType();
    switch (dstType) {
      case ZSET:
        return moveMessageToZset(moveMessageRequest);
      case LIST:
        return moveMessageToList(moveMessageRequest);
      default:
        throw new UnknownSwitchCase(dstType.name());
    }
  }

  @Override
  public BooleanResponse deleteQueueMessages(String queueName, int remainingMessages) {
    int start = -1 * remainingMessages;
    int end = -1;
    if (remainingMessages == 0) {
      start = 2;
      end = 1;
    }
    if (stringRqueueRedisTemplate.type(queueName)
        == org.springframework.data.redis.connection.DataType.LIST) {
      stringRqueueRedisTemplate.ltrim(queueName, start, end);
      return new BooleanResponse(true);
    }
    return new BooleanResponse(false);
  }

  @Override
  public String getLatestVersion() {
    if (System.currentTimeMillis() - versionFetchTime > Constants.SECONDS_IN_A_DAY * ONE_MILLI) {
      String response = readUrl(Constants.MAVEN_REPO_LINK + "/maven-metadata.xml");
      if (response != null) {
        List<String> lines =
            Arrays.stream(response.split("\n"))
                .map(String::trim)
                .filter(e -> e.startsWith("<latest>"))
                .collect(Collectors.toList());
        if (!lines.isEmpty()) {
          latestVersion = lines.get(0);
          versionFetchTime = System.currentTimeMillis();
        }
      }
    }
    return latestVersion;
  }

  @Override
  public List<List<Object>> getRunningTasks() {
    List<String> queues = getQueues();
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queues)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (String queue : queues) {
                  connection.zCard(QueueUtils.getProcessingQueueName(queue).getBytes());
                }
              }));
    }
    List<Object> headers = new ArrayList<>();
    headers.add("Queue");
    headers.add("Processing ZSET");
    headers.add("Size");
    rows.add(headers);
    for (int i = 0; i < queues.size(); i++) {
      List<Object> row = new ArrayList<>();
      row.add(queues.get(i));
      row.add(QueueUtils.getProcessingQueueName(queues.get(i)));
      row.add(result.get(i));
      rows.add(row);
    }
    return rows;
  }

  @Override
  public List<List<Object>> getWaitingTasks() {
    List<String> queues = getQueues();
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queues)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (String queue : queues) {
                  connection.lLen(queue.getBytes());
                }
              }));
    }
    List<Object> headers = new ArrayList<>();
    headers.add("Queue");
    headers.add("Size");
    rows.add(headers);
    for (int i = 0; i < queues.size(); i++) {
      List<Object> row = new ArrayList<>();
      row.add(queues.get(i));
      row.add(result.get(i));
      rows.add(row);
    }
    return rows;
  }

  @Override
  public StringResponse getDataType(String name) {
    return new StringResponse(
        DataType.convertDataType(stringRqueueRedisTemplate.type(name)).name());
  }

  @Override
  public List<List<Object>> getScheduledTasks() {
    List<String> queues = getQueues();
    List<QueueMetaData> queueMetaDatas =
        getQueueMetadata(queues).stream()
            .filter(QueueMetaData::isDelayed)
            .collect(Collectors.toList());
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queueMetaDatas)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (QueueMetaData queueMetaData : queueMetaDatas) {
                  connection.zCard(
                      QueueUtils.getDelayedQueueName(queueMetaData.getName()).getBytes());
                }
              }));
    }
    List<Object> headers = new ArrayList<>();
    headers.add("Queue");
    headers.add("Scheduled ZSET");
    headers.add("Size");
    rows.add(headers);
    for (int i = 0; i < queueMetaDatas.size(); i++) {
      List<Object> row = new ArrayList<>();
      QueueMetaData queueMetaData = queueMetaDatas.get(i);
      row.add(queueMetaData.getName());
      row.add(QueueUtils.getDelayedQueueName(queueMetaData.getName()));
      row.add(result.get(i));
      rows.add(row);
    }
    return rows;
  }

  private void addRows(
      List<Object> result, List<List<Object>> rows, List<Entry<String, String>> queueNameAndDlq) {
    for (int i = 0, j = 0; i < queueNameAndDlq.size(); i++) {
      Entry<String, String> entry = queueNameAndDlq.get(i);
      List<Object> row = new ArrayList<>();
      if (entry.getValue().isEmpty()) {
        row.add(entry.getKey());
        row.add("");
        row.add("");
      } else {
        if (i == 0
            || !queueNameAndDlq.get(i).getKey().equals(queueNameAndDlq.get(i - 1).getKey())) {
          row.add(entry.getKey());
        } else {
          row.add("");
        }
        row.add(entry.getValue());
        row.add(result.get(j++));
      }
      rows.add(row);
    }
  }

  @Override
  public List<List<Object>> getDeadLetterTasks() {
    List<String> queues = getQueues();
    List<Entry<String, String>> queueNameAndDlq = new ArrayList<>();
    for (QueueMetaData queueMetaData : getQueueMetadata(queues)) {
      if (queueMetaData.hasDeadLetterQueue()) {
        for (String dlq : queueMetaData.getDeadLetterQueues()) {
          queueNameAndDlq.add(new HashMap.SimpleEntry<>(queueMetaData.getName(), dlq));
        }
      } else {
        queueNameAndDlq.add(new HashMap.SimpleEntry<>(queueMetaData.getName(), ""));
      }
    }
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queueNameAndDlq)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (Entry<String, String> entry : queueNameAndDlq) {
                  if (!entry.getValue().isEmpty()) {
                    connection.lLen(entry.getValue().getBytes());
                  }
                }
              }));
    }
    List<Object> headers = new ArrayList<>();
    headers.add("Queue");
    headers.add("Dead Letter Queue");
    headers.add("Size");
    rows.add(headers);
    addRows(result, rows, queueNameAndDlq);
    return rows;
  }
}
