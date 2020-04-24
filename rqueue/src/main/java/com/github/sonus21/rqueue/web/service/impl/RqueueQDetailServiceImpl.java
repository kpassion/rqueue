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

import static com.github.sonus21.rqueue.utils.StringUtils.clean;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageMetaDataService;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.db.MessageMetaData;
import com.github.sonus21.rqueue.models.db.QueueMetaData;
import com.github.sonus21.rqueue.models.enums.ActionType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.response.DataStructureMetaData;
import com.github.sonus21.rqueue.models.response.QueueExplorePageResponse;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.utils.TimeUtils;
import com.github.sonus21.rqueue.web.service.RqueueDashboardUtilityService;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
@AllArgsConstructor(onConstructor = @__(@Autowired))
public class RqueueQDetailServiceImpl implements RqueueQDetailService {
  private RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  private RqueueMessageTemplate rqueueMessageTemplate;
  private RqueueDashboardUtilityService rqueueDashboardUtilityService;
  private RqueueMessageMetaDataService rqueueMessageMetaDataService;

  @Override
  public List<QueueMetaData> getQueueMetadata() {
    List<String> queues = rqueueDashboardUtilityService.getQueues();
    return rqueueDashboardUtilityService.getQueueMetadata(queues);
  }

  @Override
  public QueueMetaData getMeta(String queueName) {
    List<QueueMetaData> metaData =
        rqueueDashboardUtilityService.getQueueMetadata(Collections.singletonList(queueName));
    if (CollectionUtils.isEmpty(metaData)) {
      return null;
    }
    return metaData.get(0);
  }

  @Override
  public Map<String, List<Entry<NavTab, DataStructureMetaData>>> getQueueDataStructureDetails(
      List<QueueMetaData> queueMetaData) {
    return queueMetaData.stream()
        .collect(Collectors.toMap(QueueMetaData::getName, this::getQueueDataStructureDetails));
  }

  @Override
  public List<Entry<NavTab, DataStructureMetaData>> getQueueDataStructureDetails(
      QueueMetaData queueMetaData) {
    List<Entry<NavTab, DataStructureMetaData>> metaData = new ArrayList<>();
    if (queueMetaData == null) {
      return metaData;
    }
    String name = queueMetaData.getName();
    Long pending = stringRqueueRedisTemplate.getListSize(name);
    metaData.add(
        new HashMap.SimpleEntry<>(
            NavTab.PENDING,
            new DataStructureMetaData(name, DataType.LIST, pending == null ? 0 : pending)));
    String processingQueueName = QueueUtils.getProcessingQueueName(name);
    Long running = stringRqueueRedisTemplate.getZsetSize(processingQueueName);
    metaData.add(
        new HashMap.SimpleEntry<>(
            NavTab.RUNNING,
            new DataStructureMetaData(
                processingQueueName, DataType.ZSET, running == null ? 0 : running)));
    if (queueMetaData.isDelayed()) {
      String timeQueueName = QueueUtils.getDelayedQueueName(name);
      Long scheduled = stringRqueueRedisTemplate.getZsetSize(timeQueueName);
      metaData.add(
          new HashMap.SimpleEntry<>(
              NavTab.SCHEDULED,
              new DataStructureMetaData(
                  timeQueueName, DataType.ZSET, scheduled == null ? 0 : scheduled)));
    }
    if (!CollectionUtils.isEmpty(queueMetaData.getDeadLetterQueues())) {
      for (String dlq : queueMetaData.getDeadLetterQueues()) {
        Long dlqSize = stringRqueueRedisTemplate.getListSize(dlq);
        metaData.add(
            new HashMap.SimpleEntry<>(
                NavTab.DEAD,
                new DataStructureMetaData(dlq, DataType.LIST, dlqSize == null ? 0 : dlqSize)));
      }
    }
    return metaData;
  }

  @Override
  public List<NavTab> getNavTabs(QueueMetaData queueMetaData) {
    List<NavTab> navTabs = new ArrayList<>();
    if (queueMetaData != null) {
      navTabs.add(NavTab.PENDING);
      if (queueMetaData.isDelayed()) {
        navTabs.add(NavTab.SCHEDULED);
      }
      navTabs.add(NavTab.RUNNING);
      if (!CollectionUtils.isEmpty(queueMetaData.getDeadLetterQueues())) {
        navTabs.add(NavTab.DEAD);
      }
    }
    return navTabs;
  }

  private List<RqueueMessage> readFromZset(String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;
    return rqueueMessageTemplate.readFromZset(name, start, end);
  }

  private List<RqueueMessage> readFromList(String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;
    return rqueueMessageTemplate.readFromList(name, start, end);
  }

  private interface RowBuilder {
    List<Serializable> row(RqueueMessage rqueueMessage, boolean deleted);
  }

  private List<List<Serializable>> buildRows(
      List<RqueueMessage> rqueueMessages, RowBuilder rowBuilder) {
    if (CollectionUtils.isEmpty(rqueueMessages)) {
      return Collections.emptyList();
    }
    List<String> ids =
        rqueueMessages.stream()
            .map(RqueueMessage::getId)
            .map(QueueUtils::getMetaDataKey)
            .collect(Collectors.toList());

    List<MessageMetaData> vals = rqueueMessageMetaDataService.findAll(ids);
    Map<String, Boolean> msgIdToDeleted =
        vals.stream().collect(Collectors.toMap(MessageMetaData::getMessageId, e -> true));
    return rqueueMessages.stream()
        .map(e -> rowBuilder.row(e, msgIdToDeleted.getOrDefault(e.getId(), false)))
        .collect(Collectors.toList());
  }

  class ZsetRowBuilder implements RowBuilder {
    private final boolean deadLetterQueue;
    private final boolean timeQueue;
    private final long currentTime;

    ZsetRowBuilder(boolean deadLetterQueue, boolean timeQueue) {
      this.deadLetterQueue = deadLetterQueue;
      this.timeQueue = timeQueue;
      this.currentTime = System.currentTimeMillis();
    }

    @Override
    public List<Serializable> row(RqueueMessage rqueueMessage, boolean deleted) {
      List<Serializable> row = new ArrayList<>();
      row.add(rqueueMessage.getId());
      row.add(rqueueMessage.toString());
      if (timeQueue) {
        row.add(TimeUtils.millisToHumanRepresentation(rqueueMessage.getProcessAt() - currentTime));
      }
      if (!deadLetterQueue) {
        if (!deleted) {
          row.add(ActionType.DELETE);
        } else {
          row.add(Constants.BLANK);
        }
      }
      return row;
    }
  }

  static class ListRowBuilder implements RowBuilder {
    private final boolean deadLetterQueue;

    ListRowBuilder(boolean deadLetterQueue) {
      this.deadLetterQueue = deadLetterQueue;
    }

    @Override
    public List<Serializable> row(RqueueMessage rqueueMessage, boolean deleted) {
      List<Serializable> row = new ArrayList<>();
      row.add(rqueueMessage.getId());
      row.add(rqueueMessage.toString());
      if (!deadLetterQueue) {
        if (deleted) {
          row.add("");
        } else {
          row.add(ActionType.DELETE);
        }
      } else {
        row.add(TimeUtils.formatMilliToString(rqueueMessage.getReEnqueuedAt()));
      }
      return row;
    }
  }

  @Override
  public QueueExplorePageResponse getExplorePageData(
      String src, String name, DataType type, int pageNumber, int itemPerPage) {
    QueueMetaData metaData = getMeta(src);
    QueueExplorePageResponse response = new QueueExplorePageResponse();
    boolean deadLetterQueue = metaData.isDelayedQueue(name);
    boolean timeQueue = QueueUtils.isTimeQueue(name);
    setHeadersIfRequired(deadLetterQueue, timeQueue, response, pageNumber);

    if (deadLetterQueue) {
      response.setAction(ActionType.DELETE);
    } else {
      response.setAction(ActionType.NONE);
    }
    switch (type) {
      case ZSET:
        response.setRows(
            buildRows(
                readFromZset(name, pageNumber, itemPerPage),
                new ZsetRowBuilder(deadLetterQueue, timeQueue)));

        break;
      case LIST:
        response.setRows(
            buildRows(
                readFromList(name, pageNumber, itemPerPage), new ListRowBuilder(deadLetterQueue)));
        break;
      default:
        throw new UnknownSwitchCase(type.name());
    }
    return response;
  }

  private QueueExplorePageResponse responseForSet(String name) {
    List<Object> items = new ArrayList<>(stringRqueueRedisTemplate.getMembers(name));
    QueueExplorePageResponse response = new QueueExplorePageResponse();
    response.setHeaders(Collections.singletonList("Item"));
    response.setAction(ActionType.NONE);
    List<List<Serializable>> rows = new ArrayList<>();
    for (Object item : items) {
      rows.add(Collections.singletonList(item.toString()));
    }
    response.setRows(rows);
    return response;
  }

  private QueueExplorePageResponse responseForKeyVal(String name) {
    QueueExplorePageResponse response = new QueueExplorePageResponse();
    response.setHeaders(Collections.singletonList("Value"));
    response.setAction(ActionType.NONE);
    Object val = stringRqueueRedisTemplate.get(name);
    List<List<Serializable>> rows =
        Collections.singletonList(Collections.singletonList(String.valueOf(val)));
    response.setRows(rows);
    return response;
  }

  private QueueExplorePageResponse responseForZset(
      String name, String key, int pageNumber, int itemPerPage) {
    QueueExplorePageResponse response = new QueueExplorePageResponse();
    response.setAction(ActionType.NONE);
    int start = pageNumber * itemPerPage;
    int end = start + itemPerPage - 1;
    List<List<Serializable>> rows = new ArrayList<>();
    if (!StringUtils.isEmpty(key)) {
      Double score = stringRqueueRedisTemplate.getZsetMemberScore(name, key);
      response.setHeaders(Collections.singletonList("Score"));
      rows.add(Collections.singletonList(score));
    } else {
      response.setHeaders(Arrays.asList("Item", "Score"));
      for (TypedTuple<String> tuple : stringRqueueRedisTemplate.zrangeWithScore(name, start, end)) {
        rows.add(Arrays.asList(String.valueOf(tuple.getValue()), tuple.getScore()));
      }
    }
    response.setRows(rows);
    return response;
  }

  private QueueExplorePageResponse responseForList(String name, int pageNumber, int itemPerPage) {
    QueueExplorePageResponse response = new QueueExplorePageResponse();
    response.setHeaders(Collections.singletonList("Item"));
    response.setAction(ActionType.NONE);
    int start = pageNumber * itemPerPage;
    int end = start + itemPerPage - 1;
    List<List<Serializable>> rows = new ArrayList<>();
    for (Object s : stringRqueueRedisTemplate.lrange(name, start, end)) {
      List<Serializable> singletonList = Collections.singletonList(s.toString());
      rows.add(singletonList);
    }
    response.setRows(rows);
    return response;
  }

  @Override
  public QueueExplorePageResponse viewData(
      String name, DataType type, String key, int pageNumber, int itemPerPage) {
    if (StringUtils.isEmpty(name)) {
      return QueueExplorePageResponse.createErrorMessage("Data name cannot be empty.");
    }
    if (DataType.isUnknown(type)) {
      return QueueExplorePageResponse.createErrorMessage("Data type is not provided.");
    }
    switch (type) {
      case SET:
        return responseForSet(clean(name));
      case ZSET:
        return responseForZset(clean(name), clean(key), pageNumber, itemPerPage);
      case LIST:
        return responseForList(clean(name), pageNumber, itemPerPage);
      case KEY:
        return responseForKeyVal(clean(name));
      default:
        throw new UnknownSwitchCase(type.name());
    }
  }

  private void setHeadersIfRequired(
      boolean deadLetterQueue,
      boolean timeQueue,
      QueueExplorePageResponse response,
      int pageNumber) {
    if (pageNumber != 0) {
      return;
    }
    List<String> headers = new ArrayList<>();
    headers.add("Id");
    headers.add("Message");
    if (timeQueue) {
      headers.add("Time left");
    }
    if (!deadLetterQueue) {
      headers.add("Action");
    } else {
      headers.add("AddedOn");
    }
    response.setHeaders(headers);
  }
}
