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

import static com.github.sonus21.rqueue.utils.QueueUtils.getLockKey;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.models.aggregator.QueueEvents;
import com.github.sonus21.rqueue.models.aggregator.TasksStat;
import com.github.sonus21.rqueue.models.db.MessageMetaData;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import com.github.sonus21.rqueue.models.event.QueueTaskEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.github.sonus21.rqueue.utils.TimeUtils;
import com.github.sonus21.rqueue.web.dao.RqueueQStatsDao;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
public class RqueueTaskAggregatorService
    implements ApplicationListener<QueueTaskEvent>, DisposableBean, SmartLifecycle {
  @NonNull private final RqueueWebConfig rqueueWebConfig;
  @NonNull private final RqueueLockManager rqueueLockManager;
  @NonNull private final RqueueQStatsDao rqueueQStatsDao;
  private final Object lifecycleMgr = new Object();
  private boolean running = false;
  private boolean stopped = false;
  private ThreadPoolTaskScheduler taskExecutor;
  private Map<String, QueueEvents> queueNameToEvents;
  private BlockingQueue<QueueEvents> queue;

  @Override
  public void destroy() throws Exception {
    stop();
    if (this.taskExecutor != null) {
      this.taskExecutor.destroy();
    }
  }

  @Override
  public void start() {
    synchronized (lifecycleMgr) {
      running = true;
      if (!rqueueWebConfig.isCollectListenerStats()) {
        return;
      }
      this.queueNameToEvents = new ConcurrentHashMap<>();
      this.queue = new LinkedBlockingDeque<>();
      int threadCount = rqueueWebConfig.getStatsAggregatorThreadCount();
      this.taskExecutor = ThreadUtils.createTaskScheduler(threadCount, "RqueueTaskAggregator-", 30);
      for (int i = 0; i < threadCount; i++) {
        this.taskExecutor.submit(new EventAggregator());
      }
      lifecycleMgr.notifyAll();
    }
  }

  @Override
  public void stop() {
    synchronized (lifecycleMgr) {
      if (this.taskExecutor != null) {
        this.taskExecutor.shutdown();
      }
      stopped = true;
      lifecycleMgr.notifyAll();
    }
  }

  @Override
  public boolean isRunning() {
    synchronized (lifecycleMgr) {
      return this.running;
    }
  }

  @Override
  public void onApplicationEvent(QueueTaskEvent event) {
    synchronized (event.getSource()) {
      if (log.isTraceEnabled()) {
        log.trace("Event {}", event);
      }
      String queueName = (String) event.getSource();
      QueueEvents queueEvents = queueNameToEvents.get(queueName);
      if (queueEvents == null) {
        queueEvents = new QueueEvents(event);
      } else {
        queueEvents.addEvent(event);
      }
      if (queueEvents.processingRequired(
          rqueueWebConfig.getAggregateEventWaitTime(),
          rqueueWebConfig.getAggregateEventCount())) {
        if (log.isTraceEnabled()) {
          log.trace("Adding events to the queue");
        }
        queue.add(queueEvents);
        queueNameToEvents.remove(queueName);
      } else {
        queueNameToEvents.put(queueName, queueEvents);
      }
    }
  }

  private class EventAggregator implements Runnable {
    private void aggregate(QueueTaskEvent event, TasksStat stat) {
      switch (event.getStatus()) {
        case DISCARDED:
          stat.discarded += 1;
          break;
        case SUCCESSFUL:
          stat.success += 1;
          break;
        case MOVED_TO_DLQ:
          stat.movedToDlq += 1;
          break;
      }
      RqueueMessage rqueueMessage = event.getRqueueMessage();
      MessageMetaData messageMetaData = event.getMessageMetaData();
      if (rqueueMessage.getFailureCount() != 0) {
        stat.retried += 1;
      }
      stat.minExecution = Math.min(stat.minExecution, messageMetaData.getTotalExecutionTime());
      stat.maxExecution = Math.max(stat.maxExecution, messageMetaData.getTotalExecutionTime());
      stat.jobCount += 1;
      stat.totalExecutionTime += messageMetaData.getTotalExecutionTime();
    }

    private void aggregate(QueueEvents events) {
      List<QueueTaskEvent> queueTaskEvents = events.taskEvents;
      QueueTaskEvent queueTaskEvent = queueTaskEvents.get(0);
      LocalDate date = queueTaskEvent.getLocalDate();
      Map<LocalDate, TasksStat> localDateTasksStatMap = new HashMap<>();
      TasksStat stat = new TasksStat();
      for (QueueTaskEvent event : queueTaskEvents) {
        if (!date.equals(event.getLocalDate())) {
          stat = localDateTasksStatMap.getOrDefault(event.getLocalDate(), new TasksStat());
        }
        aggregate(event, stat);
        localDateTasksStatMap.put(event.getLocalDate(), stat);
      }
      String queueName = (String) queueTaskEvent.getSource();
      String queueStatKey = QueueUtils.getQueueStatKey(queueName);
      QueueStatistics queueStatistics = rqueueQStatsDao.findById(queueStatKey);
      if (queueStatistics == null) {
        queueStatistics = new QueueStatistics(queueStatKey);
      }
      LocalDate today = LocalDate.now(ZoneOffset.UTC);
      queueStatistics.updateTime();
      for (Entry<LocalDate, TasksStat> entry : localDateTasksStatMap.entrySet()) {
        queueStatistics.update(stat, entry.getKey().toString());
      }
      queueStatistics.pruneStats(today, rqueueWebConfig.getHistoryDay());
      rqueueQStatsDao.save(queueStatistics);
    }

    private void processEvents(QueueEvents events) {
      List<QueueTaskEvent> queueTaskEvents = events.taskEvents;
      if (!CollectionUtils.isEmpty(queueTaskEvents)) {
        QueueTaskEvent queueTaskEvent = queueTaskEvents.get(0);
        String queueName = (String) queueTaskEvent.getSource();
        String queueStatKey = QueueUtils.getQueueStatKey(queueName);
        String lockKey = getLockKey(queueStatKey);
        if (rqueueLockManager.acquireLock(
            lockKey, Duration.ofSeconds(Constants.AGGREGATION_LOCK_DURATION_IN_SECONDS))) {
          aggregate(events);
          rqueueLockManager.releaseLock(lockKey);
        } else {
          log.warn("Unable to acquire lock, will retry later");
          queue.add(events);
        }
      }
    }

    @Override
    public void run() {
      QueueEvents events;
      while (!stopped) {
        try {
          if (log.isTraceEnabled()) {
            log.trace("Aggregating queue stats");
          }
          events = queue.poll();
          if (events == null) {
            TimeUtils.sleepLog(Constants.MIN_DELAY, false);
          } else {
            processEvents(events);
          }
        } catch (Exception e) {
          log.error("Error in aggregator job ", e);
          TimeUtils.sleepLog(Constants.MIN_DELAY, false);
        }
      }
    }
  }
}
