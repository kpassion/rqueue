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

package com.github.sonus21.rqueue.listener;

import static com.github.sonus21.rqueue.utils.Constants.DELTA_BETWEEN_RE_ENQUEUE_TIME;
import static com.github.sonus21.rqueue.utils.Constants.SECONDS_IN_A_WEEK;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.metrics.RqueueCounter;
import com.github.sonus21.rqueue.models.db.MessageMetaData;
import com.github.sonus21.rqueue.models.db.TaskStatus;
import com.github.sonus21.rqueue.models.event.QueueTaskEvent;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.RedisUtils;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

@Slf4j
class MessageExecutor extends MessageContainerBase implements Runnable {
  private final QueueDetail queueDetail;
  private final Message<String> message;
  private final RqueueMessage rqueueMessage;
  private final String processingQueueName;
  private final String messageMetaDataId;
  private MessageMetaData messageMetaData;

  MessageExecutor(
      RqueueMessage message,
      QueueDetail queueDetail,
      WeakReference<RqueueMessageListenerContainer> container) {
    super(container);
    rqueueMessage = message;
    this.queueDetail = queueDetail;
    this.processingQueueName = QueueUtils.getProcessingQueueName(queueDetail.getQueueName());
    this.messageMetaDataId = QueueUtils.getMessageMetaDataKey(message.getId());
    this.message =
        new GenericMessage<>(
            message.getMessage(), QueueUtils.getQueueHeaders(queueDetail.getQueueName()));
  }

  private boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }

  private boolean isErrorEnabled() {
    return log.isErrorEnabled();
  }

  private boolean isWarningEnabled() {
    return log.isWarnEnabled();
  }

  private int getMaxRetryCount() {
    return rqueueMessage.getRetryCount() == null
        ? queueDetail.getNumRetries()
        : rqueueMessage.getRetryCount();
  }

  private Object getPayload() {
    return MessageUtils.convertMessageToObject(message, getMessageConverters());
  }

  private void callMessageProcessor(TaskStatus status, RqueueMessage rqueueMessage) {
    MessageProcessor messageProcessor = null;
    switch (status) {
      case DELETED:
        messageProcessor =
            Objects.requireNonNull(container.get()).getManualDeletionMessageProcessor();
        break;
      case MOVED_TO_DLQ:
        messageProcessor =
            Objects.requireNonNull(container.get()).getDeadLetterQueueMessageProcessor();
        break;
      case DISCARDED:
        messageProcessor = Objects.requireNonNull(container.get()).getDiscardMessageProcessor();
        break;
      case SUCCESSFUL:
        messageProcessor =
            Objects.requireNonNull(container.get()).getPostExecutionMessageProcessor();
        break;
      default:
        break;
    }
    if (messageProcessor != null) {
      try {
        log.debug("Calling {} processor for {}", status, rqueueMessage);
        Object payload = getPayload();
        messageProcessor.process(payload);
      } catch (Exception e) {
        log.error("Message processor {} call failed", status, e);
      }
    }
  }

  @SuppressWarnings("ConstantConditions")
  private void updateCounter(boolean failOrExecution) {
    RqueueCounter rqueueCounter = container.get().rqueueCounter;
    if (rqueueCounter == null) {
      return;
    }
    if (failOrExecution) {
      rqueueCounter.updateFailureCount(queueDetail.getQueueName());
    } else {
      rqueueCounter.updateExecutionCount(queueDetail.getQueueName());
    }
  }

  private void publishEvent(TaskStatus status, long jobExecutionStartTime) {
    if (Objects.requireNonNull(container.get()).rqueueWebConfig.isCollectListenerStats()) {
      addOrDeleteMetadata(jobExecutionStartTime, false);
      QueueTaskEvent event =
          new QueueTaskEvent(queueDetail.getQueueName(), status, rqueueMessage, messageMetaData);
      Objects.requireNonNull(container.get()).applicationEventPublisher.publishEvent(event);
    }
  }

  private void addOrDeleteMetadata(long jobExecutionTime, boolean saveOrDelete) {
    if (messageMetaData == null) {
      messageMetaData =
          Objects.requireNonNull(container.get())
              .getMessageMetaDataService()
              .get(messageMetaDataId);
    }
    if (messageMetaData == null) {
      messageMetaData = new MessageMetaData(messageMetaDataId, rqueueMessage.getId());
    }
    messageMetaData.addExecutionTime(jobExecutionTime);
    if (saveOrDelete) {
      Objects.requireNonNull(container.get())
          .getMessageMetaDataService()
          .save(messageMetaData, Duration.ofSeconds(SECONDS_IN_A_WEEK));
    } else {
      Objects.requireNonNull(container.get()).getMessageMetaDataService().delete(messageMetaDataId);
    }
  }

  private void deleteMessage(
      TaskStatus status, int currentFailureCount, long jobExecutionStartTime) {
    getRqueueMessageTemplate().removeElementFromZset(processingQueueName, rqueueMessage);
    rqueueMessage.setFailureCount(currentFailureCount);
    callMessageProcessor(status, rqueueMessage);
    publishEvent(status, jobExecutionStartTime);
  }

  private void moveMessageToDlq(int currentFailureCount, long jobExecutionStartTime)
      throws CloneNotSupportedException {
    if (isWarningEnabled()) {
      log.warn(
          "Message {} Moved to dead letter queue: {}, dead letter queue: {}",
          getPayload(),
          queueDetail.getQueueName(),
          queueDetail.getDeadLetterQueueName());
    }
    RqueueMessage newMessage = rqueueMessage.clone();
    newMessage.setFailureCount(currentFailureCount);
    newMessage.updateReEnqueuedAt();
    callMessageProcessor(TaskStatus.MOVED_TO_DLQ, newMessage);
    RedisUtils.executePipeLine(
        getRqueueMessageTemplate().getTemplate(),
        (connection, keySerializer, valueSerializer) -> {
          byte[] newMessageBytes = valueSerializer.serialize(newMessage);
          byte[] oldMessageBytes = valueSerializer.serialize(rqueueMessage);
          byte[] processingQueueNameBytes = keySerializer.serialize(processingQueueName);
          byte[] dlqNameBytes = keySerializer.serialize(queueDetail.getDeadLetterQueueName());
          connection.rPush(dlqNameBytes, newMessageBytes);
          connection.zRem(processingQueueNameBytes, oldMessageBytes);
        });
    publishEvent(TaskStatus.MOVED_TO_DLQ, jobExecutionStartTime);
  }

  private void parkMessageForRetry(int currentFailureCount, long jobExecutionStartTime)
      throws CloneNotSupportedException {
    if (isDebugEnabled()) {
      log.debug(
          "Message {} will be retried later, queue: {}, processing queue: {}",
          getPayload(),
          queueDetail.getQueueName(),
          processingQueueName);
    }
    RqueueMessage newMessage = rqueueMessage.clone();
    newMessage.setFailureCount(currentFailureCount);
    newMessage.updateReEnqueuedAt();
    getRqueueMessageTemplate().replaceMessage(processingQueueName, rqueueMessage, newMessage);
    addOrDeleteMetadata(jobExecutionStartTime, true);
  }

  private void discardMessage(int currentFailureCount, long jobExecutionStartTime) {
    if (isErrorEnabled()) {
      log.warn(
          "Message {} discarded due to retry limit exhaust queue: {}",
          getPayload(),
          queueDetail.getQueueName());
    }
    deleteMessage(TaskStatus.DISCARDED, currentFailureCount, jobExecutionStartTime);
  }

  private void handleManualDeletion(int currentFailureCount, long jobExecutionStartTime) {
    if (isWarningEnabled()) {
      log.info(
          "Message Deleted manually {} successfully, Queue: {} ",
          rqueueMessage,
          queueDetail.getQueueName());
    }
    deleteMessage(TaskStatus.DELETED, currentFailureCount, jobExecutionStartTime);
  }

  private void taskExecutedSuccessfully(int currentFailureCount, long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log.debug(
          "Message consumed {} successfully, Queue: {} ",
          rqueueMessage,
          queueDetail.getQueueName());
    }
    deleteMessage(TaskStatus.SUCCESSFUL, currentFailureCount, jobExecutionStartTime);
  }

  private void handlePostProcessing(
      boolean executed,
      boolean deleted,
      int currentFailureCount,
      int maxRetryCount,
      long jobExecutionStartTime) {
    if (!isQueueActive(queueDetail.getQueueName())) {
      return;
    }
    try {
      if (deleted) {
        handleManualDeletion(currentFailureCount, jobExecutionStartTime);
      } else {
        if (!executed) {
          if (queueDetail.isDlqSet()) {
            moveMessageToDlq(currentFailureCount, jobExecutionStartTime);
          } else if (currentFailureCount < maxRetryCount) {
            parkMessageForRetry(currentFailureCount, jobExecutionStartTime);
          } else {
            discardMessage(currentFailureCount, jobExecutionStartTime);
          }
        } else {
          taskExecutedSuccessfully(currentFailureCount, jobExecutionStartTime);
        }
      }
    } catch (Exception e) {
      log.error("Error occurred in post processing", e);
    }
  }

  private long getMaxProcessingTime() {
    return System.currentTimeMillis()
        + queueDetail.getVisibilityTimeout()
        - DELTA_BETWEEN_RE_ENQUEUE_TIME;
  }

  private boolean isMessageDeleted(String id) {
    notNull(id, "Message id must be present");
    messageMetaData =
        Objects.requireNonNull(container.get()).getMessageMetaDataService().get(messageMetaDataId);
    if (messageMetaData == null) {
      return false;
    }
    return messageMetaData.isDeleted();
  }

  @Override
  public void run() {
    boolean executed = false;
    int currentFailureCount = rqueueMessage.getFailureCount();
    int maxRetryCount = getMaxRetryCount();
    long maxRetryTime = getMaxProcessingTime();
    long startTime = System.currentTimeMillis();
    boolean deleted = false;
    do {
      if (!isQueueActive(queueDetail.getQueueName())) {
        return;
      }
      if (isMessageDeleted(rqueueMessage.getId())) {
        deleted = true;
        break;
      }
      try {
        updateCounter(false);
        getMessageHandler().handleMessage(message);
        executed = true;
      } catch (Exception e) {
        updateCounter(true);
        currentFailureCount += 1;
      }
    } while (currentFailureCount < maxRetryCount
        && !executed
        && System.currentTimeMillis() < maxRetryTime);
    handlePostProcessing(executed, deleted, currentFailureCount, maxRetryCount, startTime);
  }
}
