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

package com.github.sonus21.rqueue.example;

import static com.github.sonus21.rqueue.utils.Constants.ONE_MILLI;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeUtils;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageListener {
  private Random random = new Random();

  @Value("${delay.queue.fail.percentage:0}")
  private int percentageFailure;

  private boolean shouldFail() {
    if (percentageFailure == 0) {
      return false;
    }
    if (percentageFailure >= 100) {
      return true;
    }
    return random.nextInt(100) < percentageFailure;
  }

  @RqueueListener(value = "${rqueue.simple.queue}", active = "false")
  public void consumeMessage(String message) {
    log.info("simple: {}, {}", message, message.getClass());
  }

  @RqueueListener(
      value = {"${rqueue.delay.queue}", "${rqueue.delay2.queue}"},
      delayedQueue = "${rqueue.delay.queue.delayed-queue}",
      numRetries = "${rqueue.delay.queue.retries}",
      visibilityTimeout = "60*60*1000")
  public void onMessage(String message) {
    log.info("delay: {}", message);
    if (shouldFail()) {
      TimeUtils.sleep(Constants.SECONDS_IN_A_MINUTE * ONE_MILLI);
      log.error("delay {}", message);
      throw new NullPointerException("Failing On Purpose " + message);
    }
  }

  @RqueueListener(
      value = "job-queue",
      delayedQueue = "true",
      deadLetterQueue = "job-morgue",
      numRetries = "2")
  public void onMessage(Job job) throws Exception {
    if (shouldFail()) {
      log.error("job-queue: {}", job);
      throw new Exception("OMG!");
    }
    log.info("job-queue: {}", job);
  }
}
