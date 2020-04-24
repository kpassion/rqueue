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

package com.github.sonus21.rqueue.utils;

import com.github.sonus21.rqueue.exception.TimedOutException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.BooleanSupplier;

/**
 * WaitForUtil method wait for some event to occur. It accepts a callback as well that can be
 * invoked on successive failure of the method. That can be used to diagnosis the test. A callback
 * method is used to identify the outcome of some event, if the specified method returns true then
 * call is stopped otherwise every 100Ms the callback would be called to get the outcome. It tries
 * for continuously over 10seconds If callback does not return true within 10 seconds then it will
 * throw TimeOutException. If postmortem method is provided then it will call that method before
 * throwing exception.
 */
public class TimeUtils {
  private static DateFormat simple = new SimpleDateFormat("dd MMM yyyy HH:mm");

  private TimeUtils() {}

  public static void waitFor(
      BooleanSupplier callback, long waitTimeInMilliSeconds, String description)
      throws TimedOutException {
    waitFor(callback, waitTimeInMilliSeconds, description, () -> {});
  }

  public static void waitFor(BooleanSupplier callback, String description)
      throws TimedOutException {
    waitFor(callback, 10000L, description);
  }

  public static void waitFor(BooleanSupplier callback, String description, Runnable postmortem)
      throws TimedOutException {
    waitFor(callback, 10000L, description, postmortem);
  }

  public static void waitFor(
      BooleanSupplier callback,
      long waitTimeInMilliSeconds,
      String description,
      Runnable postmortem)
      throws TimedOutException {
    long endTime = System.currentTimeMillis() + waitTimeInMilliSeconds;
    do {
      if (Boolean.TRUE.equals(callback.getAsBoolean())) {
        return;
      }
      sleep(100L);
    } while (System.currentTimeMillis() < endTime);
    try {
      postmortem.run();
    } catch (Exception e) {
      e.printStackTrace();
    }
    throw new TimedOutException("Timed out waiting for " + description);
  }

  public static void sleep(long time) {
    sleepLog(time, true);
  }

  public static void sleepLog(long time, boolean log) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (log) {
        e.printStackTrace();
      }
    }
  }

  public static String millisToHumanRepresentation(long milli) {
    long seconds = milli / 1000;
    long minutes = seconds / Constants.SECONDS_IN_A_MINUTE;
    seconds = seconds % Constants.SECONDS_IN_A_MINUTE; // remaining seconds
    long hours = minutes / Constants.MINUTES_IN_AN_HOUR;
    minutes = minutes % Constants.MINUTES_IN_AN_HOUR; // remaining minutes
    long days = hours / Constants.HOURS_IN_A_DAY;
    hours = hours % Constants.HOURS_IN_A_DAY; // remaining hours
    String s;
    if (days != 0) {
      s = days + " Day, " + hours + " Hours, " + minutes + " Minutes, " + seconds + " Seconds.";
    } else {
      if (hours != 0) {
        s = hours + " Hours, " + minutes + " Minutes, " + seconds + " Seconds.";
      } else if (minutes != 0) {
        s = minutes + " Minutes, " + seconds + " Seconds.";
      } else {
        s = seconds + " Seconds.";
      }
    }
    return s;
  }

  public static String formatMilliToString(Long milli) {
    if (milli == null) {
      return "";
    }
    Date time = new Date(milli);
    return simple.format(time);
  }
}
