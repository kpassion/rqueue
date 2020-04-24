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

package com.github.sonus21.rqueue.web.controller;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.models.db.QueueMetaData;
import com.github.sonus21.rqueue.models.db.TaskStatus;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.response.DataStructureMetaData;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.web.service.RqueueDashboardUtilityService;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.jtwig.spring.JtwigViewResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.View;

@Controller
@RequestMapping("rqueue")
@AllArgsConstructor(onConstructor = @__(@Autowired))
public class RqueueViewController {
  @Qualifier("rqueueViewResolver")
  private final JtwigViewResolver rqueueViewResolver;

  private final RqueueConfig rqueueConfig;
  private final RqueueQDetailService rqueueQDetailService;
  private final RqueueDashboardUtilityService rqueueDashboardUtilityService;

  private void addNavData(Model model, NavTab tab) {
    for (NavTab navTab : NavTab.values()) {
      String name = navTab.name().toLowerCase() + "Active";
      boolean active = tab == navTab;
      model.addAttribute(name, active);
    }
  }

  private void addBasicDetails(Model model) {
    model.addAttribute("latestVersion", rqueueDashboardUtilityService.getLatestVersion());
    model.addAttribute("mavenRepoLink", Constants.MAVEN_REPO_LINK);
    model.addAttribute(
        "time", OffsetDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    model.addAttribute("version", rqueueConfig.getVersion());
  }

  @GetMapping
  public View index(Model model) throws Exception {
    addBasicDetails(model);
    addNavData(model, null);
    model.addAttribute("title", "Rqueue Dashboard");
    model.addAttribute("aggregatorTypes", AggregationType.values());
    model.addAttribute("typeSelectors", TaskStatus.getActiveChartStatus());
    return rqueueViewResolver.resolveViewName("index", Locale.ENGLISH);
  }

  @GetMapping("queues")
  public View queues(Model model) throws Exception {
    addBasicDetails(model);
    addNavData(model, NavTab.QUEUES);
    model.addAttribute("title", "Queues");
    List<QueueMetaData> queueMetaData =
        rqueueQDetailService.getQueueMetadata().stream()
            .sorted(Comparator.comparing(QueueMetaData::getName))
            .collect(Collectors.toList());
    List<Entry<String, List<Entry<NavTab, DataStructureMetaData>>>> queueNameMetaData =
        new ArrayList<>(
            rqueueQDetailService.getQueueDataStructureDetails(queueMetaData).entrySet());
    queueNameMetaData.sort(Comparator.comparing(Entry::getKey));
    model.addAttribute("queues", queueMetaData);
    model.addAttribute("queueMetaData", queueNameMetaData);
    return rqueueViewResolver.resolveViewName("queues", Locale.ENGLISH);
  }

  @GetMapping("queues/{queueName}")
  public View queueDetail(Model model, @PathVariable String queueName) throws Exception {
    addBasicDetails(model);
    addNavData(model, NavTab.QUEUES);
    model.addAttribute("title", queueName);
    model.addAttribute("queueName", queueName);
    model.addAttribute("queueActive", true);
    model.addAttribute("aggregatorTypes", AggregationType.values());
    model.addAttribute("typeSelectors", TaskStatus.getActiveChartStatus());
    QueueMetaData metaData = rqueueQDetailService.getMeta(queueName);
    model.addAttribute("queueActions", rqueueQDetailService.getNavTabs(metaData));
    model.addAttribute(
        "queueMetaDataStructures", rqueueQDetailService.getQueueDataStructureDetails(metaData));
    model.addAttribute("meta", metaData);
    return rqueueViewResolver.resolveViewName("queue_detail", Locale.ENGLISH);
  }

  @GetMapping("running")
  public View running(Model model) throws Exception {
    addBasicDetails(model);
    addNavData(model, NavTab.RUNNING);
    model.addAttribute("title", "Running Tasks");
    List<List<Object>> l = rqueueDashboardUtilityService.getRunningTasks();
    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("scheduled")
  public View scheduled(Model model) throws Exception {
    addBasicDetails(model);
    addNavData(model, NavTab.SCHEDULED);
    model.addAttribute("title", "Scheduled Tasks");
    List<List<Object>> l = rqueueDashboardUtilityService.getScheduledTasks();
    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("dead")
  public View dead(Model model) throws Exception {
    addBasicDetails(model);
    addNavData(model, NavTab.DEAD);
    model.addAttribute("title", "Tasks moved to dead letter queue");
    List<List<Object>> l = rqueueDashboardUtilityService.getDeadLetterTasks();

    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("pending")
  public View pending(Model model) throws Exception {
    addBasicDetails(model);
    addNavData(model, NavTab.PENDING);
    model.addAttribute("title", "Tasks waiting for execution");
    List<List<Object>> l = rqueueDashboardUtilityService.getWaitingTasks();
    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("utility")
  public View utility(Model model) throws Exception {
    addBasicDetails(model);
    addNavData(model, NavTab.UTILITY);
    model.addAttribute("title", "Utility");
    model.addAttribute("supportedDataType", DataType.getEnabledDataTypes());
    return rqueueViewResolver.resolveViewName("utility", Locale.ENGLISH);
  }
}
