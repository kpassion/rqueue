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

import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.request.MoveMessageRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import com.github.sonus21.rqueue.models.response.MoveMessageResponse;
import com.github.sonus21.rqueue.models.response.QueueExplorePageResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.web.service.RqueueDashboardChartService;
import com.github.sonus21.rqueue.web.service.RqueueDashboardUtilityService;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import com.github.sonus21.rqueue.web.service.RqueueQManagerService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController()
@RequestMapping("rqueue/api/v1")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class RqueueRestController {
  @NonNull private final RqueueDashboardChartService rqueueDashboardChartService;
  @NonNull private final RqueueQDetailService rqueueQDetailService;
  @NonNull private final RqueueDashboardUtilityService rqueueDashboardUtilityService;
  @NonNull private final RqueueQManagerService rqueueQManagerService;

  @PostMapping("chart")
  @ResponseBody
  public ChartDataResponse getDashboardData(@RequestBody ChartDataRequest chartDataRequest) {
    return rqueueDashboardChartService.getDashboardChartData(chartDataRequest);
  }

  @GetMapping("explore")
  @ResponseBody
  public QueueExplorePageResponse exploreQueue(
      @RequestParam DataType type,
      @RequestParam String name,
      @RequestParam String src,
      @RequestParam(defaultValue = "0", name = "page") int pageNumber,
      @RequestParam(defaultValue = "20", name = "count") int itemPerPage) {
    return rqueueQDetailService.getExplorePageData(src, name, type, pageNumber, itemPerPage);
  }

  @DeleteMapping("data-set/{queueName}")
  @ResponseBody
  public BooleanResponse deleteAll(@PathVariable String queueName) {
    return rqueueDashboardUtilityService.deleteQueueMessages(queueName, 0);
  }

  @DeleteMapping("data-set/{queueName}/{messageId}")
  @ResponseBody
  public BooleanResponse deleteMessage(
      @PathVariable String queueName, @PathVariable String messageId) {
    return rqueueDashboardUtilityService.deleteMessage(queueName, messageId);
  }

  @GetMapping("data-type")
  @ResponseBody
  public StringResponse dataType(@RequestParam String name) {
    return rqueueDashboardUtilityService.getDataType(name);
  }

  @PutMapping("move")
  @ResponseBody
  public MoveMessageResponse dataType(@RequestBody MoveMessageRequest request) {
    return rqueueDashboardUtilityService.moveMessage(request);
  }

  @GetMapping("data")
  @ResponseBody
  public QueueExplorePageResponse viewData(
      @RequestParam String name,
      @RequestParam DataType type,
      @RequestParam(required = false) String key,
      @RequestParam(defaultValue = "0", name = "page") int pageNumber,
      @RequestParam(defaultValue = "20", name = "count") int itemPerPage) {
    return rqueueQDetailService.viewData(name, type, key, pageNumber, itemPerPage);
  }

  @DeleteMapping("queues/{queueName}")
  @ResponseBody
  public BaseResponse deleteQueue(@PathVariable String queueName) {
    return rqueueQManagerService.deleteQueue(queueName);
  }
}
