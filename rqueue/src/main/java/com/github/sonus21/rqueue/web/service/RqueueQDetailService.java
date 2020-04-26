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

import com.github.sonus21.rqueue.models.db.QueueMetadata;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.response.DataStructureMetaData;
import com.github.sonus21.rqueue.models.response.QueueExplorePageResponse;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public interface RqueueQDetailService {

  List<QueueMetadata> getQueueMetadata();

  QueueMetadata getMeta(String queueName);

  Map<String, List<Entry<NavTab, DataStructureMetaData>>> getQueueDataStructureDetails(
      List<QueueMetadata> queueMetaData);

  List<Entry<NavTab, DataStructureMetaData>> getQueueDataStructureDetails(
      QueueMetadata queueMetaData);

  List<NavTab> getNavTabs(QueueMetadata queueMetaData);

  QueueExplorePageResponse getExplorePageData(
      String src, String name, DataType type, int pageNumber, int itemPerPage);

  QueueExplorePageResponse viewData(String name, DataType type, String key,
      int pageNumber,
      int itemPerPage);
}
