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

package com.github.sonus21.rqueue.models.request;

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode(callSuper = false)
public class MoveMessageRequest implements Serializable {
  private static final long serialVersionUID = -5105668034442269108L;
  private DataType srcType;
  private String src;
  private DataType dstType;
  private String dst;
  private Map<String, Serializable> others = new HashMap<>();

  public String validationMessage() {
    this.src = StringUtils.clean(src);
    if (StringUtils.isEmpty(src)) {
      return "Source cannot be empty.";
    }
    this.dst = StringUtils.clean(dst);
    if (StringUtils.isEmpty(src)) {
      return "Destination cannot be empty.";
    }
    if (src.equals(dst)) {
      return "Source and Destination cannot be same.";
    }
    if (DataType.isUnknown(srcType)) {
      return "Source data type cannot be unknown.";
    }
    if (DataType.isUnknown(dstType)) {
      return "Destination data type cannot be unknown.";
    }
    List<DataType> dataTypes = DataType.getEnabledDataTypes();
    if (!dataTypes.contains(srcType)) {
      return "Source data type is not supported.";
    }
    if (!dataTypes.contains(dstType)) {
      return "Destination data type is not supported.";
    }
    return null;
  }

  public int getMessageCount(RqueueWebConfig rqueueWebConfig) {
    if (others == null) {
      this.others = new HashMap<>();
      return rqueueWebConfig.getMaxMessageMoveCount();
    }
    Integer requestMessageCount = (Integer) others.get("maxMessages");
    if (requestMessageCount == null) {
      return rqueueWebConfig.getMaxMessageMoveCount();
    }
    return requestMessageCount;
  }
}
