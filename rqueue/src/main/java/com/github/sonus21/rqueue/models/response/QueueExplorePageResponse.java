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

package com.github.sonus21.rqueue.models.response;

import com.github.sonus21.rqueue.models.enums.ActionType;
import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class QueueExplorePageResponse extends BaseResponse {
  private static final long serialVersionUID = -8359552138158857044L;
  private List<String> headers;
  private ActionType action;
  private List<List<Serializable>> rows;

  public static QueueExplorePageResponse createErrorMessage(String message) {
    QueueExplorePageResponse response = new QueueExplorePageResponse();
    response.setCode(1);
    response.setMessage(message);
    return response;
  }
}
