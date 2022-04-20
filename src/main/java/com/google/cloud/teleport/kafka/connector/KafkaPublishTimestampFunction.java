/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.kafka.connector;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

/** An interface for providing custom timestamp for elements written to Kafka. */
public interface KafkaPublishTimestampFunction<T> extends Serializable {

  /**
   * Returns timestamp for element being published to Kafka. See @{@link
   * org.apache.kafka.clients.producer.ProducerRecord}.
   *
   * @param element The element being published.
   * @param elementTimestamp Timestamp of the element from the context (i.e. @{@link
   *     DoFn.ProcessContext#timestamp()}
   */
  Instant getTimestamp(T element, Instant elementTimestamp);

  /**
   * Returns {@link KafkaPublishTimestampFunction} returns element timestamp from ProcessContext.
   */
  static <T> KafkaPublishTimestampFunction<T> withElementTimestamp() {
    return (element, elementTimestamp) -> elementTimestamp;
  }
}
