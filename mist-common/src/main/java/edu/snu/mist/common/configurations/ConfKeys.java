/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.common.configurations;

public final class ConfKeys {

  private ConfKeys() {

  }

  public enum SourceConfKeys {
    TIMESTAMP_EXTRACT_FUNC
  }

  public enum KafkaSourceConfKeys {
    IS_KAFKA_SOURCE,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_CONFIG
  }

  public enum NettySourceConfKeys {
    SOCKET_HOST_ADDR,
    SOCKET_HOST_PORT
  }

  public enum MQTTSourceConfKeys {
    MQTT_BROKER_URI,
    MQTT_TOPIC
  }
}
