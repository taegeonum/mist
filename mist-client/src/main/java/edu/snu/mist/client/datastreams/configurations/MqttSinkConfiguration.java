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
package edu.snu.mist.client.datastreams.configurations;

import edu.snu.mist.common.parameters.MQTTBrokerURI;
import edu.snu.mist.common.parameters.MQTTTopic;
import edu.snu.mist.common.sinks.MqttSink;
import edu.snu.mist.common.sinks.Sink;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * The class represents the mqtt socket sink configuration.
 */
public final class MqttSinkConfiguration extends ConfigurationModuleBuilder {
  /**
   * The parameter for MQTT broker URI.
   */
  public static final RequiredParameter<String> MQTT_BROKER_URI = new RequiredParameter<>();

  /**
   * The parameter for MQTT topic name.
   */
  public static final RequiredParameter<String> MQTT_TOPIC = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new MqttSinkConfiguration()
      .bindNamedParameter(MQTTBrokerURI.class, MQTT_BROKER_URI)
      .bindNamedParameter(MQTTTopic.class, MQTT_TOPIC)
      .bindImplementation(Sink.class, MqttSink.class)
      .build();
}