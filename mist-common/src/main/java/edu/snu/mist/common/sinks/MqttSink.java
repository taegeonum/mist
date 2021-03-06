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
package edu.snu.mist.common.sinks;

import edu.snu.mist.common.parameters.MQTTBrokerURI;
import edu.snu.mist.common.parameters.MQTTTopic;
import edu.snu.mist.common.shared.MQTTResource;
import org.apache.reef.tang.annotations.Parameter;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.inject.Inject;
import java.io.IOException;

/**
 * This class publishes MQTT messages to MQTT broker.
 */
public final class MqttSink implements Sink<MqttMessage> {

  /**
   * MQTT publisher client.
   */
  private final IMqttAsyncClient mqttClient;

  /**
   * MQTT topic.
   */
  private final String topic;

  @Inject
  public MqttSink(
      @Parameter(MQTTBrokerURI.class) final String brokerURI,
      @Parameter(MQTTTopic.class) final String topic,
      final MQTTResource sharedResource) throws IOException, MqttException {
    this.topic = topic;
    this.mqttClient = sharedResource.getMqttSinkClient(brokerURI, topic);
  }

  @Override
  public void close() throws Exception {
    // TODO:[MIST-494] Safely close MQTT publisher client.
  }

  @Override
  public void handle(final MqttMessage input) {
    try {
      mqttClient.publish(topic, input);
    } catch (MqttException e) {
      e.printStackTrace();
    }
  }
}
