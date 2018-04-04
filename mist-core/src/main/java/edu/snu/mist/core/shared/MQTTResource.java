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
package edu.snu.mist.core.shared;

import edu.snu.mist.core.sources.MQTTDataGenerator;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.IOException;

/**
 * The interface for class which manages MQTT resource.
 */
@DefaultImplementation(MQTTSharedResource.class)
public interface MQTTResource extends AutoCloseable {

  IMqttAsyncClient getMqttSinkClient(String brokerURI, String topic)
      throws MqttException, IOException;

  MQTTDataGenerator getDataGenerator(String brokerURI, String topic);

  void deleteMqttSinkClient(String brokerURI, String topic, IMqttAsyncClient client);
}
