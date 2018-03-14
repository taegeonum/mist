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
package edu.snu.mist.common.sources;

import org.eclipse.paho.client.mqttv3.*;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents MQTT clients implemented with eclipse Paho.
 * It will subscribe a MQTT broker and send the received data toward appropriate DataGenerator.
 */
public final class MQTTSubscribeClient implements MqttCallback {
  /**
   * A flag for start.
   */
  private final AtomicBoolean started;
  /**
   * The actual Paho MQTT client.
   */
  private IMqttAsyncClient client;
  /**
   * The URI of broker to connect.
   */
  private final String brokerURI;
  /**
   * The id of client.
   */
  private final String clientId;
  /**
   * The map coupling MQTT topic name and list of MQTTDataGenerators.
   */
  private final ConcurrentMap<String, Queue<MQTTDataGenerator>> dataGeneratorListMap;

  private final CountDownLatch countDownLatch;

  /**
   * Mqtt sink keep-alive time in seconds.
   */
  private final int mqttSourceKeepAliveSec;

  /**
   * Construct a client connected with target MQTT broker.
   * @param brokerURI the URI of broker to connect
   */
  public MQTTSubscribeClient(final String brokerURI,
                             final String clientId,
                             final int mqttSourceKeepAliveSec) {
    this.started = new AtomicBoolean(false);
    this.brokerURI = brokerURI;
    this.clientId = clientId;
    this.countDownLatch = new CountDownLatch(1);
    this.dataGeneratorListMap = new ConcurrentHashMap<>();
    this.mqttSourceKeepAliveSec = mqttSourceKeepAliveSec;
  }

  /**
   * Return the MQTTDataGenerator having MQTT client connected with the target broker.
   * When the start() method of the DataGenerator is called, the client will start to subscribe the requested topic.
   * If a DataGenerator having topic of connected broker is requested multiple-time,
   * already constructed DataGenerator will be returned.
   * @param topic the topic of connected broker to subscribe
   * @return requested MQTTDataGenerator connected with the target broker and topic
   */
  public MQTTDataGenerator connectToTopic(final String topic) {
    Queue<MQTTDataGenerator> dataGeneratorList = dataGeneratorListMap.get(topic);
    if (dataGeneratorList == null) {
      dataGeneratorList = new ConcurrentLinkedQueue<>();
      dataGeneratorListMap.put(topic, dataGeneratorList);
    }
    final MQTTDataGenerator dataGenerator = new MQTTDataGenerator(this, topic);
    dataGeneratorListMap.get(topic).add(dataGenerator);
    return dataGenerator;
  }

  /**
   * Start to subscribe a topic.
   */
  void subscribe(final String topic) throws MqttException {

    if (!started.get()) {
      if (started.compareAndSet(false, true)) {
        client = new MqttAsyncClient(brokerURI, clientId);
        final MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setKeepAliveInterval(mqttSourceKeepAliveSec);
        client.connect(mqttConnectOptions).waitForCompletion();
        client.setCallback(this);
        countDownLatch.countDown();
      }
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    client.subscribe(topic, 0);
  }

  /**
   * Unsubscribe a topic.
   */
  void unsubscribe(final String topic) throws MqttException {
    throw new UnsupportedOperationException("Unsubscribing is not supported now!");
  }

  /**
   * Close the connection between target MQTT broker.
   */
  public void disconnect() {
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
  }

  @Override
  public void connectionLost(final Throwable cause) {
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
  }

  @Override
  public void messageArrived(final String topic, final MqttMessage message) {
    final Queue<MQTTDataGenerator> dataGeneratorList = dataGeneratorListMap.get(topic);
    if (dataGeneratorList != null) {
      dataGeneratorList.forEach(dataGenerator -> dataGenerator.emitData(message));
    }
  }

  @Override
  public void deliveryComplete(final IMqttDeliveryToken token) {
    // do nothing because this class does not publish
  }
}
