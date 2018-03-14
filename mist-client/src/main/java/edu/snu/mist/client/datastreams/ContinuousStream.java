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
package edu.snu.mist.client.datastreams;

import edu.snu.mist.common.functions.*;
import edu.snu.mist.common.operators.CepEventPattern;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.WindowInformation;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Continuous Stream is a normal Stream used inside MIST. It emits one stream data (typed T) for one time.
 * It should be distinguished from WindowedStream.
 */
public interface ContinuousStream<T> extends MISTStream<T> {

  /**
   * @return the number of conditional branches diverged from this stream
   */
  int getCondBranchCount();

  /**
   * @return the branch index of this stream
   */
  int getBranchIndex();

  /**
   * Applies map operation to the current stream and creates a new stream.
   * @param mapFunc the function used for the transformation provided by a user
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> ContinuousStream<OUT> map(MISTFunction<T, OUT> mapFunc);

  /**
   * Applies flatMap operation to the current stream and creates a new stream.
   * @param flatMapFunc the function used for the transformation provided by a user
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> ContinuousStream<OUT> flatMap(MISTFunction<T, List<OUT>> flatMapFunc);

  /**
   * Applies filter operation to the current stream and creates a new stream.
   * @param filterFunc the function used for the transformation provided by a user
   * @return new transformed stream after applying the operation
   */
  ContinuousStream<T> filter(MISTPredicate<T> filterFunc);

  /**
   * Applies reduceByKey operation to the current stream.
   * @param keyFieldIndex the field index of key field
   * @param keyType the type of key. This parameter is used for type inference and dynamic type checking
   * @param reduceFunc function used for reduce operation
   * @param <K> the type of key in resulting stream
   * @param <V> the type of value in resulting stream
   * @return new transformed stream after applying the operation
   */
  <K, V> ContinuousStream<Map<K, V>> reduceByKey(
      int keyFieldIndex, Class<K> keyType, MISTBiFunction<V, V, V> reduceFunc);

  /**
   * Applies user-defined stateful operator to the current stream.
   * This stream will produce outputs on every stream input.
   * @param applyStatefulFunction the user-defined ApplyStatefulFunction
   * @param <OUT> the type of stream output
   * @return new transformed stream after applying the user-defined stateful operation
   */
  <OUT> ContinuousStream<OUT> applyStateful(ApplyStatefulFunction<T, OUT> applyStatefulFunction);

  /**
   * Applies state transition operator to the current stream.
   * @param initialState initial state
   * @param finalState set of final state
   * @param stateTable state table to transition state
   * @return new transformed stream after applying state transition operation
   */
  ContinuousStream<Tuple2<T, String>> stateTransition(
          final String initialState,
          final Set<String> finalState,
          final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable) throws IOException;

  /**
   * Applies cep operator to the current stream.
   * @param cepEventPatterns sequence of cep events
   * @param windowTime window time
   * @return new transformed stream after applying cep operation
   * The return value, map structure contains matched pattern of input events.
   * It consists of state name as key and input event list as value.
   */
  ContinuousStream<Map<String, List<T>>> cepOperator(final List<CepEventPattern<T>> cepEventPatterns,
                                                     final long windowTime) throws IOException;

  /**
   * Applies union operation to the current stream and input continuous stream passed as a parameter.
   * Both two streams for union should be continuous stream type.
   * @param inputStream the stream to be unified with this stream
   * @return new unified stream after applying type-checking
   */
  ContinuousStream<T> union(ContinuousStream<T> inputStream);

  /**
   * Creates a new WindowsStream according to the WindowInformation.
   * @param windowInfo the WindowInformation contains some information used during windowing operation
   * @return new windowed stream after applying the windowing operation
   */
  WindowedStream<T> window(WindowInformation windowInfo);

  /**
   * Joins current stream with the input stream.
   * Two streams are windowed according to the WindowInfo and joined within the window.
   * @param inputStream the stream to be joined with this stream
   * @param joinBiPredicate the function that decides to join a pair of inputs in two streams
   * @param windowInfo the windowing information for joining two streams
   * @param <U> the data type of the input stream to be joined with this stream
   * @return new windowed and joined stream
   */
  <U> WindowedStream<Tuple2<T, U>> join(ContinuousStream<U> inputStream,
                                        MISTBiPredicate<T, U> joinBiPredicate,
                                        WindowInformation windowInfo);

  /**
   * Branches out to a continuous stream with condition.
   * If an input data is matched with the condition, it will be routed only to the relevant downstream.
   * If many conditions in one branch point are matched, the earliest condition will be taken.
   * These branches can represent a control flow.
   * @param condition the predicate which test the branch condition
   * @return the new continuous stream which is branched out from this branch point
   */
  ContinuousStream<T> routeIf(MISTPredicate<T> condition);

  /**
   * Push the text stream to the socket server.
   * @param serverAddr socket server address
   * @param serverPort socket server port
   * @return sink stream
   */
  MISTStream<String> textSocketOutput(String serverAddr, int serverPort);

  /**
   * Publish the mqtt text stream to the mqtt broker.
   * @param brokerURI broker URI
   * @param topic topic
   * @return sink stream
   */
  MISTStream<MqttMessage> mqttOutput(String brokerURI, String topic);
}
