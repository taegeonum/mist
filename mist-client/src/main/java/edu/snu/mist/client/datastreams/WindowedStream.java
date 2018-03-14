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

import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.windows.WindowData;

import java.util.Map;

/**
 * Windowed stream interface created by window methods.
 * It emits a WindowData that contains a collection of data, the window's start and end information.
 * It should be distinguished from ContinuousStream.
 */
public interface WindowedStream<T> extends MISTStream<WindowData<T>> {

  /**
   * It reduces the windowed stream by an user-designated key.
   * @param keyFieldNum key index
   * @param keyType the type of the key
   * @param reduceFunc the reduce function
   * @param <K> the type of key in resulting stream
   * @param <V> the type of value in resulting stream
   * @return new reduced continuous stream after applying the operation
   */
  <K, V> ContinuousStream<Map<K, V>> reduceByKeyWindow(
      int keyFieldNum, Class<K> keyType, MISTBiFunction<V, V, V> reduceFunc);

  /**
   * It aggregates the windowed stream by an user-defined aggregation function.
   * @param aggregateFunc the function that aggregates input WindowData
   * @param <R> the type of result
   * @return new aggregated continuous stream after applying the aggregation function
   */
  <R> ContinuousStream<R> aggregateWindow(MISTFunction<WindowData<T>, R> aggregateFunc);

  /**
   * It applies an user-defined stateful operation to the collection of data received from upstream window operator.
   * @param applyStatefulFunction the user-defined ApplyStatefulFunction
   * @param <R> the type of result
   * @return new aggregated continuous stream after applying the stateful operation
   */
  <R> ContinuousStream<R> applyStatefulWindow(ApplyStatefulFunction<T, R> applyStatefulFunction);
}