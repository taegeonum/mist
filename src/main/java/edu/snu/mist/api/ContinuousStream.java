/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.api;

import edu.snu.mist.api.operators.*;
import edu.snu.mist.api.window.WindowEmitPolicy;
import edu.snu.mist.api.window.WindowSizePolicy;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Continuous Stream is a normal Stream used inside MIST. It emits one stream data (typed T) for one time.
 * It should be distinguished from WindowedStream.
 */
public interface ContinuousStream<T> extends MISTStream<T> {
  /**
   * @return The type of this continuous stream (e.g. SourceStream, OperatorStream, ...)
   */
  StreamType.ContinuousType getContinuousType();

  /**
   * Applies map operation to the current stream and creates a new stream.
   * @param mapFunc the function used for the transformation provided by a user.
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> MapOperatorStream<T, OUT> map(Function<T, OUT> mapFunc);

  /**
   * Applies flatMap operation to the current stream and creates a new stream.
   * @param flatMapFunc the function used for the transformation provided by a user.
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> FlatMapOperatorStream<T, OUT> flatMap(Function<T, List<OUT>> flatMapFunc);

  /**
   * Applies filter operation to the current stream and creates a new stream.
   * @param filterFunc the function used for the transformation provided by a user.
   * @return new transformed stream after applying the operation
   */
  FilterOperatorStream<T> filter(Predicate<T> filterFunc);

  /**
   * Applies reduceByKey operation to the current stream.
   * @param keyFieldIndex the field index of key field
   * @param keyType the type of key. This parameter is used for type inference and dynamic type checking
   * @param reduceFunc function used for reduce operation
   * @param <K> the type of key in resulting stream
   * @param <V> the type of value in resulting stream
   * @return new transformed stream after applying the operation
   */
  <K, V> ReduceByKeyOperatorStream<T, K, V> reduceByKey(
      int keyFieldIndex, Class<K> keyType, BiFunction<V, V, V> reduceFunc);

  /**
   * Applies user-defined stateful operator to the current stream.
   * This stream will produce outputs on every stream input.
   * @param updateStateFunc the function which produces new state with the current state and the input
   * @param produceResultFunc the function which produces result with the updated state and the input
   * @param <S> the type of the operator state
   * @param <OUT> the type of stream output
   * @return new transformed stream after applying the user-defined stateful operation
   */
  <S, OUT> ApplyStatefulOperatorStream<T, OUT, S> applyStateful(BiFunction<T, S, S> updateStateFunc,
                                                                Function<S, OUT> produceResultFunc);

  /**
   * It creates a new WindowsStream according to the policy defined in windowSizePolicy and windowEmitPolicy.
   * @param windowSizePolicy The option which decides the size of the window
   * @param windowEmitPolicy The option which decides when to emit windowed stream
   * @return new windowed stream after applying the window operation
   */
  WindowedStream<T> window(WindowSizePolicy windowSizePolicy, WindowEmitPolicy windowEmitPolicy);
}