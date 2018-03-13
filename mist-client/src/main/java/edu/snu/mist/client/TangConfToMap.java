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
package edu.snu.mist.client;

import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.configurations.sources.SourceConfKeys;
import edu.snu.mist.common.parameters.SerializedTimestampExtractUdf;
import edu.snu.mist.common.sources.KafkaDataGenerator;
import edu.snu.mist.common.sources.NettyTextDataGenerator;
import edu.snu.mist.formats.avro.AvroVertexTypeEnum;
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.Node;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

public final class TangConfToMap {

  @Inject
  private TangConfToMap() {

  }

  public Map<String, String> convertTangConfToMap(final Configuration conf,
                                                  final AvroVertexTypeEnum type) {

    if (type == AvroVertexTypeEnum.SOURCE) {
      return convertSourceConfToMap(conf);
    } else if (type == AvroVertexTypeEnum.OPERATOR) {
      return convertOperatorConfToMap(conf);
    } else {
      return convertSinkConfToMap(conf);
    }
  }

  private Map<String, String> convertSourceConfToMap(final Configuration conf) {
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final ClassHierarchy classHierarchy = conf.getClassHierarchy();

    final Node kafkaNode = classHierarchy.getNode(KafkaDataGenerator.class.getName());
    final Node nettyNode = classHierarchy.getNode(NettyTextDataGenerator.class.getName());
    final Map<String, String> map = new HashMap<>();

    try {
      final String serializedTimestampExtractUdf = injector.getNamedInstance(SerializedTimestampExtractUdf.class);
      map.put(ConfKeys.SourceConfKeys.TIMESTAMP_EXTRACT_FUNC.name(), serializedTimestampExtractUdf);
    } catch (final InjectionException e) {
      // this is not used
    }

    if (conf.getBoundImplementation((ClassNode)kafkaNode) != null) {
      // kafka conf
      putKafkaSourceConf(conf, map);
    } else if (conf.getBoundImplementation((ClassNode)nettyNode) != null) {
      // netty conf
      map.put(SourceConfKeys.IS_NETTY_SOURCE, "1");
    } else {
      // mqtt conf
      map.put(SourceConfKeys.IS_MQTT_SOURCE, "1");
    }
  }

  private void putKafkaSourceConf(final Configuration conf,
                                  final Map<String, String> map) {
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final
    map.put(ConfKeys.KafkaSourceConfKeys.IS_KAFKA_SOURCE.name(), "1");
    map.put(ConfKeys.KafkaSourceConfKeys.KAFKA_TOPIC.name(), )
  }

  private void putNettySourceConf(final Configuration conf,
                                  final Map<String, String> map) {

  }

  private Map<String, String> convertOperatorConfToMap(final Configuration conf) {

  }

  private Map<String, String> convertSinkConfToMap(final Configuration conf) {

  }
}
