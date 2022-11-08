/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public class SystemColumnSet {

  private final ImmutableMap<Class<? extends Table>, RelDataType> columnMapping;
  private final RelDataTypeFactory typeFactory;

  public SystemColumnSet(final ImmutableMap<Class<? extends Table>, RelDataType> columnMapping,
      RelDataTypeFactory typeFactory) {
    this.columnMapping = columnMapping;
    this.typeFactory = typeFactory;
  }

  /**
   * Returns a structured {@link RelDataType} containing the system columns for a given
   * implementation of {@link Table}
   */
  public RelDataType get(final Class<? extends Table> tableClazz) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Class<? extends Table> keyClazz : columnMapping.keySet()) {
      if (keyClazz.isAssignableFrom(tableClazz)) {
        RelDataType forTable = columnMapping.get(keyClazz);
        builder.addAll(forTable.getFieldList());
      }
    }
    return builder.build();
  }

  public static Builder builder(RelDataTypeFactory typeFactory) {
    return new Builder(typeFactory);
  }


  public static class Builder {

    private final Map<Class<? extends Table>, RelDataType> columnMapping = new HashMap<>();
    private final RelDataTypeFactory typeFactory;

    public Builder(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
    }

    public RelDataType put(final Class<? extends Table> key, final RelDataType value) {
      if (!value.isStruct()) {
        throw new IllegalArgumentException(
            "RelDataType for system columns must be a structured type");
      }
      columnMapping.put(key, value);
      return value;
    }

    public SystemColumnSet build() {
      return new SystemColumnSet(ImmutableMap.copyOf(columnMapping), typeFactory);
    }
  }
}
