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
package org.apache.calcite.test;

import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SystemColumnSet;

import org.junit.jupiter.api.Test;

public class SqlValidatorSystemColumnsTest extends SqlValidatorTestCase {

  @Override public SqlValidatorFixture fixture() {
    return super.fixture()
        .withConformance(SqlConformanceEnum.BIG_QUERY)
        .withFactory(tf ->
            tf.withTypeSystem(ts ->
                // Provide a TypeSystem implementation setting some SystemColumns
               new DelegatingTypeSystem(ts) {
                 @Override public SystemColumnSet deriveSystemColumnSet(
                     final SystemColumnSet systemColumnSet,
                     final RelDataTypeFactory typeFactory) {

                   SystemColumnSet.Builder builder = SystemColumnSet.builder(typeFactory);

                   RelDataType type = typeFactory.builder()
                       .add("_PARTITIONTIME", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
                       .build();

                   builder.put(ModifiableTable.class, type);

                   return builder.build();
                 }
               }
            )
        );
  }

  @Test public void testImplicitSystemColumn() {
    sql("select EMPNO from \"SALES\".EMP where _PARTITIONTIME = '2020-11-03'")
        .ok();
  }

  @Test public void testStarExpandsWithoutSystemColumn(){
    String expected = "RecordType(INTEGER NOT NULL EMPNO, VARCHAR(20) NOT NULL ENAME, "
        + "VARCHAR(10) NOT NULL JOB, INTEGER MGR, TIMESTAMP(0) NOT NULL HIREDATE, "
        + "INTEGER NOT NULL SAL, INTEGER NOT NULL COMM, "
        + "INTEGER NOT NULL DEPTNO, "
        + "BOOLEAN NOT NULL SLACKER) NOT NULL";
    sql("select * from \"SALES\".EMP where _PARTITIONTIME = '2020-11-03'")
        .type(expected).ok();
  }
}
