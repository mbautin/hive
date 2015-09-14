/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.generic;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestNumericHistogram {

  private void assertApproxEquals(double expected, double actual) {
    assertTrue(
        "Values are too far: expected=" + expected + ", actual=" + actual,
        Math.abs(expected - actual) < 1e-7);
  }

  @Test
  public void testInterpolation() {
    NumericHistogram h = new NumericHistogram();
    h.allocate(100);
    h.add(10.0);
    h.add(10.0);
    h.add(20.0);
    h.add(20.0);
    h.add(20.0);
    h.add(40.0);
    h.add(40.0);
    h.add(40.0);
    h.add(50.0);
    h.add(50.0);
    assertApproxEquals(10.0, h.quantile(1 / 18.0));
    assertApproxEquals(10.0, h.quantile(2 / 18.0));
    assertApproxEquals(15.0, h.quantile(3 / 18.0));
    assertApproxEquals(20.0, h.quantile(4 / 18.0));
    assertApproxEquals(20.0, h.quantile(5 / 18.0));
    assertApproxEquals(20.0, h.quantile(6 / 18.0));
    assertApproxEquals(20.0, h.quantile(7 / 18.0));
    assertApproxEquals(20.0, h.quantile(8 / 18.0));
    assertApproxEquals(30.0, h.quantile(9 / 18.0));
    assertApproxEquals(40.0, h.quantile(10 / 18.0));
    assertApproxEquals(40.0, h.quantile(11 / 18.0));
    assertApproxEquals(40.0, h.quantile(12 / 18.0));
    assertApproxEquals(40.0, h.quantile(13 / 18.0));
    assertApproxEquals(40.0, h.quantile(14 / 18.0));
    assertApproxEquals(45.0, h.quantile(15 / 18.0));
    assertApproxEquals(50.0, h.quantile(16 / 18.0));
    assertApproxEquals(50.0, h.quantile(17 / 18.0));
  }
}
