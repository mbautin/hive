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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@WindowFunctionDescription(
  description = @Description(
    name = "exact_lag",
    value = "_FUNC_(value, order_by_value, lag_amount)"
  ),
  supportsWindow = true,
  pivotResult = false,
  impliesOrder = true
)
public class GenericUDAFExactLag extends AbstractGenericUDAFResolver
{
  static final Log LOG = LogFactory.getLog(GenericUDAFExactLag.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException
  {
    if (parameters.length != 3)
    {
      throw new UDFArgumentTypeException(3,
        "Three arguments expected: value, ORDER BY value, and lag amount of the ORDER BY value, " +
        "got " + parameters.length + " arguments");
    }
    return createEvaluator();
  }

  protected GenericUDAFExactLagEvaluator createEvaluator()
  {
    return new GenericUDAFExactLagEvaluator();
  }

  static class ExactLagBuffer implements AggregationBuffer
  {
    Object val;
    Long orderByVal;
    Long lastOrderByVal;
    boolean valSet;
    boolean firstRow;
    int lagAmount;

    ExactLagBuffer()
    {
      init();
    }

    void init()
    {
      val = null;
      valSet = false;
      firstRow = true;
    }

  }

  public static class GenericUDAFExactLagEvaluator extends GenericUDAFEvaluator
  {
    ObjectInspector inputOI;
    ObjectInspector outputOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException
    {
      super.init(m, parameters);
      if (m != Mode.COMPLETE)
      {
        throw new HiveException(
            "Only COMPLETE mode supported for Rank function");
      }
      inputOI = parameters[0];
      outputOI = ObjectInspectorUtils.getStandardObjectInspector(
        inputOI, ObjectInspectorCopyOption.WRITABLE);
      return outputOI;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException
    {
      return new ExactLagBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException
    {
      ((ExactLagBuffer) agg).init();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException
    {
      ExactLagBuffer b = (ExactLagBuffer) agg;

      if (b.firstRow)
      {
        b.firstRow = false;
        b.lagAmount = PrimitiveObjectInspectorUtils.getInt(
          parameters[2],
          PrimitiveObjectInspectorFactory.writableIntObjectInspector);
      }

      if (!b.valSet)
      {
        b.val = ObjectInspectorUtils.copyToStandardObject(
          parameters[0], inputOI, ObjectInspectorCopyOption.WRITABLE);

        if (parameters[1] == null) {
          b.orderByVal = null;
        } else {
          b.orderByVal = (Long) parameters[1];
        }

        b.valSet = true;
      }

      if (parameters[1] == null) {
        b.lastOrderByVal = null;
      } else {
        b.lastOrderByVal = (Long) parameters[1];
      }

    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException
    {
      throw new HiveException("terminatePartial not supported");
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException
    {
      throw new HiveException("merge not supported");
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException
    {
      ExactLagBuffer b = (ExactLagBuffer) agg;
      if (b.lastOrderByVal - b.orderByVal == b.lagAmount) {
        return b.val;
      } else {
        return null;
      }
    }

  }
}

