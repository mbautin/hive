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
    name = "exact_row_lag",
    value = "_FUNC_(value, lag_amount)"
  ),
  supportsWindow = true,
  pivotResult = false,
  impliesOrder = false
)
public class GenericUDAFExactRowLag extends AbstractGenericUDAFResolver
{
  static final Log LOG = LogFactory.getLog(GenericUDAFExactRowLag.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException
  {
    if (parameters.length != 2)
    {
      throw new UDFArgumentTypeException(3,
        "Two arguments expected: value, lag_amount, " +
        "got " + parameters.length + " arguments");
    }
    return createEvaluator();
  }

  protected GenericUDAFExactRowLagEvaluator createEvaluator()
  {
    return new GenericUDAFExactRowLagEvaluator();
  }

  static class ExactRowLagBuffer implements AggregationBuffer
  {
    Object val;
    boolean valSet;
    boolean firstRow;
    long rowsSeen;
    int lagAmount;

    ExactRowLagBuffer()
    {
      init();
    }

    void init()
    {
      val = null;
      valSet = false;
      firstRow = true;
      rowsSeen = 0;
      lagAmount = 0;
    }

  }

  public static class GenericUDAFExactRowLagEvaluator extends GenericUDAFEvaluator
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
          "Only COMPLETE mode supported for the exact_row_lag function");
      }
      inputOI = parameters[0];
      outputOI = ObjectInspectorUtils.getStandardObjectInspector(
        inputOI, ObjectInspectorCopyOption.WRITABLE);
      return outputOI;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException
    {
      return new ExactRowLagBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException
    {
      ((ExactRowLagBuffer) agg).init();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException
    {
      ExactRowLagBuffer b = (ExactRowLagBuffer) agg;

      if (b.firstRow)
      {
        b.firstRow = false;
        b.lagAmount = PrimitiveObjectInspectorUtils.getInt(
          parameters[1],
          PrimitiveObjectInspectorFactory.writableIntObjectInspector);
      }

      if (!b.valSet)
      {
        b.val = ObjectInspectorUtils.copyToStandardObject(
          parameters[0], inputOI, ObjectInspectorCopyOption.WRITABLE);

        b.valSet = true;
      }

      b.rowsSeen++;
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
      ExactRowLagBuffer b = (ExactRowLagBuffer) agg;
      if (b.rowsSeen == b.lagAmount) {
        return b.val;
      } else {
        return null;
      }
    }

  }
}

