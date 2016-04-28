/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.spark.api.java.function.Function2;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.functionobjects.CM;
import org.apache.sysml.runtime.functionobjects.KahanPlus;
import org.apache.sysml.runtime.instructions.cp.CM_COV_Object;
import org.apache.sysml.runtime.instructions.cp.KahanObject;
import org.apache.sysml.runtime.matrix.data.WeightedCell;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.CMOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class PerformGroupByAggInCombiner implements Function2<WeightedCell, WeightedCell, WeightedCell> {

	private static final long serialVersionUID = -813530414567786509L;
	
	private Operator _op;
	
	public PerformGroupByAggInCombiner(Operator op) {
		_op = op;
	}

	@Override
	public WeightedCell call(WeightedCell value1, WeightedCell value2) 
		throws Exception 
	{
		WeightedCell outCell = new WeightedCell();
		CM_COV_Object cmObj = new CM_COV_Object(); 
		if(_op instanceof CMOperator) //everything except sum
		{
			if( ((CMOperator) _op).isPartialAggregateOperator() )
			{
				cmObj.reset();
				CM lcmFn = CM.getCMFnObject(((CMOperator) _op).aggOpType); // cmFn.get(key.getTag());
				
				//partial aggregate cm operator
				lcmFn.execute(cmObj, value1.getValue(), value1.getWeight());
				lcmFn.execute(cmObj, value2.getValue(), value2.getWeight());
				
				outCell.setValue(cmObj.getRequiredPartialResult(_op));
				outCell.setWeight(cmObj.getWeight());	
			}
			else //forward tuples to reducer
			{
				throw new DMLRuntimeException("Incorrect usage, should have used PerformGroupByAggInReducer");
			}				
		}
		else if(_op instanceof AggregateOperator) //sum
		{
			AggregateOperator aggop=(AggregateOperator) _op;
				
			if( aggop.correctionExists ) {
				KahanObject buffer=new KahanObject(aggop.initialValue, 0);
				
				KahanPlus.getKahanPlusFnObject();
				
				//partial aggregate with correction
				aggop.increOp.fn.execute(buffer, value1.getValue()*value1.getWeight());
				aggop.increOp.fn.execute(buffer, value2.getValue()*value2.getWeight());
				
				outCell.setValue(buffer._sum);
				outCell.setWeight(1);
			}
			else //no correction
			{
				double v = aggop.initialValue;
				
				//partial aggregate without correction
				v=aggop.increOp.fn.execute(v, value1.getValue()*value1.getWeight());
				v=aggop.increOp.fn.execute(v, value2.getValue()*value2.getWeight());
				
				outCell.setValue(v);
				outCell.setWeight(1);
			}				
		}
		else
			throw new DMLRuntimeException("Unsupported operator in grouped aggregate instruction:" + _op);
		
		return outCell;
	}
}
