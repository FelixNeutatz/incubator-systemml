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

package org.apache.sysml.runtime.instructions.flink;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.functionobjects.KahanPlus;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.cp.DoubleObject;
import org.apache.sysml.runtime.instructions.cp.ScalarObject;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;

/**
 * 
 */
public class AggregateTernaryFLInstruction extends ComputationFLInstruction
{
	
	public AggregateTernaryFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand in3,
										 CPOperand out, String opcode, String istr )
	{
		super(op, in1, in2, in3, out, opcode, istr);
		_fltype = FLINSTRUCTION_TYPE.AggregateTernary;
	}

	public static AggregateTernaryFLInstruction parseInstruction(String str ) 
		throws DMLRuntimeException 
	{
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		
		if ( opcode.equalsIgnoreCase("tak+*")) {
			InstructionUtils.checkNumFields ( parts, 4 );
			
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			CPOperand in3 = new CPOperand(parts[3]);
			CPOperand out = new CPOperand(parts[4]);

			AggregateOperator agg = new AggregateOperator(0, KahanPlus.getKahanPlusFnObject());
			AggregateBinaryOperator op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
			
			return new AggregateTernaryFLInstruction(op, in1, in2, in3, out, opcode, str);
		} 
		else {
			throw new DMLRuntimeException("AggregateTertiaryInstruction.parseInstruction():: Unknown opcode " + opcode);
		}
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{	
		FlinkExecutionContext flec = (FlinkExecutionContext)ec;
		
		//get input
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in1 = flec.getBinaryBlockDataSetHandleForVariable( input1.getName() );
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in2 = flec.getBinaryBlockDataSetHandleForVariable( input2.getName() );
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in3 = input3.isLiteral() ? null : //matrix or literal 1
													 flec.getBinaryBlockDataSetHandleForVariable( input3.getName() );
		
		//execute aggregate ternary operation
		AggregateBinaryOperator aggop = (AggregateBinaryOperator) _optr;
		DataSet<MatrixBlock> out = null;
		if( in3 != null ) { //3 inputs
			out = in1.join( in2 ).where(0).equalTo(0).with(new Join1()).join( in3 ).where(0).equalTo(0)
				     .with(new DataSetAggregateTernaryFunction(aggop));
		}
		else { //2 inputs (third is literal 1)
			out = in1.join( in2 ).where(0).equalTo(0)
					 .with(new DataSetAggregateTernaryFunction2(aggop));
		}
				
		//aggregate and create output (no lineage because scalar)	   
		MatrixBlock tmp = DataSetAggregateUtils.sumStable(out);
		DoubleObject ret = new DoubleObject(tmp.getValue(0, 0));
		flec.setVariable(output.getName(), ret);
	}
	
	/**
	 * 
	 */
	private static class DataSetAggregateTernaryFunction 
		implements JoinFunction<Tuple3<MatrixIndexes,MatrixBlock,MatrixBlock>,Tuple2<MatrixIndexes,MatrixBlock>, MatrixBlock>
	{
		private static final long serialVersionUID = 6410232464410434210L;
		
		private AggregateBinaryOperator _aggop = null;
		
		public DataSetAggregateTernaryFunction( AggregateBinaryOperator aggop ) 
		{
			_aggop = aggop;
		}
	
		@Override
		public MatrixBlock join(Tuple3<MatrixIndexes,MatrixBlock,MatrixBlock> a, Tuple2<MatrixIndexes,MatrixBlock> b)
			throws Exception 
		{
			//get inputs
			MatrixBlock in1 = a.f1;
			MatrixBlock in2 = a.f2;
			MatrixBlock in3 = b.f1;
			
			//execute aggregate ternary operation
			ScalarObject ret = in1.aggregateTernaryOperations(in1, in2, in3, _aggop);
			
			//create output matrix block (w/ correction)
			MatrixBlock out = new MatrixBlock(2,1,false);
			out.setValue(0, 0, ret.getDoubleValue());
			return out;
		}
	}


	private static class Join1
		implements JoinFunction<Tuple2<MatrixIndexes,MatrixBlock>,Tuple2<MatrixIndexes,MatrixBlock>, Tuple3<MatrixIndexes,MatrixBlock, MatrixBlock>>
	{
		public Join1(){
		}

		@Override
		public Tuple3<MatrixIndexes,MatrixBlock, MatrixBlock> join(Tuple2<MatrixIndexes,MatrixBlock> a, Tuple2<MatrixIndexes,MatrixBlock> b)
			throws Exception
		{
			return new Tuple3<MatrixIndexes, MatrixBlock, MatrixBlock>(a.f0, a.f1, b.f1);
		}
	}
	
	/**
	 * 
	 */
	private static class DataSetAggregateTernaryFunction2 
		implements JoinFunction<Tuple2<MatrixIndexes,MatrixBlock>,Tuple2<MatrixIndexes,MatrixBlock>, MatrixBlock>
	{
		private static final long serialVersionUID = -6615412819746331700L;
		
		private AggregateBinaryOperator _aggop = null;
		
		public DataSetAggregateTernaryFunction2( AggregateBinaryOperator aggop ) 
		{
			_aggop = aggop;		
		}
	
		@Override
		public MatrixBlock join(Tuple2<MatrixIndexes,MatrixBlock> a, Tuple2<MatrixIndexes,MatrixBlock> b)
			throws Exception 
		{
			//get inputs
			MatrixBlock in1 = a.f1;
			MatrixBlock in2 = b.f1;
			
			//execute aggregate ternary operation
			ScalarObject ret = in1.aggregateTernaryOperations(in1, in2, null, _aggop);
			
			//create output matrix block (w/ correction)
			MatrixBlock out = new MatrixBlock(2,1,false);
			out.setValue(0, 0, ret.getDoubleValue());
			return out;
		}
	}
}
