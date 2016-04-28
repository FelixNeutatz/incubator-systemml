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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.hops.AggBinaryOp.SparkAggType;
import org.apache.sysml.lops.PartialAggregate.CorrectionLocationType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.functionobjects.ReduceAll;
import org.apache.sysml.runtime.functionobjects.ReduceCol;
import org.apache.sysml.runtime.functionobjects.ReduceRow;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.functions.AggregateDropCorrectionFunction;
import org.apache.sysml.runtime.instructions.flink.functions.FilterDiagBlocksFunction;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;

/**
 * 
 */
public class AggregateUnaryFLInstruction extends UnaryFLInstruction
{
	
	private SparkAggType _aggtype = null;
	private AggregateOperator _aop = null;
	
	public AggregateUnaryFLInstruction(AggregateUnaryOperator auop, AggregateOperator aop, CPOperand in, CPOperand out, SparkAggType aggtype, String opcode, String istr){
		super(auop, in, out, opcode, istr);
		_aggtype = aggtype;
		_aop = aop;
	}
	
	/**
	 * 
	 * @param str
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static AggregateUnaryFLInstruction parseInstruction(String str)
		throws DMLRuntimeException 
	{
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 3);
		String opcode = parts[0];
		
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		SparkAggType aggtype = SparkAggType.valueOf(parts[3]);
		
		String aopcode = InstructionUtils.deriveAggregateOperatorOpcode(opcode);
		CorrectionLocationType corrLoc = InstructionUtils.deriveAggregateOperatorCorrectionLocation(opcode);
		String corrExists = (corrLoc != CorrectionLocationType.NONE) ? "true" : "false";
		
		AggregateUnaryOperator aggun = InstructionUtils.parseBasicAggregateUnaryOperator(opcode);
		AggregateOperator aop = InstructionUtils.parseAggregateOperator(aopcode, corrExists, corrLoc.toString());
		return new AggregateUnaryFLInstruction(aggun, aop, in1, out, aggtype, opcode, str);
	}
	
	@Override
	public void processInstruction( ExecutionContext ec )
		throws DMLRuntimeException
	{
		FlinkExecutionContext flec = (FlinkExecutionContext)ec;
		MatrixCharacteristics mc = flec.getMatrixCharacteristics(input1.getName());
		
		//get input
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in = flec.getBinaryBlockDataSetHandleForVariable( input1.getName() );
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out = in;
		
		//filter input blocks for trace
		if( getOpcode().equalsIgnoreCase("uaktrace") )
			out = out.filter(new FilterDiagBlocksFunction());
		
		//execute unary aggregate operation
		AggregateUnaryOperator auop = (AggregateUnaryOperator)_optr;
		AggregateOperator aggop = _aop;
		if( _aggtype == SparkAggType.NONE ) {
			//in case of no block aggregation, we always drop the correction as well as
			//use a partitioning-preserving mapvalues 
			out = out.map(new DataSetUAggValueFunction(auop, mc.getRowsPerBlock(), mc.getColsPerBlock()));
		}
		else {
			//in case of single/multi-block aggregation, we always keep the correction
			out = out.map(new DataSetUAggFunction(auop, mc.getRowsPerBlock(), mc.getColsPerBlock()));
		}
		
		
		//perform aggregation if necessary and put output into symbol table
		if( _aggtype == SparkAggType.SINGLE_BLOCK )
		{
			MatrixBlock out2 = DataSetAggregateUtils.aggStable(out, aggop);
			
			//drop correction after aggregation
			out2.dropLastRowsOrColums(aggop.correctionLocation);
			
			//put output block into symbol table (no lineage because single block)
			//this also includes implicit maintenance of matrix characteristics
			flec.setMatrixOutput(output.getName(), out2);
		}
		else //MULTI_BLOCK or NONE
		{
			if( _aggtype == SparkAggType.MULTI_BLOCK ) {
				out = DataSetAggregateUtils.aggByKeyStable(out, aggop);
	
				//drop correction after aggregation if required (aggbykey creates 
				//partitioning, drop correction via partitioning-preserving mapvalues)
				if( auop.aggOp.correctionExists )
					out = out.map( new AggregateDropCorrectionFunction(aggop) );
			}
			
			//put output DataSet handle into symbol table
			updateUnaryAggOutputMatrixCharacteristics(flec);
			flec.setDataSetHandleForVariable(output.getName(), out);	
			flec.addLineageDataSet(output.getName(), input1.getName());
		}		
	}
	
	/**
	 * 
	 * @param flec
	 * @param auop
	 * @throws DMLRuntimeException
	 */
	protected void updateUnaryAggOutputMatrixCharacteristics(FlinkExecutionContext flec) 
		throws DMLRuntimeException
	{
		AggregateUnaryOperator auop = (AggregateUnaryOperator)_optr;
		
		MatrixCharacteristics mc1 = flec.getMatrixCharacteristics(input1.getName());
		MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
		if(!mcOut.dimsKnown()) {
			if(!mc1.dimsKnown()) {
				throw new DMLRuntimeException("The output dimensions are not specified and cannot be inferred from input:" + mc1.toString() + " " + mcOut.toString());
			}
			else {
				//infer statistics from input based on operator
				if( auop.indexFn instanceof ReduceAll )
					mcOut.set(1, 1, mc1.getRowsPerBlock(), mc1.getColsPerBlock());
				else if (auop.indexFn instanceof ReduceCol)
					mcOut.set(mc1.getRows(), 1, mc1.getRowsPerBlock(), mc1.getColsPerBlock());
				else if (auop.indexFn instanceof ReduceRow)
					mcOut.set(1, mc1.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
			}
		}
	}

	/**
	 * 
	 */
	private static class DataSetUAggFunction implements MapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>> 
	{
		private static final long serialVersionUID = 2672082409287856038L;
		
		private AggregateUnaryOperator _op = null;
		private int _brlen = -1;
		private int _bclen = -1;
		
		public DataSetUAggFunction( AggregateUnaryOperator op, int brlen, int bclen )
		{
			_op = op;
			_brlen = brlen;
			_bclen = bclen;
		}
		
		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> map( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			MatrixIndexes ixIn = arg0.f0;
			MatrixBlock blkIn = arg0.f1;

			MatrixIndexes ixOut = new MatrixIndexes();
			MatrixBlock blkOut = new MatrixBlock();
			
			//unary aggregate operation (always keep the correction)
			OperationsOnMatrixValues.performAggregateUnary( ixIn, blkIn, 
					  ixOut, blkOut, _op, _brlen, _bclen);
			
			//output new tuple
			return new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut);
		}
	}
	
	/**
	 * 
	 */
	private static class DataSetUAggValueFunction implements MapFunction<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>> 
	{
		private static final long serialVersionUID = 5352374590399929673L;
		
		private AggregateUnaryOperator _op = null;
		private int _brlen = -1;
		private int _bclen = -1;
		private MatrixIndexes _ix = null;
		
		public DataSetUAggValueFunction( AggregateUnaryOperator op, int brlen, int bclen )
		{
			_op = op;
			_brlen = brlen;
			_bclen = bclen;
			
			_ix = new MatrixIndexes(1,1);
		}
		
		@Override
		public Tuple2<MatrixIndexes,MatrixBlock> map( Tuple2<MatrixIndexes,MatrixBlock> arg0 ) 
			throws Exception 
		{
			MatrixBlock blkOut = new MatrixBlock();
			
			//unary aggregate operation
			arg0.f1.aggregateUnaryOperations(_op, blkOut, _brlen, _bclen, _ix);
			
			//always drop correction since no aggregation
			blkOut.dropLastRowsOrColums(_op.aggOp.correctionLocation);
			
			//output new tuple
			return new Tuple2(arg0.f0, blkOut);
		}
	}
}
