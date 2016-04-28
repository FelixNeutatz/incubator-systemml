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
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;

/**
 * 
 */
public class CumulativeAggregateFLInstruction extends AggregateUnaryFLInstruction 
{
	
	public CumulativeAggregateFLInstruction(AggregateUnaryOperator op, CPOperand in1, CPOperand out, String opcode, String istr )
	{
		super(op, null, in1, out, null, opcode, istr);
		_fltype = FLINSTRUCTION_TYPE.CumsumAggregate;
	}

	/**
	 * 
	 * @param str
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static CumulativeAggregateFLInstruction parseInstruction(String str ) 
		throws DMLRuntimeException 
	{
		String[] parts = InstructionUtils.getInstructionPartsWithValueType( str );
		InstructionUtils.checkNumFields ( parts, 2 );
				
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		
		AggregateUnaryOperator aggun = InstructionUtils.parseCumulativeAggregateUnaryOperator(opcode);
		
		return new CumulativeAggregateFLInstruction(aggun, in1, out, opcode, str);	
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{
		FlinkExecutionContext flec = (FlinkExecutionContext)ec;
		MatrixCharacteristics mc = flec.getMatrixCharacteristics(input1.getName());
		long rlen = mc.getRows();
		int brlen = mc.getRowsPerBlock();
		int bclen = mc.getColsPerBlock();
		
		//get input
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in = flec.getBinaryBlockDataSetHandleForVariable( input1.getName() );
		
		//execute unary aggregate (w/ implicit drop correction)
		AggregateUnaryOperator auop = (AggregateUnaryOperator) _optr;
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out = 
				in.map(new DataSetCumAggFunction(auop, rlen, brlen, bclen));
		out = DataSetAggregateUtils.mergeByKey(out);
		
		//put output handle in symbol table
		flec.setDataSetHandleForVariable(output.getName(), out);	
		flec.addLineageDataSet(output.getName(), input1.getName());
	}
	
	/**
	 * 
	 * 
	 */
	private static class DataSetCumAggFunction implements MapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>> 
	{
		private static final long serialVersionUID = 11324676268945117L;
		
		private AggregateUnaryOperator _op = null;		
		private long _rlen = -1;
		private int _brlen = -1;
		private int _bclen = -1;
		
		public DataSetCumAggFunction( AggregateUnaryOperator op, long rlen, int brlen, int bclen )
		{
			_op = op;
			_rlen = rlen;
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
			
			//process instruction
			OperationsOnMatrixValues.performAggregateUnary( ixIn, blkIn, ixOut, blkOut, 
					                            ((AggregateUnaryOperator)_op), _brlen, _bclen);
			if( ((AggregateUnaryOperator)_op).aggOp.correctionExists )
				blkOut.dropLastRowsOrColums(((AggregateUnaryOperator)_op).aggOp.correctionLocation);
			
			//cumsum expand partial aggregates
			long rlenOut = (long)Math.ceil((double)_rlen/_brlen);
			long rixOut = (long)Math.ceil((double)ixIn.getRowIndex()/_brlen);
			int rlenBlk = (int) Math.min(rlenOut-(rixOut-1)*_brlen, _brlen);
			int clenBlk = blkOut.getNumColumns();
			int posBlk = (int) ((ixIn.getRowIndex()-1) % _brlen);
			MatrixBlock blkOut2 = new MatrixBlock(rlenBlk, clenBlk, false);
			blkOut2.copy(posBlk, posBlk, 0, clenBlk-1, blkOut, true);
			ixOut.setIndexes(rixOut, ixOut.getColumnIndex());
			
			//output new tuple
			return new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut2);
		}
	}
}
