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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.functionobjects.DiagIndex;
import org.apache.sysml.runtime.functionobjects.RevIndex;
import org.apache.sysml.runtime.functionobjects.SortIndex;
import org.apache.sysml.runtime.functionobjects.SwapIndex;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.functions.FilterDiagBlocksFunction;
import org.apache.sysml.runtime.instructions.flink.functions.ReorgMapFunction;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.instructions.flink.utils.FlinkUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;
import org.apache.sysml.runtime.util.UtilFunctions;

import java.util.ArrayList;

public class ReorgFLInstruction extends UnaryFLInstruction {

        //sort-specific attributes (to enable variable attributes)
        private CPOperand _col = null;
        private CPOperand _desc = null;
        private CPOperand _ixret = null;
        private boolean _bSortIndInMem = false;

        public ReorgFLInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String istr){
            super(op, in, out, opcode, istr);
            _fltype = FLINSTRUCTION_TYPE.Reorg;
        }

        public ReorgFLInstruction(Operator op, CPOperand in, CPOperand col, CPOperand desc, CPOperand ixret, CPOperand out, String opcode, boolean bSortIndInMem, String istr){
            this(op, in, out, opcode, istr);
            _col = col;
            _desc = desc;
            _ixret = ixret;
            _fltype = FLINSTRUCTION_TYPE.Reorg;
            _bSortIndInMem = bSortIndInMem;
        }

        public static ReorgFLInstruction parseInstruction ( String str )
                throws DMLRuntimeException
        {
            CPOperand in = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
            CPOperand out = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
            String opcode = InstructionUtils.getOpCode(str);

            if ( opcode.equalsIgnoreCase("r'") ) {
                parseUnaryInstruction(str, in, out); //max 2 operands
                return new ReorgFLInstruction(new ReorgOperator(SwapIndex.getSwapIndexFnObject()), in, out, opcode, str);
            }
            else if ( opcode.equalsIgnoreCase("rev") ) {
                parseUnaryInstruction(str, in, out); //max 2 operands
                return new ReorgFLInstruction(new ReorgOperator(RevIndex.getRevIndexFnObject()), in, out, opcode, str);
            }
            else if ( opcode.equalsIgnoreCase("rdiag") ) {
                parseUnaryInstruction(str, in, out); //max 2 operands
                return new ReorgFLInstruction(new ReorgOperator(DiagIndex.getDiagIndexFnObject()), in, out, opcode, str);
            }
            else if ( opcode.equalsIgnoreCase("rsort") ) {
                String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
                InstructionUtils.checkNumFields(parts, 5, 6);
                in.split(parts[1]);
                out.split(parts[5]);
                CPOperand col = new CPOperand(parts[2]);
                CPOperand desc = new CPOperand(parts[3]);
                CPOperand ixret = new CPOperand(parts[4]);
                boolean bSortIndInMem = false;

                if(parts.length > 5)
                    bSortIndInMem = Boolean.parseBoolean(parts[6]);

                return new ReorgFLInstruction(new ReorgOperator(SortIndex.getSortIndexFnObject(1,false,false)),
                        in, col, desc, ixret, out, opcode, bSortIndInMem, str);
            }
            else {
                throw new DMLRuntimeException("Unknown opcode while parsing a ReorgInstruction: " + str);
            }
        }

        @Override
        public void processInstruction(ExecutionContext ec)
                throws DMLRuntimeException
        {
            FlinkExecutionContext sec = (FlinkExecutionContext) ec;
            String opcode = getOpcode();

            //get input dataset handle
            DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in1 = sec.getBinaryBlockDataSetHandleForVariable( input1.getName() );
            DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out = null;
            MatrixCharacteristics mcIn = sec.getMatrixCharacteristics(input1.getName());

            if( opcode.equalsIgnoreCase("r'") ) //TRANSPOSE
            {
                //execute transpose reorg operation
                out = in1.map(new ReorgMapFunction(opcode));
            }
			else if( opcode.equalsIgnoreCase("rev") ) //REVERSE
			{
				//execute reverse reorg operation
				out = in1.flatMap(new DataSetRevFunction(mcIn));
				if( mcIn.getRows() % mcIn.getRowsPerBlock() != 0 )
					out = DataSetAggregateUtils.mergeByKey(out);
			}
			else if ( opcode.equalsIgnoreCase("rdiag") ) // DIAG
			{
				if(mcIn.getCols() == 1) { // diagV2M
					out = in1.flatMap(new DataSetDiagV2MFunction(mcIn));
				}
				else { // diagM2V
					//execute diagM2V operation
					out = in1.filter(new FilterDiagBlocksFunction())
						.map(new ReorgMapFunction(opcode));
				}
			}
            else if ( opcode.equalsIgnoreCase("rsort") ) //ORDER
            {
                throw new UnsupportedOperationException();
            }
            else {
                throw new DMLRuntimeException("Error: Incorrect opcode in ReorgFLInstruction:" + opcode);
            }

            //store output dataset handle
            updateReorgMatrixCharacteristics(sec);
            sec.setDataSetHandleForVariable(output.getName(), out);
            sec.addLineageDataSet(output.getName(), input1.getName());
        }

        /**
         *
         * @param sec
         * @throws DMLRuntimeException
         */
        private void updateReorgMatrixCharacteristics(FlinkExecutionContext sec)
                throws DMLRuntimeException
        {
            MatrixCharacteristics mc1 = sec.getMatrixCharacteristics(input1.getName());
            MatrixCharacteristics mcOut = sec.getMatrixCharacteristics(output.getName());

            //infer initially unknown dimensions from inputs
            if( !mcOut.dimsKnown() )
            {
                if( !mc1.dimsKnown() )
                    throw new DMLRuntimeException("Unable to compute output matrix characteristics from input.");

                if ( getOpcode().equalsIgnoreCase("r'") )
                    mcOut.set(mc1.getCols(), mc1.getRows(), mc1.getColsPerBlock(), mc1.getRowsPerBlock());
                else if ( getOpcode().equalsIgnoreCase("rdiag") )
                    mcOut.set(mc1.getRows(), (mc1.getCols()>1)?1:mc1.getRows(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
                else if ( getOpcode().equalsIgnoreCase("rsort") ) {
                    boolean ixret = sec.getScalarInput(_ixret.getName(), _ixret.getValueType(), _ixret.isLiteral()).getBooleanValue();
                    mcOut.set(mc1.getRows(), ixret?1:mc1.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
                }
            }

            //infer initially unknown nnz from input
            if( !mcOut.nnzKnown() && mc1.nnzKnown() ){
                boolean sortIx = getOpcode().equalsIgnoreCase("rsort") && sec.getScalarInput(_ixret.getName(), _ixret.getValueType(), _ixret.isLiteral()).getBooleanValue();
                if( sortIx )
                    mcOut.setNonZeros(mc1.getRows());
                else //default (r', rdiag, rsort data)
                    mcOut.setNonZeros(mc1.getNonZeros());
            }
        }

	/**
	 *
	 */
	private static class DataSetDiagV2MFunction implements FlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>>
	{
		private static final long serialVersionUID = 31065772250744103L;

		private ReorgOperator _reorgOp = null;
		private MatrixCharacteristics _mcIn = null;

		public DataSetDiagV2MFunction(MatrixCharacteristics mcIn)
			throws DMLRuntimeException
		{
			_reorgOp = new ReorgOperator(DiagIndex.getDiagIndexFnObject());
			_mcIn = mcIn;
		}

		@Override
		public void flatMap( Tuple2<MatrixIndexes, MatrixBlock> arg0, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out)
			throws Exception
		{
			ArrayList<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();

			MatrixIndexes ixIn = arg0.f0;
			MatrixBlock blkIn = arg0.f1;

			//compute output indexes and reorg data
			long rix = ixIn.getRowIndex();
			MatrixIndexes ixOut = new MatrixIndexes(rix, rix);
			MatrixBlock blkOut = (MatrixBlock) blkIn.reorgOperations(_reorgOp, new MatrixBlock(), -1, -1, -1);
			ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(ixOut,blkOut));

			// insert newly created empty blocks for entire row
			int numBlocks = (int) Math.ceil((double)_mcIn.getRows()/_mcIn.getRowsPerBlock());
			for(int i = 1; i <= numBlocks; i++) {
				if(i != ixOut.getColumnIndex()) {
					int lrlen = UtilFunctions.computeBlockSize(_mcIn.getRows(), rix, _mcIn.getRowsPerBlock());
					int lclen = UtilFunctions.computeBlockSize(_mcIn.getRows(), i, _mcIn.getRowsPerBlock());
					MatrixBlock emptyBlk = new MatrixBlock(lrlen, lclen, true);
					out.collect(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(rix, i), emptyBlk));
				}
			}
		}
	}

	/**
	 *
	 */
	private static class DataSetRevFunction implements FlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>>
	{
		private static final long serialVersionUID = 1183373828539843938L;

		private MatrixCharacteristics _mcIn = null;

		public DataSetRevFunction(MatrixCharacteristics mcIn)
			throws DMLRuntimeException
		{
			_mcIn = mcIn;
		}

		@Override
		public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> arg0 , Collector<Tuple2<MatrixIndexes, MatrixBlock>> out)
			throws Exception
		{
			//construct input
			IndexedMatrixValue in = FlinkUtils.toIndexedMatrixBlock(arg0);

			//execute reverse operation
			ArrayList<IndexedMatrixValue> out1 = new ArrayList<IndexedMatrixValue>();
			LibMatrixReorg.rev(in, _mcIn.getRows(), _mcIn.getRowsPerBlock(), out1);

			//construct output
			FlinkUtils.fromIndexedMatrixBlock(out1, out);
		}
	}

	/**
	 *
	 */
	private static class ExtractColumn implements MapFunction<MatrixBlock, MatrixBlock>
	{
		private static final long serialVersionUID = -1472164797288449559L;

		private int _col;

		public ExtractColumn(int col) {
			_col = col;
		}

		@Override
		public MatrixBlock map(MatrixBlock arg0)
			throws Exception
		{
			return arg0.sliceOperations(0, arg0.getNumRows()-1, _col, _col, new MatrixBlock());
		}
	}
}


