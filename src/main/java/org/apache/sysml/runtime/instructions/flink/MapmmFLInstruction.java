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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.hops.AggBinaryOp;
import org.apache.sysml.lops.MapMult;
import org.apache.sysml.lops.MapMult.CacheType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import static org.apache.sysml.runtime.instructions.flink.functions.MatrixMultiplicationFunctions.*;

/**
 * Implementation of {@link org.apache.sysml.runtime.instructions.Instruction} to run matrix multiplication on Flink.
 */
public class MapmmFLInstruction extends BinaryFLInstruction {

	private final MapMult.CacheType cacheType;
	private final boolean outputEmtpy;
	private final AggBinaryOp.SparkAggType aggType;

	public MapmmFLInstruction(CPOperand input1, CPOperand input2, CPOperand output,
							  CacheType cacheType, boolean outputEmtpy, AggBinaryOp.SparkAggType aggType,
							  String opcode, String istr) {
		super(null, input1, input2, output, opcode, istr);
		this.cacheType = cacheType;
		this.aggType = aggType;
		this.outputEmtpy = outputEmtpy;
	}

	/**
	 * Factory method that parses an instruction string of the form
	 * <p>
	 * <pre><code>FLINK°mapmm°_mVar1·MATRIX·DOUBLE°_mVar2·MATRIX·DOUBLE°_mVar3·MATRIX·DOUBLE°RIGHT°false°MULTI_BLOCK</code></pre>
	 *
	 * and returns a new {@link MapmmFLInstruction} representing the instruction string.
	 *
	 * @param instr Instruction string, operands separated by <code>°</code> and value types by <code>·</code>
	 * @return a new MapmmFLInstruction
	 * @throws DMLRuntimeException
	 */
	public static MapmmFLInstruction parseInstruction(String instr) throws DMLRuntimeException {
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(instr);
		String opcode = parts[0];
		if (opcode.equalsIgnoreCase(MapMult.OPCODE)) {
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			CPOperand out = new CPOperand(parts[3]);
			MapMult.CacheType type = MapMult.CacheType.valueOf(parts[4]);
			boolean outputEmpty = Boolean.parseBoolean(parts[5]);
			AggBinaryOp.SparkAggType aggtype = AggBinaryOp.SparkAggType.valueOf(parts[6]);

			return new MapmmFLInstruction(in1, in2, out, type, outputEmpty, aggtype, opcode, instr);
		} else {
			throw new DMLRuntimeException("MapmmSPInstruction.parseInstruction():: Unknown opcode " + opcode);
		}
	}

	@Override
	public void processInstruction(ExecutionContext ec) throws DMLRuntimeException {
		assert ec instanceof FlinkExecutionContext :
				"Expected " + FlinkExecutionContext.class.getCanonicalName() + " got " + ec.getClass().getCanonicalName();
		FlinkExecutionContext fec = (FlinkExecutionContext) ec;

		DataSet<Tuple2<MatrixIndexes, MatrixBlock>> A = fec.getBinaryBlockDataSetHandleForVariable(input1.getName());
		DataSet<Tuple2<MatrixIndexes, MatrixBlock>> B = fec.getBinaryBlockDataSetHandleForVariable(input2.getName());
		DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = A
				.join(B)
				.where(new ColumnSelector())
				.equalTo(new RowSelector())
				.with(new MultiplyMatrixBlocks())
				.groupBy(new MatrixIndexesSelector())
//				.combineGroup(new SumMatrixBlocksCombine())
				.reduce(new SumMatrixBlocksStable());

		// register variable for output
		fec.setDataSetHandleForVariable(output.getName(), out);
	}
}