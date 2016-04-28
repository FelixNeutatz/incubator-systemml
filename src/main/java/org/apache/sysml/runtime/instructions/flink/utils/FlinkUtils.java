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

package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixCell;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.util.UtilFunctions;

import java.util.ArrayList;

public class FlinkUtils {
    /**
     * @param env
     * @param mc
     * @return
     */
    public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> getEmptyBlockRDD(ExecutionEnvironment env,
                                                                               MatrixCharacteristics mc) {
        //create all empty blocks
        ArrayList<Tuple2<MatrixIndexes, MatrixBlock>> list = new ArrayList<Tuple2<MatrixIndexes, MatrixBlock>>();
        int nrblks = (int) Math.ceil((double) mc.getRows() / mc.getRowsPerBlock());
        int ncblks = (int) Math.ceil((double) mc.getCols() / mc.getColsPerBlock());
        for (long r = 1; r <= nrblks; r++)
            for (long c = 1; c <= ncblks; c++) {
                int lrlen = UtilFunctions.computeBlockSize(mc.getRows(), r, mc.getRowsPerBlock());
                int lclen = UtilFunctions.computeBlockSize(mc.getCols(), c, mc.getColsPerBlock());
                MatrixIndexes ix = new MatrixIndexes(r, c);
                MatrixBlock mb = new MatrixBlock(lrlen, lclen, true);
                list.add(new Tuple2<MatrixIndexes, MatrixBlock>(ix, mb));
            }

        //create rdd of in-memory list
        return env.fromCollection(list);
    }

	/**
	 *
	 * @param in
	 * @return
	 */
	public static Tuple2<MatrixIndexes,MatrixBlock> fromIndexedMatrixBlock( IndexedMatrixValue in ){
		return new Tuple2<MatrixIndexes,MatrixBlock>(in.getIndexes(), (MatrixBlock)in.getValue());
	}


	/**
	 * Utility to compute dimensions and non-zeros in a given RDD of binary cells.
	 *
	 * @param rdd
	 * @param computeNNZ
	 * @return
	 */
	public static MatrixCharacteristics computeMatrixCharacteristics(DataSet<Tuple2<MatrixIndexes, MatrixCell>> input)
	{
		//TODO: check whether there is a better way
		// compute dimensions and nnz in single pass
		MatrixCharacteristics ret = null;
		try {
			ret = input
				.map(new AnalyzeCellMatrixCharacteristics())
				.reduce(new AggregateMatrixCharacteristics()).collect().get(0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}

	/**
	 *
	 */
	private static class AggregateMatrixCharacteristics implements ReduceFunction<MatrixCharacteristics>
	{
		private static final long serialVersionUID = 4263886749699779994L;

		@Override
		public MatrixCharacteristics reduce(MatrixCharacteristics arg0, MatrixCharacteristics arg1)
			throws Exception
		{
			return new MatrixCharacteristics(
				Math.max(arg0.getRows(), arg1.getRows()),  //max
				Math.max(arg0.getCols(), arg1.getCols()),  //max
				arg0.getRowsPerBlock(),
				arg0.getColsPerBlock(),
				arg0.getNonZeros() + arg1.getNonZeros() ); //sum
		}
	}

	/**
	 *
	 */
	private static class AnalyzeCellMatrixCharacteristics implements MapFunction<Tuple2<MatrixIndexes,MatrixCell>, MatrixCharacteristics>
	{
		private static final long serialVersionUID = 8899395272683723008L;

		@Override
		public MatrixCharacteristics map(Tuple2<MatrixIndexes, MatrixCell> arg0)
			throws Exception
		{
			long rix = arg0.f0.getRowIndex();
			long cix = arg0.f0.getColumnIndex();
			long nnz = (arg0.f1.getValue()!=0) ? 1 : 0;
			return new MatrixCharacteristics(rix, cix, 0, 0, nnz);
		}
	}

	/**
	 *
	 * @param in
	 * @return
	 */
	public static void fromIndexedMatrixBlock(ArrayList<IndexedMatrixValue> in, Collector<Tuple2<MatrixIndexes,MatrixBlock>> out)
	{
		ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
		for( IndexedMatrixValue imv : in )
			out.collect(fromIndexedMatrixBlock(imv));
	}

	/**
	 *
	 * @param ix
	 * @param mb
	 * @return
	 */
	public static IndexedMatrixValue toIndexedMatrixBlock(MatrixIndexes ix, MatrixBlock mb ) {
		return new IndexedMatrixValue(ix, mb);
	}
}
