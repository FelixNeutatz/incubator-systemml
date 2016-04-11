package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.util.UtilFunctions;

import java.util.ArrayList;

public class FlinkUtils {
	/**
	 *
	 * @param env
	 * @param mc
	 * @return
	 */
	public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> getEmptyBlockRDD(ExecutionEnvironment env, MatrixCharacteristics mc )
	{
		//create all empty blocks
		ArrayList<Tuple2<MatrixIndexes,MatrixBlock>> list = new ArrayList<Tuple2<MatrixIndexes,MatrixBlock>>();
		int nrblks = (int)Math.ceil((double)mc.getRows()/mc.getRowsPerBlock());
		int ncblks = (int)Math.ceil((double)mc.getCols()/mc.getColsPerBlock());
		for(long r=1; r<=nrblks; r++)
			for(long c=1; c<=ncblks; c++)
			{
				int lrlen = UtilFunctions.computeBlockSize(mc.getRows(), r, mc.getRowsPerBlock());
				int lclen = UtilFunctions.computeBlockSize(mc.getCols(), c, mc.getColsPerBlock());
				MatrixIndexes ix = new MatrixIndexes(r, c);
				MatrixBlock mb = new MatrixBlock(lrlen, lclen, true);
				list.add(new Tuple2<MatrixIndexes,MatrixBlock>(ix,mb));
			}

		//create rdd of in-memory list
		return env.fromCollection(list);
	}
}
