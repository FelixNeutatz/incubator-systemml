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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.WeightedCell;
import org.apache.sysml.runtime.util.UtilFunctions;

import java.util.ArrayList;

public class ExtractGroupNWeights implements FlatJoinFunction<Tuple2<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>>, Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,WeightedCell>> {

	private static final long serialVersionUID = -188180042997588072L;

	@Override
	public void join(Tuple2<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>> a, Tuple2<MatrixIndexes,MatrixBlock> b, Collector<Tuple2<MatrixIndexes,WeightedCell>> out)
		throws Exception 
	{
		MatrixBlock group = a.f0.f1;
		MatrixBlock target = a.f1.f1;
		MatrixBlock weight = b.f1;
		
		//sanity check matching block dimensions
		if(group.getNumRows() != target.getNumRows() || group.getNumRows()!=target.getNumRows()) {
			throw new Exception("The blocksize for group/target/weight blocks are mismatched: " + group.getNumRows()  + ", " + target.getNumRows() + ", " + weight.getNumRows());
		}
		
		//output weighted cells		
		for(int i = 0; i < group.getNumRows(); i++) {
			WeightedCell weightedCell = new WeightedCell();
			weightedCell.setValue(target.quickGetValue(i, 0));
			weightedCell.setWeight(weight.quickGetValue(i, 0));
			long groupVal = UtilFunctions.toLong(group.quickGetValue(i, 0));
			if(groupVal < 1) {
				throw new Exception("Expected group values to be greater than equal to 1 but found " + groupVal);
			}
			MatrixIndexes ix = new MatrixIndexes(groupVal, 1);
			out.collect(new Tuple2<MatrixIndexes, WeightedCell>(ix, weightedCell));
		}
	}
}
