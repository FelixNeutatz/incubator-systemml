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
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.WeightedCell;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.util.UtilFunctions;

import java.io.Serializable;

public abstract class ExtractGroup implements Serializable 
{
	private static final long serialVersionUID = -7059358143841229966L;

	protected long _bclen = -1; 
	protected long _ngroups = -1; 
	protected Operator _op = null;
	
	public ExtractGroup(long bclen, long ngroups, Operator op ) {
		_bclen = bclen;
		_ngroups = ngroups;
		_op = op;
	}
	
	/**
	 * 
	 * @param ix
	 * @param group
	 * @param target
	 * @return
	 * @throws Exception 
	 */
	protected void execute(MatrixIndexes ix, MatrixBlock group, MatrixBlock target, Collector<Tuple2<MatrixIndexes, WeightedCell>> out) throws Exception
	{
		//sanity check matching block dimensions
		if(group.getNumRows() != target.getNumRows()) {
			throw new Exception("The blocksize for group and target blocks are mismatched: " + group.getNumRows()  + " != " + target.getNumRows());
		}
		
		//output weighted cells
		long coloff = (ix.getColumnIndex()-1)*_bclen;
		
		//local pre-aggregation for sum w/ known output dimensions
		if(_op instanceof AggregateOperator && _ngroups > 0 
			&& OptimizerUtils.isValidCPDimensions(_ngroups, target.getNumColumns()) ) 
		{
			MatrixBlock tmp = group.groupedAggOperations(target, null, new MatrixBlock(), (int)_ngroups, _op);
			
			for(int i=0; i<tmp.getNumRows(); i++) {
				for( int j=0; j<tmp.getNumColumns(); j++ ) {
					double tmpval = tmp.quickGetValue(i, j);
					if( tmpval != 0 ) {
						WeightedCell weightedCell = new WeightedCell();
						weightedCell.setValue(tmpval);
						weightedCell.setWeight(1);
						MatrixIndexes ixout = new MatrixIndexes(i+1,coloff+j+1);
						out.collect(new Tuple2<MatrixIndexes, WeightedCell>(ixout, weightedCell));
					}
				}
			}
		}
		//general case without pre-aggregation
		else 
		{
			for(int i = 0; i < group.getNumRows(); i++) {
				long groupVal = UtilFunctions.toLong(group.quickGetValue(i, 0));
				if(groupVal < 1) {
					throw new Exception("Expected group values to be greater than equal to 1 but found " + groupVal);
				}
				for( int j=0; j<target.getNumColumns(); j++ ) {
					WeightedCell weightedCell = new WeightedCell();
					weightedCell.setValue(target.quickGetValue(i, j));
					weightedCell.setWeight(1);
					MatrixIndexes ixout = new MatrixIndexes(groupVal,coloff+j+1);
					out.collect(new Tuple2<MatrixIndexes, WeightedCell>(ixout, weightedCell));
				}
			}
		}
	}
	
	/**
	 * 
	 */
	public static class ExtractGroupJoin extends ExtractGroup implements FlatJoinFunction<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes, WeightedCell>> 
	{
		private static final long serialVersionUID = 8890978615936560266L;

		public ExtractGroupJoin(long bclen, long ngroups, Operator op) {
			super(bclen, ngroups, op);
		}
		
		@Override
		public void join(Tuple2<MatrixIndexes, MatrixBlock> a, Tuple2<MatrixIndexes, MatrixBlock> b , Collector<Tuple2<MatrixIndexes, WeightedCell>> out)
				throws Exception 
		{
			MatrixIndexes ix = a.f0;
			MatrixBlock group = a.f1;
			MatrixBlock target = b.f1;
	
			execute(ix, group, target, out);
		}	
	}
}
