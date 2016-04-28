package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class BroadcastFunction {
	
	public static HashMap<Long, HashMap<Long, MatrixBlock>> open(RuntimeContext rc, HashMap<Long, HashMap<Long, MatrixBlock>> _pbc) throws Exception {
		_pbc = new HashMap<Long, HashMap<Long, MatrixBlock>>();

		Collection<Tuple2<MatrixIndexes,MatrixBlock>> blocklist = rc.getBroadcastVariable("bcastVar");

		HashMap<Long, MatrixBlock> tempMap = null;
		long columnIndex = 0L;
		long rowIndex = 0L;
		
		for (Tuple2<MatrixIndexes,MatrixBlock> broadcastTuple : blocklist){
			columnIndex = broadcastTuple.f0.getColumnIndex();
			rowIndex = broadcastTuple.f0.getRowIndex();

			tempMap = _pbc.get(rowIndex);
			if (tempMap == null) {
				tempMap = new HashMap<Long, MatrixBlock>();
			}
			tempMap.put(columnIndex, broadcastTuple.f1);
			_pbc.put(rowIndex, tempMap);
		}
		return _pbc;
	}
	
	public static HashMap<Long, HashMap<Long, MatrixBlock>> close(HashMap<Long, HashMap<Long, MatrixBlock>> _pbc) throws Exception {
		for (Map.Entry<Long,HashMap<Long,MatrixBlock>> e : _pbc.entrySet()) {
			e.getValue().clear();
		}
		_pbc.clear();
		return _pbc;
	}
}
