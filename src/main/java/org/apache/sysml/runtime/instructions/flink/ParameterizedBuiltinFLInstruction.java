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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.sysml.lops.Lop;
import org.apache.sysml.lops.PartialAggregate.CorrectionLocationType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.FrameObject;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.functionobjects.KahanPlus;
import org.apache.sysml.runtime.functionobjects.ParameterizedBuiltin;
import org.apache.sysml.runtime.functionobjects.ValueFunction;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.functions.RichFlatMapBroadcastFunction;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.instructions.flink.utils.FlinkUtils;
import org.apache.sysml.runtime.instructions.mr.GroupedAggregateInstruction;
import org.apache.sysml.runtime.instructions.flink.functions.ExtractGroupBroadcast;
import org.apache.sysml.runtime.instructions.flink.functions.ExtractGroup.ExtractGroupJoin;
import org.apache.sysml.runtime.instructions.flink.functions.ExtractGroupNWeights;
import org.apache.sysml.runtime.instructions.flink.functions.ReplicateVectorFunction;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.*;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.CMOperator;
import org.apache.sysml.runtime.matrix.operators.CMOperator.AggregateOperationTypes;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.SimpleOperator;
import org.apache.sysml.runtime.transform.DataTransform;
import org.apache.sysml.runtime.util.UtilFunctions;

import java.util.ArrayList;
import java.util.HashMap;

public class ParameterizedBuiltinFLInstruction extends ComputationFLInstruction 
{	
	protected HashMap<String,String> params;
	
	//removeEmpty-specific attributes
	private boolean _bRmEmptyBC = false;
	
	public ParameterizedBuiltinFLInstruction(Operator op, HashMap<String,String> paramsMap, CPOperand out, String opcode, String istr, boolean bRmEmptyBC )
	{
		super(op, null, null, out, opcode, istr);
		_fltype = FLINSTRUCTION_TYPE.ParameterizedBuiltin;
		params = paramsMap;
		_bRmEmptyBC = bRmEmptyBC;
	}

	public HashMap<String,String> getParams() { return params; }
	
	public static HashMap<String, String> constructParameterMap(String[] params) {
		// process all elements in "params" except first(opcode) and last(output)
		HashMap<String,String> paramMap = new HashMap<String,String>();
		
		// all parameters are of form <name=value>
		String[] parts;
		for ( int i=1; i <= params.length-2; i++ ) {
			parts = params[i].split(Lop.NAME_VALUE_SEPARATOR);
			paramMap.put(parts[0], parts[1]);
		}
		
		return paramMap;
	}
	
	public static ParameterizedBuiltinFLInstruction parseInstruction (String str ) 
		throws DMLRuntimeException 
	{
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		// first part is always the opcode
		String opcode = parts[0];

		if( opcode.equalsIgnoreCase("mapgroupedagg") )
		{
			CPOperand target = new CPOperand( parts[1] ); 
			CPOperand groups = new CPOperand( parts[2] );
			CPOperand out = new CPOperand( parts[3] );

			HashMap<String,String> paramsMap = new HashMap<String, String>();
			paramsMap.put(Statement.GAGG_TARGET, target.getName());
			paramsMap.put(Statement.GAGG_GROUPS, groups.getName());
			paramsMap.put(Statement.GAGG_NUM_GROUPS, parts[4]);
			
			Operator op = new AggregateOperator(0, KahanPlus.getKahanPlusFnObject(), true, CorrectionLocationType.LASTCOLUMN);
			
			return new ParameterizedBuiltinFLInstruction(op, paramsMap, out, opcode, str, false);
		}
		else
		{
			// last part is always the output
			CPOperand out = new CPOperand( parts[parts.length-1] ); 

			// process remaining parts and build a hash map
			HashMap<String,String> paramsMap = constructParameterMap(parts);

			// determine the appropriate value function
			ValueFunction func = null;
					
			if ( opcode.equalsIgnoreCase("groupedagg")) {
				// check for mandatory arguments
				String fnStr = paramsMap.get("fn");
				if ( fnStr == null ) 
					throw new DMLRuntimeException("Function parameter is missing in groupedAggregate.");
				if ( fnStr.equalsIgnoreCase("centralmoment") ) {
					if ( paramsMap.get("order") == null )
						throw new DMLRuntimeException("Mandatory \"order\" must be specified when fn=\"centralmoment\" in groupedAggregate.");
				}
				
				Operator op = GroupedAggregateInstruction.parseGroupedAggOperator(fnStr, paramsMap.get("order"));
				return new ParameterizedBuiltinFLInstruction(op, paramsMap, out, opcode, str, false);
			}
			else if(   opcode.equalsIgnoreCase("rmempty") ) 
			{
				boolean bRmEmptyBC = false; 
				if(parts.length > 6)
					bRmEmptyBC = Boolean.parseBoolean(parts[5]);
									
				func = ParameterizedBuiltin.getParameterizedBuiltinFnObject(opcode);
				return new ParameterizedBuiltinFLInstruction(new SimpleOperator(func), paramsMap, out, opcode, str, bRmEmptyBC);
			}
			else if(   opcode.equalsIgnoreCase("rexpand") 
					|| opcode.equalsIgnoreCase("replace")
					|| opcode.equalsIgnoreCase("transform") ) 
			{
				func = ParameterizedBuiltin.getParameterizedBuiltinFnObject(opcode);
				return new ParameterizedBuiltinFLInstruction(new SimpleOperator(func), paramsMap, out, opcode, str, false);
			}
			else {
				throw new DMLRuntimeException("Unknown opcode (" + opcode + ") for ParameterizedBuiltin Instruction.");
			}
		}
	}
	

	@Override 
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException 
	{
		FlinkExecutionContext flec = (FlinkExecutionContext)ec;
		String opcode = getOpcode();
		
		//opcode guaranteed to be a valid opcode (see parsing)
		if( opcode.equalsIgnoreCase("mapgroupedagg") )
		{		
			//get input DataSet handle
			String targetVar = params.get(Statement.GAGG_TARGET);
			String groupsVar = params.get(Statement.GAGG_GROUPS);			
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> target = flec.getBinaryBlockDataSetHandleForVariable(targetVar);
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> groups = flec.getBinaryBlockDataSetHandleForVariable(groupsVar);
			MatrixCharacteristics mc1 = flec.getMatrixCharacteristics( targetVar );
			MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
			CPOperand ngrpOp = new CPOperand(params.get(Statement.GAGG_NUM_GROUPS));
			int ngroups = (int)flec.getScalarInput(ngrpOp.getName(), ngrpOp.getValueType(), ngrpOp.isLiteral()).getLongValue();
			
			//execute map grouped aggregate
			DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = 
					target.flatMap(new DataSetMapGroupedAggFunction(_optr, 
							ngroups, mc1.getRowsPerBlock(), mc1.getColsPerBlock())).withBroadcastSet(groups, "bcastVar");
			out = DataSetAggregateUtils.sumByKeyStable(out);
			
			//updated characteristics and handle outputs
			mcOut.set(ngroups, mc1.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock(), -1);
			flec.setDataSetHandleForVariable(output.getName(), out);
			flec.addLineageDataSet( output.getName(), targetVar );
		}
		else if ( opcode.equalsIgnoreCase("groupedagg") ) 
		{	
			throw new UnsupportedOperationException("groupedagg is not yet supported");
			/*
			boolean broadcastGroups = Boolean.parseBoolean(params.get("broadcast"));
			
			//get input DataSet handle
			String groupsVar = params.get(Statement.GAGG_GROUPS);
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> target = flec.getBinaryBlockDataSetHandleForVariable( params.get(Statement.GAGG_TARGET) );
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> groups = broadcastGroups ? null : flec.getBinaryBlockDataSetHandleForVariable( groupsVar );
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> weights = null;
			
			MatrixCharacteristics mc1 = flec.getMatrixCharacteristics( params.get(Statement.GAGG_TARGET) );
			MatrixCharacteristics mc2 = flec.getMatrixCharacteristics( groupsVar );
			if(mc1.dimsKnown() && mc2.dimsKnown() && (mc1.getRows() != mc2.getRows() || mc2.getCols() !=1)) {
				throw new DMLRuntimeException("Grouped Aggregate dimension mismatch between target and groups.");
			}
			MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
			
			DataSet<Tuple2<MatrixIndexes, WeightedCell>> groupWeightedCells = null;
			
			// Step 1: First extract groupWeightedCells from group, target and weights
			if ( params.get(Statement.GAGG_WEIGHTS) != null ) {
				weights = flec.getBinaryBlockDataSetHandleForVariable( params.get(Statement.GAGG_WEIGHTS) );
				
				MatrixCharacteristics mc3 = flec.getMatrixCharacteristics( params.get(Statement.GAGG_WEIGHTS) );
				if(mc1.dimsKnown() && mc3.dimsKnown() && (mc1.getRows() != mc3.getRows() || mc1.getCols() != mc3.getCols())) {
					throw new DMLRuntimeException("Grouped Aggregate dimension mismatch between target, groups, and weights.");
				}
				
				//probably wrong
				groupWeightedCells = groups.join(target).where(0).equalTo(0).join(weights).where(0).equalTo(0).with(new ExtractGroupNWeights());
			}
			else //input vector or matrix
			{
				String ngroupsStr = params.get(Statement.GAGG_NUM_GROUPS);
				long ngroups = (ngroupsStr != null) ? (long) Double.parseDouble(ngroupsStr) : -1;
				
				//execute basic grouped aggregate (extract and preagg)
				if( broadcastGroups ) {
					DataSet<Tuple2<MatrixIndexes,MatrixBlock>> pbm = flec.getBinaryBlockDataSetHandleForVariable( groupsVar );
					groupWeightedCells = target
							.flatMap(new ExtractGroupBroadcast(mc1.getColsPerBlock(), ngroups, _optr)).withBroadcastSet(pbm,  "bcastVar");
				}
				else { //general case
					
					//replicate groups if necessary
					if( mc1.getNumColBlocks() > 1 ) {
						groups = groups.flatMap(
							new ReplicateVectorFunction(false, mc1.getNumColBlocks() ));
					}
					
					groupWeightedCells = groups.join(target).where(0).equalTo(0).with(new ExtractGroupJoin(mc1.getColsPerBlock(), ngroups, _optr));		
				}
			}
			
			// Step 2: Make sure we have brlen required while creating <MatrixIndexes, MatrixCell> 
			if(mc1.getRowsPerBlock() == -1) {
				throw new DMLRuntimeException("The block sizes are not specified for grouped aggregate");
			}
			int brlen = mc1.getRowsPerBlock();
			
			// Step 3: Now perform grouped aggregate operation (either on combiner side or reducer side)
			DataSet<Tuple2<MatrixIndexes, MatrixCell>> out = null;
			if(_optr instanceof CMOperator && ((CMOperator) _optr).isPartialAggregateOperator() 
				|| _optr instanceof AggregateOperator ) {
				out = groupWeightedCells.groupBy(0).combineGroup(new PerformGroupByAggInCombiner(_optr))
						.map(new CreateMatrixCell(brlen, _optr));
			}
			else {
				// Use groupby key because partial aggregation is not supported
				out = groupWeightedCells.groupBy(0)
						.reduceGroup(new PerformGroupByAggInReducer(_optr))
						.map(new CreateMatrixCell(brlen, _optr));
			}
			
			// Step 4: Set output characteristics and DataSet handle 
			setOutputCharacteristicsForGroupedAgg(mc1, mcOut, out);
			
			//store output DataSet handle
			flec.setDataSetHandleForVariable(output.getName(), out);
			flec.addLineageDataSet( output.getName(), params.get(Statement.GAGG_TARGET) );
			if ( params.get(Statement.GAGG_WEIGHTS) != null ) {
				flec.addLineageDataSet(output.getName(), params.get(Statement.GAGG_WEIGHTS) );
			}*/
		}
		else if ( opcode.equalsIgnoreCase("rmempty") ) 
		{
			String rddInVar = params.get("target");
			String rddOffVar = params.get("offset");
			
			//get input DataSet handle
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in = flec.getBinaryBlockDataSetHandleForVariable( rddInVar );
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> off;
			MatrixCharacteristics mcIn = flec.getMatrixCharacteristics(rddInVar);
			boolean rows = flec.getScalarInput(params.get("margin"), ValueType.STRING, true).getStringValue().equals("rows");
			long maxDim = flec.getScalarInput(params.get("maxdim"), ValueType.DOUBLE, false).getLongValue();
			long brlen = mcIn.getRowsPerBlock();
			long bclen = mcIn.getColsPerBlock();
			long numRep = (long)Math.ceil( rows ? (double)mcIn.getCols()/bclen : (double)mcIn.getRows()/brlen);
			
			//execute remove empty rows/cols operation
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out;
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> broadcastOff;

			if(_bRmEmptyBC){
				broadcastOff = flec.getBinaryBlockDataSetHandleForVariable( rddOffVar );
				// Broadcast offset vector
				out = in
					.flatMap(new DataSetRemoveEmptyFunctionInMem(rows, maxDim, brlen, bclen)).withBroadcastSet(broadcastOff, "bcastVar");
			}
			else {
				off = flec.getBinaryBlockDataSetHandleForVariable( rddOffVar );
				out = in
					.join( off.flatMap(new ReplicateVectorFunction(!rows,numRep)) ).where(0).equalTo(0)
					.with(new DataSetRemoveEmptyFunction(rows, maxDim, brlen, bclen));
			}				

			out = DataSetAggregateUtils.mergeByKey(out);
			
			//store output DataSet handle
			flec.setDataSetHandleForVariable(output.getName(), out);
			flec.addLineageDataSet(output.getName(), rddInVar);
			if(!_bRmEmptyBC)
				flec.addLineageDataSet(output.getName(), rddOffVar);
			
			//update output statistics (required for correctness)
			MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
			mcOut.set(rows?maxDim:mcIn.getRows(), rows?mcIn.getCols():maxDim, (int)brlen, (int)bclen, mcIn.getNonZeros());
		}
		else if ( opcode.equalsIgnoreCase("replace") ) 
		{	
			//get input DataSet handle
			String rddVar = params.get("target");
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in1 = flec.getBinaryBlockDataSetHandleForVariable( rddVar );
			MatrixCharacteristics mcIn = flec.getMatrixCharacteristics(rddVar);
			
			//execute replace operation
			double pattern = Double.parseDouble( params.get("pattern") );
			double replacement = Double.parseDouble( params.get("replacement") );
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out = 
					in1.map(new DataSetReplaceFunction(pattern, replacement));
			
			//store output rdd handle
			flec.setDataSetHandleForVariable(output.getName(), out);
			flec.addLineageDataSet(output.getName(), rddVar);
			
			//update output statistics (required for correctness)
			MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
			mcOut.set(mcIn.getRows(), mcIn.getCols(), mcIn.getRowsPerBlock(), mcIn.getColsPerBlock(), (pattern!=0 && replacement!=0)?mcIn.getNonZeros():-1);
		}
		else if ( opcode.equalsIgnoreCase("rexpand") ) 
		{
			String rddInVar = params.get("target");
			
			//get input DataSet handle
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in = flec.getBinaryBlockDataSetHandleForVariable( rddInVar );
			MatrixCharacteristics mcIn = flec.getMatrixCharacteristics(rddInVar);
			double maxVal = Double.parseDouble( params.get("max") );
			long lmaxVal = UtilFunctions.toLong(maxVal);
			boolean dirRows = params.get("dir").equals("rows");
			boolean cast = Boolean.parseBoolean(params.get("cast"));
			boolean ignore = Boolean.parseBoolean(params.get("ignore"));
			long brlen = mcIn.getRowsPerBlock();
			long bclen = mcIn.getColsPerBlock();
			
			//execute remove empty rows/cols operation
			DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out = in
					.flatMap(new DataSetRExpandFunction(maxVal, dirRows, cast, ignore, brlen, bclen));		
			out = DataSetAggregateUtils.mergeByKey(out);
			
			//store output DataSet handle
			flec.setDataSetHandleForVariable(output.getName(), out);
			flec.addLineageDataSet(output.getName(), rddInVar);
			
			//update output statistics (required for correctness)
			MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
			mcOut.set(dirRows?lmaxVal:mcIn.getRows(), dirRows?mcIn.getRows():lmaxVal, (int)brlen, (int)bclen, -1);
		}
		else if ( opcode.equalsIgnoreCase("transform") ) 
		{
			// perform data transform on Flink
			try {
				DataTransform.flDataTransform(
						this, 
						new FrameObject[] { (FrameObject) flec.getVariable(params.get("target")) }, 
						new MatrixObject[] { (MatrixObject) flec.getVariable(output.getName()) }, ec);
			} catch (Exception e) {
				throw new DMLRuntimeException(e);
			}
		}
	}
	
	 

	public static class DataSetReplaceFunction implements MapFunction<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>> 
	{
		private static final long serialVersionUID = 6576713401901671659L;
		
		private double _pattern; 
		private double _replacement;
		
		public DataSetReplaceFunction(double pattern, double replacement) 
		{
			_pattern = pattern;
			_replacement = replacement;
		}
		
		@Override
		public Tuple2<MatrixIndexes,MatrixBlock> map(Tuple2<MatrixIndexes,MatrixBlock> arg0) 
			throws Exception 
		{
			return new Tuple2(arg0.f0, (MatrixBlock) arg0.f1.replaceOperations(new MatrixBlock(), _pattern, _replacement));
		}
	}
	
	

	 

	public static class DataSetRemoveEmptyFunction implements FlatJoinFunction<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>> 
	{
		private static final long serialVersionUID = 4906304771183325289L;

		private boolean _rmRows; 
		private long _len;
		private long _brlen;
		private long _bclen;
				
		public DataSetRemoveEmptyFunction(boolean rmRows, long len, long brlen, long bclen) 
		{
			_rmRows = rmRows;
			_len = len;
			_brlen = brlen;
			_bclen = bclen;
		}

		@Override
		public void join(Tuple2<MatrixIndexes, MatrixBlock> a, Tuple2<MatrixIndexes, MatrixBlock> b, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out)
			throws Exception 
		{
			//prepare inputs (for internal api compatibility)
			IndexedMatrixValue data = FlinkUtils.toIndexedMatrixBlock(a.f0,a.f1);
			IndexedMatrixValue offsets = FlinkUtils.toIndexedMatrixBlock(b.f0,b.f1);
			
			//execute remove empty operations
			ArrayList<IndexedMatrixValue> in = new ArrayList<IndexedMatrixValue>();
			LibMatrixReorg.rmempty(data, offsets, _rmRows, _len, _brlen, _bclen, in);

			//prepare and return outputs
			FlinkUtils.fromIndexedMatrixBlock(in, out);
		}
	}
	
	


	 

	public static class DataSetRemoveEmptyFunctionInMem extends RichFlatMapBroadcastFunction<Tuple2<MatrixIndexes,MatrixBlock>,Tuple2<MatrixIndexes,MatrixBlock>>
	{
		private static final long serialVersionUID = 4906304771183325289L;

		private boolean _rmRows; 
		private long _len;
		private long _brlen;
		private long _bclen;
				
		public DataSetRemoveEmptyFunctionInMem(boolean rmRows, long len, long brlen, long bclen) 
		{
			_rmRows = rmRows;
			_len = len;
			_brlen = brlen;
			_bclen = bclen;
		}

		@Override
		public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> arg0, Collector<Tuple2<MatrixIndexes,MatrixBlock>> out)
			throws Exception 
		{
			//prepare inputs (for internal api compatibility)
			IndexedMatrixValue data = FlinkUtils.toIndexedMatrixBlock(arg0.f0, arg0.f1);
			//IndexedMatrixValue offsets = FlinkUtils.toIndexedMatrixBlock(arg0._1(),arg0._2()._2());
			IndexedMatrixValue offsets = null;
			if(_rmRows)
				offsets = FlinkUtils.toIndexedMatrixBlock(arg0.f0, _pbc.get(arg0.f0.getRowIndex()).get(1L));
			else
				offsets = FlinkUtils.toIndexedMatrixBlock(arg0.f0, _pbc.get(1L).get(arg0.f0.getColumnIndex()));
			
			//execute remove empty operations
			ArrayList<IndexedMatrixValue> inA = new ArrayList<IndexedMatrixValue>();
			LibMatrixReorg.rmempty(data, offsets, _rmRows, _len, _brlen, _bclen, inA);

			//prepare and return outputs
			FlinkUtils.fromIndexedMatrixBlock(inA, out);
		}
	}
	
	

	 

	public static class DataSetRExpandFunction implements FlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>,Tuple2<MatrixIndexes,MatrixBlock>>
	{
		private static final long serialVersionUID = -6153643261956222601L;
		
		private double _maxVal;
		private boolean _dirRows;
		private boolean _cast;
		private boolean _ignore;
		private long _brlen;
		private long _bclen;
		
		public DataSetRExpandFunction(double maxVal, boolean dirRows, boolean cast, boolean ignore, long brlen, long bclen) 
		{
			_maxVal = maxVal;
			_dirRows = dirRows;
			_cast = cast;
			_ignore = ignore;
			_brlen = brlen;
			_bclen = bclen;
		}

		@Override
		public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> arg0, Collector<Tuple2<MatrixIndexes,MatrixBlock>> out)
			throws Exception 
		{
			//prepare inputs (for internal api compatibility)
			IndexedMatrixValue data = FlinkUtils.toIndexedMatrixBlock(arg0.f0,arg0.f1);
			
			//execute rexpand operations
			ArrayList<IndexedMatrixValue> in = new ArrayList<IndexedMatrixValue>();
			LibMatrixReorg.rexpand(data, _maxVal, _dirRows, _cast, _ignore, _brlen, _bclen, in);
			
			//prepare and return outputs
			FlinkUtils.fromIndexedMatrixBlock(in, out);
		}
	}
	
	public static class DataSetMapGroupedAggFunction extends RichFlatMapBroadcastFunction<Tuple2<MatrixIndexes,MatrixBlock>,Tuple2<MatrixIndexes,MatrixBlock>> 
	{
		private static final long serialVersionUID = 6795402640178679851L;

		private Operator _op = null;
		private int _ngroups = -1;
		private int _brlen = -1;
		private int _bclen = -1;
		
		public DataSetMapGroupedAggFunction(Operator op, int ngroups, int brlen, int bclen) 
		{
			_op = op;
			_ngroups = ngroups;
			_brlen = brlen;
			_bclen = bclen;
		}
		
		@Override
		public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> arg0, Collector<Tuple2<MatrixIndexes,MatrixBlock>> out)
			throws Exception 
		{
			//get all inputs
			MatrixIndexes ix = arg0.f0;
			MatrixBlock target = arg0.f1;
			MatrixBlock groups = _pbc.get(ix.getRowIndex()).get(1L);
			
			//execute map grouped aggregate operations
			IndexedMatrixValue in1 = FlinkUtils.toIndexedMatrixBlock(ix, target);
			ArrayList<IndexedMatrixValue> outlist = new ArrayList<IndexedMatrixValue>();
			OperationsOnMatrixValues.performMapGroupedAggregate(_op, in1, groups, _ngroups, _brlen, _bclen, outlist);
			
			//output all result blocks
			FlinkUtils.fromIndexedMatrixBlock(outlist, out);
		}
	}
	
	

	 

	public static class CreateMatrixCell implements MapFunction<Tuple2<MatrixIndexes,WeightedCell>, Tuple2<MatrixIndexes,MatrixCell>> 
	{
		private static final long serialVersionUID = -5783727852453040737L;
		
		int brlen; Operator op;
		public CreateMatrixCell(int brlen, Operator op) {
			this.brlen = brlen;
			this.op = op;
		}

		@Override
		public Tuple2<MatrixIndexes,MatrixCell> map(Tuple2<MatrixIndexes,WeightedCell> t) 
			throws Exception 
		{
			WeightedCell kv = t.f1;
			double val = -1;
			if(op instanceof CMOperator)
			{
				AggregateOperationTypes agg=((CMOperator)op).aggOpType;
				switch(agg)
				{
				case COUNT:
					val = kv.getWeight();
					break;
				case MEAN:
					val = kv.getValue();
					break;
				case CM2:
					val = kv.getValue()/ kv.getWeight();
					break;
				case CM3:
					val = kv.getValue()/ kv.getWeight();
					break;
				case CM4:
					val = kv.getValue()/ kv.getWeight();
					break;
				case VARIANCE:
					val = kv.getValue()/kv.getWeight();
					break;
				default:
					throw new DMLRuntimeException("Invalid aggreagte in CM_CV_Object: " + agg);
				}
			}
			else
			{
				//avoid division by 0
				val = kv.getValue()/kv.getWeight();
			}
			
			return new Tuple2(t.f0, new MatrixCell(val));
		}
	}
	
	

	/*
	 * 
	 * @param mc1
	 * @param mcOut
	 * @param out
	 * @throws DMLRuntimeException
	 */

	public void setOutputCharacteristicsForGroupedAgg(MatrixCharacteristics mc1, MatrixCharacteristics mcOut, DataSet<Tuple2<MatrixIndexes, MatrixCell>> out) 
		throws DMLRuntimeException 
	{
		throw new UnsupportedOperationException("FlinkUtils.cacheBinaryCell not yet implemented");
		/*
		if(!mcOut.dimsKnown()) {
			if(!mc1.dimsKnown()) {
				throw new DMLRuntimeException("The output dimensions are not specified for grouped aggregate");
			}
			
			if ( params.get(Statement.GAGG_NUM_GROUPS) != null) {
				int ngroups = (int) Double.parseDouble(params.get(Statement.GAGG_NUM_GROUPS));
				mcOut.set(ngroups, mc1.getCols(), -1, -1); //grouped aggregate with cell output
			}
			else {
				//out = FlinkUtils.cacheBinaryCellRDD(out);
				mcOut.set(FlinkUtils.computeMatrixCharacteristics(out));
				mcOut.setBlockSize(-1, -1); //grouped aggregate with cell output
			}
		}
		*/
	}
}

