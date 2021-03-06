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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.Well1024a;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.api.DMLScript.RUNTIME_PLATFORM;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.DataGenOp;
import org.apache.sysml.hops.Hop.DataGenMethod;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.lops.DataGen;
import org.apache.sysml.lops.Lop;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetConverterUtils;
import org.apache.sysml.runtime.instructions.flink.utils.SamplingUtils;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.LibMatrixDatagen;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixCell;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.RandomMatrixGenerator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.util.UtilFunctions;
import org.apache.sysml.utils.Statistics;

public class RandFLInstruction extends UnaryFLInstruction
{
	//internal configuration
	private static final long INMEMORY_NUMBLOCKS_THRESHOLD = 1024 * 1024;

	private DataGenMethod method = DataGenMethod.INVALID;

	private long rows;
	private long cols;
	private int rowsInBlock;
	private int colsInBlock;
	private double minValue;
	private double maxValue;
	private double sparsity;
	private String pdf;
	private String pdfParams;
	private long seed=0;
	private String dir;
	private double seq_from;
	private double seq_to;
	private double seq_incr;

	//sample specific attributes
	private boolean replace;

	public RandFLInstruction (Operator op, DataGenMethod mthd, CPOperand in, CPOperand out, long rows, long cols,
							  int rpb, int cpb, double minValue, double maxValue, double sparsity, long seed, String dir,
							  String probabilityDensityFunction, String pdfParams, String opcode, String istr)
	{
		super(op, in, out, opcode, istr);

		this.method = mthd;
		this.rows = rows;
		this.cols = cols;
		this.rowsInBlock = rpb;
		this.colsInBlock = cpb;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.sparsity = sparsity;
		this.seed = seed;
		this.dir = dir;
		this.pdf = probabilityDensityFunction;
		this.pdfParams = pdfParams;

	}

	public RandFLInstruction(Operator op, DataGenMethod mthd, CPOperand in, CPOperand out,
							 long rows, long cols, int rpb, int cpb, double seqFrom,
							 double seqTo, double seqIncr, String opcode, String istr)
	{
		super(op, in, out, opcode, istr);
		this.method = mthd;
		this.rows = rows;
		this.cols = cols;
		this.rowsInBlock = rpb;
		this.colsInBlock = cpb;
		this.seq_from = seqFrom;
		this.seq_to = seqTo;
		this.seq_incr = seqIncr;
	}

	public RandFLInstruction(Operator op, DataGenMethod mthd, CPOperand in,
							 CPOperand out, long rows, long cols, int rpb, int cpb,
							 double maxValue, boolean replace, long seed, String opcode,
							 String istr) {
		super(op, in, out, opcode, istr);

		this.method = mthd;
		this.rows = rows;
		this.cols = cols;
		this.rowsInBlock = rpb;
		this.colsInBlock = cpb;
		this.maxValue = maxValue;
		this.replace = replace;
		this.seed = seed;
	}

	public long getRows() {
		return rows;
	}

	public void setRows(long rows) {
		this.rows = rows;
	}

	public long getCols() {
		return cols;
	}

	public void setCols(long cols) {
		this.cols = cols;
	}

	public int getRowsInBlock() {
		return rowsInBlock;
	}

	public void setRowsInBlock(int rowsInBlock) {
		this.rowsInBlock = rowsInBlock;
	}

	public int getColsInBlock() {
		return colsInBlock;
	}

	public void setColsInBlock(int colsInBlock) {
		this.colsInBlock = colsInBlock;
	}

	public double getMinValue() {
		return minValue;
	}

	public void setMinValue(double minValue) {
		this.minValue = minValue;
	}

	public double getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(double maxValue) {
		this.maxValue = maxValue;
	}

	public double getSparsity() {
		return sparsity;
	}

	public void setSparsity(double sparsity) {
		this.sparsity = sparsity;
	}

	/**
	 *
	 * @param str
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static RandFLInstruction parseInstruction(String str)
		throws DMLRuntimeException
	{
		String[] s = InstructionUtils.getInstructionPartsWithValueType ( str );
		String opcode = s[0];

		DataGenMethod method = DataGenMethod.INVALID;
		if ( opcode.equalsIgnoreCase(DataGen.RAND_OPCODE) ) {
			method = DataGenMethod.RAND;
			InstructionUtils.checkNumFields ( str, 12 );
		}
		else if ( opcode.equalsIgnoreCase(DataGen.SEQ_OPCODE) ) {
			method = DataGenMethod.SEQ;
			// 8 operands: rows, cols, rpb, cpb, from, to, incr, outvar
			InstructionUtils.checkNumFields ( str, 8 );
		}
		else if ( opcode.equalsIgnoreCase(DataGen.SAMPLE_OPCODE) ) {
			method = DataGenMethod.SAMPLE;
			// 7 operands: range, size, replace, seed, rpb, cpb, outvar
			InstructionUtils.checkNumFields ( str, 7 );
		}

		Operator op = null;
		// output is specified by the last operand
		CPOperand out = new CPOperand(s[s.length-1]);

		if ( method == DataGenMethod.RAND ) {
			long rows = -1, cols = -1;
			if (!s[1].contains( Lop.VARIABLE_NAME_PLACEHOLDER)) {
				rows = Double.valueOf(s[1]).longValue();
			}
			if (!s[2].contains( Lop.VARIABLE_NAME_PLACEHOLDER)) {
				cols = Double.valueOf(s[2]).longValue();
			}

			int rpb = Integer.parseInt(s[3]);
			int cpb = Integer.parseInt(s[4]);

			double minValue = -1, maxValue = -1;
			if (!s[5].contains( Lop.VARIABLE_NAME_PLACEHOLDER)) {
				minValue = Double.valueOf(s[5]).doubleValue();
			}
			if (!s[6].contains( Lop.VARIABLE_NAME_PLACEHOLDER)) {
				maxValue = Double.valueOf(s[6]).doubleValue();
			}

			double sparsity = Double.parseDouble(s[7]);

			long seed = DataGenOp.UNSPECIFIED_SEED;
			if (!s[8].contains( Lop.VARIABLE_NAME_PLACEHOLDER)) {
				seed = Long.parseLong(s[8]);
			}

			String dir = s[9];
			String pdf = s[10];
			String pdfParams = s[11];

			return new RandFLInstruction(op, method, null, out, rows, cols, rpb, cpb, minValue, maxValue, sparsity, seed, dir, pdf, pdfParams, opcode, str);
		}
		else if ( method == DataGenMethod.SEQ) {
			// Example Instruction: CP:seq:11:1:1000:1000:1:0:-0.1:scratch_space/_p7932_192.168.1.120//_t0/:mVar1
			long rows = Double.valueOf(s[1]).longValue();
			long cols = Double.valueOf(s[2]).longValue();
			int rpb = Integer.parseInt(s[3]);
			int cpb = Integer.parseInt(s[4]);

			double from, to, incr;
			from = to = incr = Double.NaN;
			if (!s[5].contains( Lop.VARIABLE_NAME_PLACEHOLDER)) {
				from = Double.valueOf(s[5]);
			}
			if (!s[6].contains( Lop.VARIABLE_NAME_PLACEHOLDER)) {
				to   = Double.valueOf(s[6]);
			}
			if (!s[7].contains( Lop.VARIABLE_NAME_PLACEHOLDER)) {
				incr = Double.valueOf(s[7]);
			}

			CPOperand in = null;
			return new RandFLInstruction(op, method, in, out, rows, cols, rpb, cpb, from, to, incr, opcode, str);
		}
		else if ( method == DataGenMethod.SAMPLE)
		{
			// Example Instruction: SPARK:sample:10:100:false:1000:1000:_mVar2·MATRIX·DOUBLE
			double max = 0;
			long rows = 0, cols;
			boolean replace = false;

			if (!s[1].contains( Lop.VARIABLE_NAME_PLACEHOLDER))
				max = Double.valueOf(s[1]);
			if (!s[2].contains( Lop.VARIABLE_NAME_PLACEHOLDER))
				rows = Double.valueOf(s[2]).longValue();
			cols = 1;

			if (!s[3].contains( Lop.VARIABLE_NAME_PLACEHOLDER))
				replace = Boolean.valueOf(s[3]);

			long seed = Long.parseLong(s[4]);
			int rpb = Integer.parseInt(s[5]);
			int cpb = Integer.parseInt(s[6]);

			return new RandFLInstruction(op, method, null, out, rows, cols, rpb, cpb, max, replace, seed, opcode, str);
		}
		else
			throw new DMLRuntimeException("Unrecognized data generation method: " + method);
	}

	@Override
	public void processInstruction( ExecutionContext ec )
		throws DMLRuntimeException
	{
		FlinkExecutionContext flec = (FlinkExecutionContext)ec;

		//process specific datagen operator
		switch( method ) {
			case RAND: generateRandData(flec); break;
			case SEQ: generateSequence(flec); break;
			case SAMPLE: generateSample(flec); break;
			default:
				throw new DMLRuntimeException("Invalid datagen method: "+method);
		}
	}

	/**
	 *
	 * @param flec
	 * @throws DMLRuntimeException
	 */
	private void generateRandData(FlinkExecutionContext flec)
		throws DMLRuntimeException
	{
		//step 1: generate pseudo-random seed (because not specified) 
		long lSeed = seed; //seed per invocation
		if( lSeed == DataGenOp.UNSPECIFIED_SEED )
			lSeed = DataGenOp.generateRandomSeed();

		if( LOG.isTraceEnabled() )
			LOG.trace("Process RandSPInstruction rand with seed = "+lSeed+".");

		//step 2: potential in-memory rand operations if applicable
		if( isMemAvail(rows, cols, sparsity, minValue, maxValue)
			&&  DMLScript.rtplatform != RUNTIME_PLATFORM.SPARK )
		{
			RandomMatrixGenerator rgen = LibMatrixDatagen.createRandomMatrixGenerator(
				pdf, (int)rows, (int)cols, rowsInBlock, colsInBlock,
				sparsity, minValue, maxValue, pdfParams);
			MatrixBlock mb = MatrixBlock.randOperations(rgen, lSeed);

			flec.setMatrixOutput(output.getName(), mb);
			Statistics.decrementNoOfExecutedSPInst();
			return;
		}

		//step 3: seed generation 
		DataSet<Tuple2<MatrixIndexes, Tuple2<Long, Long>>> seedsDataSet = null;
		Well1024a bigrand = LibMatrixDatagen.setupSeedsForRand(lSeed);
		long[] nnz = LibMatrixDatagen.computeNNZperBlock(rows, cols, rowsInBlock, colsInBlock, sparsity);
		double hdfsBlkSize = InfrastructureAnalyzer.getHDFSBlockSize();
		long numBlocks = nnz.length;
		long numColBlocks = (long)Math.ceil((double)cols/(double)colsInBlock);

		//a) in-memory seed dataset construction 
		if( numBlocks < INMEMORY_NUMBLOCKS_THRESHOLD )
		{
			ArrayList<Tuple2<MatrixIndexes, Tuple2<Long, Long>>> seeds =
				new ArrayList<Tuple2<MatrixIndexes, Tuple2<Long, Long>>>();
			double partSize = 0;
			for( long i=0; i<numBlocks; i++ ) {
				long r = 1 + i/numColBlocks;
				long c = 1 + i%numColBlocks;
				MatrixIndexes indx = new MatrixIndexes(r, c);
				Long seedForBlock = bigrand.nextLong();
				seeds.add(new Tuple2<MatrixIndexes, Tuple2<Long, Long>>(indx,
					new Tuple2<Long, Long>(seedForBlock, nnz[(int)i])));
				partSize += nnz[(int)i] * 8 + 16;
			}

			//for load balancing: degree of parallelism such that ~128MB per partition
			int numPartitions = (int) Math.max(Math.min(partSize/hdfsBlkSize, numBlocks), 1);

			//create seeds dataset 
			seedsDataSet = flec.getFlinkContext().fromCollection(seeds);
		}
		//b) file-based seed dataset construction (for robustness wrt large number of blocks)
		else
		{
			String path = LibMatrixDatagen.generateUniqueSeedPath(dir);
			double partSize = 0;

			try
			{
				FileSystem fs = FileSystem.get(ConfigurationManager.getCachedJobConf());
				FSDataOutputStream fsOut = fs.create(new Path(path));
				PrintWriter pw = new PrintWriter(fsOut);
				StringBuilder sb = new StringBuilder();
				for( long i=0; i<numBlocks; i++ ) {
					sb.append(1 + i/numColBlocks);
					sb.append(',');
					sb.append(1 + i%numColBlocks);
					sb.append(',');
					sb.append(bigrand.nextLong());
					sb.append(',');
					sb.append(nnz[(int)i]);
					pw.println(sb.toString());
					sb.setLength(0);
					partSize += nnz[(int)i] * 8 + 16;
				}
				pw.close();
				fsOut.close();
			}
			catch( IOException ex ) {
				throw new DMLRuntimeException(ex);
			}

			//for load balancing: degree of parallelism such that ~128MB per partition
			int numPartitions = (int) Math.max(Math.min(partSize/hdfsBlkSize, numBlocks), 1);

			//create seeds dataset 
			seedsDataSet = flec.getFlinkContext()
				.readTextFile(path)
				.map(new ExtractSeedTuple());
		}

		//step 4: execute rand instruction over seed input
		DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = seedsDataSet
			.map(new GenerateRandomBlock(rows, cols, rowsInBlock, colsInBlock,
				sparsity, minValue, maxValue, pdf, pdfParams));

		//step 5: output handling
		MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
		if(!mcOut.dimsKnown(true)) {
			//note: we cannot compute the nnz from sparsity because this would not reflect the 
			//actual number of non-zeros, except for extreme values of sparsity equals 0 or 1.
			long lnnz = (sparsity==0 || sparsity==1) ? (long) (sparsity*rows*cols) : -1;
			mcOut.set(rows, cols, rowsInBlock, colsInBlock, lnnz);
		}
		flec.setDataSetHandleForVariable(output.getName(), out);
	}

	/**
	 *
	 * @param flec
	 * @throws DMLRuntimeException
	 */
	private void generateSequence(FlinkExecutionContext flec)
		throws DMLRuntimeException
	{
		//sanity check valid increment
		if(seq_incr == 0) {
			throw new DMLRuntimeException("ERROR: While performing seq(" + seq_from + "," + seq_to + "," + seq_incr + ")");
		}

		//handle default 1 to -1 for special case of from>to
		seq_incr = LibMatrixDatagen.updateSeqIncr(seq_from, seq_to, seq_incr);

		if( LOG.isTraceEnabled() )
			LOG.trace("Process RandSPInstruction seq with seqFrom="+seq_from+", seqTo="+seq_to+", seqIncr"+seq_incr);

		//step 1: offset generation 
		DataSet<Double> offsetsDataSet = null;
		double hdfsBlkSize = InfrastructureAnalyzer.getHDFSBlockSize();
		long nnz = (long) Math.abs(Math.round((seq_to - seq_from)/seq_incr)) + 1;
		long numBlocks = (long)Math.ceil(((double)nnz)/rowsInBlock);

		//a) in-memory offset dataset construction 
		if( numBlocks < INMEMORY_NUMBLOCKS_THRESHOLD )
		{
			ArrayList<Double> offsets = new ArrayList<Double>();
			double partSize = 0;
			for( long i=0; i<numBlocks; i++ ) {
				double off = seq_from + seq_incr*i*rowsInBlock;
				offsets.add(off);
				partSize += rowsInBlock * 8 +16;
			}

			//for load balancing: degree of parallelism such that ~128MB per partition
			int numPartitions = (int) Math.max(Math.min(partSize/hdfsBlkSize, numBlocks), 1);

			//create offset dataset
			offsetsDataSet = flec.getFlinkContext().fromCollection(offsets);
		}
		//b) file-based offset dataset construction (for robustness wrt large number of blocks)
		else
		{
			String path = LibMatrixDatagen.generateUniqueSeedPath(dir);
			double partSize = 0;

			try
			{
				FileSystem fs = FileSystem.get(ConfigurationManager.getCachedJobConf());
				FSDataOutputStream fsOut = fs.create(new Path(path));
				PrintWriter pw = new PrintWriter(fsOut);
				for( long i=0; i<numBlocks; i++ ) {
					double off = seq_from + seq_incr*i*rowsInBlock;
					pw.println(off);
					partSize += rowsInBlock * 8 +16;
				}
				pw.close();
				fsOut.close();
			}
			catch( IOException ex ) {
				throw new DMLRuntimeException(ex);
			}

			//for load balancing: degree of parallelism such that ~128MB per partition
			int numPartitions = (int) Math.max(Math.min(partSize/hdfsBlkSize, numBlocks), 1);

			//create seeds dataset 
			offsetsDataSet = flec.getFlinkContext()
				.readTextFile(path)
				.map(new ExtractOffsetTuple());
		}

		//sanity check number of non-zeros
		if(nnz != rows && rows != -1) {
			throw new DMLRuntimeException("Incorrect number of non-zeros: " + nnz + " != " + rows);
		}

		//step 2: execute seq instruction over offset input
		DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = offsetsDataSet
			.map(new GenerateSequenceBlock(rowsInBlock, seq_from, seq_to, seq_incr));

		//step 3: output handling
		MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
		if(!mcOut.dimsKnown()) {
			mcOut.set(nnz, 1, rowsInBlock, colsInBlock, nnz);
		}
		flec.setDataSetHandleForVariable(output.getName(), out);
	}

	/**
	 * Helper function to construct a sample.
	 *
	 * @param flec
	 * @throws DMLRuntimeException
	 */
	private void generateSample(FlinkExecutionContext flec)
		throws DMLRuntimeException
	{
		if ( maxValue < rows && !replace )
			throw new DMLRuntimeException("Sample (size=" + rows + ") larger than population (size=" + maxValue + ") can only be generated with replacement.");

		if( LOG.isTraceEnabled() )
			LOG.trace("Process RandSPInstruction sample with range="+ maxValue +", size="+ rows +", replace="+ replace + ", seed=" + seed);

		// sampling rate that guarantees a sample of size >= sampleSizeLowerBound 99.99% of the time.
		double fraction = SamplingUtils.computeFractionForSampleSize((int)rows, UtilFunctions.toLong(maxValue), replace);

		Well1024a bigrand = LibMatrixDatagen.setupSeedsForRand(seed);

		// divide the population range across numPartitions by creating SampleTasks
		double hdfsBlockSize = InfrastructureAnalyzer.getHDFSBlockSize();
		long outputSize = MatrixBlock.estimateSizeDenseInMemory(rows,1);
		int numPartitions = (int) Math.ceil((double)outputSize/hdfsBlockSize);
		long partitionSize = (long) Math.ceil(maxValue/numPartitions);

		ArrayList<SampleTask> offsets = new ArrayList<SampleTask>();
		long st = 1;
		while ( st <= maxValue ) {
			SampleTask s = new SampleTask();
			s.range_start = st;
			s.seed = bigrand.nextLong();

			offsets.add(s);
			st = st + partitionSize;
		}
		DataSet<SampleTask> offsetDataSet = flec.getFlinkContext().fromCollection(offsets);

		// Construct the sample in a distributed manner
		DataSet<Double> dataset = offsetDataSet.flatMap( (new GenerateSampleBlock(replace, fraction, (long)maxValue, partitionSize)) );

		// Randomize the sampled elements
		DataSet<Double> randomizedDataSet = dataset.map(new AttachRandom()).map(new DataSetConverterUtils.ExtractElement(1));

		// Trim the sampled list to required size & attach matrix indexes to randomized elements
		DataSet<Tuple2<MatrixIndexes, MatrixCell>> miDataSet = DataSetUtils.zipWithIndex(randomizedDataSet)
			.filter( new TrimSample(rows) )
			.map( new Double2MatrixCell() );

		MatrixCharacteristics mcOut = new MatrixCharacteristics(rows, 1, rowsInBlock, colsInBlock, rows);

		// Construct BinaryBlock representation
		DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mbDataSet =
			DataSetConverterUtils.binaryCellToBinaryBlock(flec.getFlinkContext(), miDataSet, mcOut, true);

		MatrixCharacteristics retDims = flec.getMatrixCharacteristics(output.getName());
		retDims.setNonZeros(rows);

		flec.setDataSetHandleForVariable(output.getName(), mbDataSet);
	}

	/**
	 * Private class that defines a sampling task. 
	 * The task produces a portion of sample from range [range_start, range_start+partitionSize].
	 *
	 */
	private static class SampleTask implements Serializable
	{
		private static final long serialVersionUID = -725284524434342939L;
		long seed;
		long range_start;

		public String toString() { return "(" + seed + "," + range_start +")"; }
	}

	/**
	 * Main class to perform distributed sampling.
	 *
	 * Each invocation of this FlatMapFunction produces a portion of sample 
	 * to be included in the final output. 
	 *
	 * The input range from which the sample is constructed is given by 
	 * [range_start, range_start+partitionSize].
	 *
	 * When replace=TRUE, the sample is produced by generating Poisson 
	 * distributed counts (denoting the number of occurrences) for each 
	 * element in the input range. 
	 *
	 * When replace=FALSE, the sample is produced by comparing a generated 
	 * random number against the required sample fraction.
	 *
	 * In the special case of fraction=1.0, the permutation of the input 
	 * range is computed, simply by creating DataSet of elements from input range.
	 *
	 */
	private static class GenerateSampleBlock implements FlatMapFunction<SampleTask, Double>
	{
		private double _frac;
		private boolean _replace;
		private long _maxValue, _partitionSize;

		public GenerateSampleBlock(boolean replace, double frac, long max, long psize)
		{
			_replace = replace;
			_frac = frac;
			_maxValue = max;
			_partitionSize = psize;
		}

		public GenerateSampleBlock(){
			
		}

		@Override
		public void flatMap(SampleTask t, Collector<Double> out)
			throws Exception {

			long st = t.range_start;
			long end = Math.min(t.range_start+_partitionSize, _maxValue);

			if ( _frac == 1.0 )
			{
				for(long i=st; i <= end; i++)
					out.collect((double)i);
			}
			else
			{
				if(_replace)
				{
					PoissonDistribution pdist = new PoissonDistribution( (_frac > 0.0 ? _frac :1.0) );
					for(long i=st; i <= end; i++)
					{
						int count = pdist.sample();
						while(count > 0) {
							out.collect((double)i);
							count--;
						}
					}
				}
				else
				{
					Random rnd = new Random(t.seed);
					for(long i=st; i <=end; i++)
						if ( rnd.nextDouble() < _frac )
							out.collect((double) i);
				}
			}
		}
	}

	/**
	 * Function that filters the constructed sample contain to required number of elements.
	 *
	 */
	private static class TrimSample implements FilterFunction<Tuple2<Long, Double>> {
		long _max;

		public TrimSample(long max) {
			_max = max;
		}
		public TrimSample() {
		}
		

		public boolean filter(Tuple2<Long, Double> v1) throws Exception {
			return ( v1.f0 < _max );
		}

	}

	/**
	 * Function to convert JavaDataSet of Doubles to JavaPairDataSet<MatrixIndexes, MatrixCell>
	 *
	 */
	private static class Double2MatrixCell implements MapFunction<Tuple2<Long, Double>, Tuple2<MatrixIndexes, MatrixCell>>
	{
		@Override
		public Tuple2<MatrixIndexes, MatrixCell> map(Tuple2<Long, Double> t)
			throws Exception {
			long rowID = t.f0+1;
			MatrixIndexes mi = new MatrixIndexes(rowID, 1);
			MatrixCell mc = new MatrixCell(t.f1);

			return new Tuple2<MatrixIndexes, MatrixCell>(mi, mc);
		}
	}

	/**
	 * Pair function to attach a random number as a key to input JavaDataSet.
	 * The produced JavaPairDataSet is subsequently used to randomize the sampled elements. 
	 *
	 */
	private static class AttachRandom extends RichMapFunction<Double, Tuple2<Double, Double>> {
		Random r = null;
		
		public AttachRandom(){
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			r = new Random();
		}
		
		@Override
		public Tuple2<Double, Double> map(Double t) throws Exception {
			return new Tuple2<Double,Double>( r.nextDouble(), t );
		}
	}

	/**
	 *
	 */
	private static class ExtractSeedTuple implements MapFunction<String, Tuple2<MatrixIndexes, Tuple2<Long, Long>>> {
		@Override
		public Tuple2<MatrixIndexes, Tuple2<Long, Long>> map(String arg)
			throws Exception
		{
			String[] parts = IOUtilFunctions.split(arg, ",");
			MatrixIndexes ix = new MatrixIndexes(
				Long.parseLong(parts[0]), Long.parseLong(parts[1]));
			Tuple2<Long,Long> seed = new Tuple2<Long,Long>(
				Long.parseLong(parts[2]), Long.parseLong(parts[3]));

			return new Tuple2<MatrixIndexes, Tuple2<Long, Long>>(ix,seed);
		}
	}

	/**
	 *
	 */
	private static class ExtractOffsetTuple implements MapFunction<String, Double> {
		@Override
		public Double map(String arg) throws Exception {
			return Double.parseDouble(arg);
		}
	}

	/**
	 *
	 */
	private static class GenerateRandomBlock implements MapFunction<Tuple2<MatrixIndexes, Tuple2<Long, Long> >, Tuple2<MatrixIndexes, MatrixBlock>>
	{
		private static final long serialVersionUID = 1616346120426470173L;

		private long _rlen;
		private long _clen;
		private int _brlen;
		private int _bclen;
		private double _sparsity;
		private double _min;
		private double _max;
		private String _pdf;
		private String _pdfParams;

		public GenerateRandomBlock(long rlen, long clen, int brlen, int bclen, double sparsity, double min, double max, String pdf, String pdfParams) {
			_rlen = rlen;
			_clen = clen;
			_brlen = brlen;
			_bclen = bclen;
			_sparsity = sparsity;
			_min = min;
			_max = max;
			_pdf = pdf;
			_pdfParams = pdfParams;
		}

		public GenerateRandomBlock(){
			
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> map(Tuple2<MatrixIndexes, Tuple2<Long, Long>> kv)
			throws Exception
		{
			//compute local block size: 
			MatrixIndexes ix = kv.f0;
			long blockRowIndex = ix.getRowIndex();
			long blockColIndex = ix.getColumnIndex();
			int lrlen = UtilFunctions.computeBlockSize(_rlen, blockRowIndex, _brlen);
			int lclen = UtilFunctions.computeBlockSize(_clen, blockColIndex, _bclen);

			long seed = kv.f1.f0;
			long blockNNZ = kv.f1.f1;

			MatrixBlock blk = new MatrixBlock();

			RandomMatrixGenerator rgen = LibMatrixDatagen.createRandomMatrixGenerator(
				_pdf, lrlen, lclen, lrlen, lclen,
				_sparsity, _min, _max, _pdfParams );

			blk.randOperationsInPlace(rgen, new long[]{blockNNZ}, null, seed);

			return new Tuple2<MatrixIndexes, MatrixBlock>(kv.f0, blk);
		}
	}

	/**
	 *
	 */
	private static class GenerateSequenceBlock implements MapFunction<Double, Tuple2<MatrixIndexes, MatrixBlock>>
	{
		private int _brlen;
		private double _global_seq_start;
		private double _global_seq_end;
		private double _seq_incr;


		public GenerateSequenceBlock(int brlen, double global_seq_start, double global_seq_end, double seq_incr) {
			_brlen = brlen;
			_global_seq_start = global_seq_start;
			_global_seq_end = global_seq_end;
			_seq_incr = seq_incr;
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> map(Double seq_from)
			throws Exception
		{
			double seq_to;
			if(_seq_incr > 0) {
				seq_to = Math.min(_global_seq_end, seq_from + _seq_incr*(_brlen-1));
			}
			else {
				seq_to = Math.max(_global_seq_end, seq_from + _seq_incr*(_brlen+1));
			}
			long globalRow = (long) ((seq_from-_global_seq_start)/_seq_incr + 1);
			long rowIndex = (long) Math.ceil((double)globalRow/(double)_brlen);

			MatrixIndexes indx = new MatrixIndexes(rowIndex, 1);
			MatrixBlock blk = MatrixBlock.seqOperations(seq_from, seq_to, _seq_incr);
			return new Tuple2<MatrixIndexes, MatrixBlock>(indx, blk);
		}
	}

	/**
	 * This will check if there is sufficient memory locally.  
	 * @return
	 */
	private boolean isMemAvail(long lRows, long lCols, double sparsity, double min, double max)
	{
		double size = (min == 0 && max == 0) ? OptimizerUtils.estimateSizeEmptyBlock(rows, cols):
			OptimizerUtils.estimateSizeExactSparsity(rows, cols, sparsity);

		return ( OptimizerUtils.isValidCPDimensions(rows, cols)
			&& OptimizerUtils.isValidCPMatrixSize(rows, cols, sparsity)
			&& size < OptimizerUtils.getLocalMemBudget() );
	}

}
