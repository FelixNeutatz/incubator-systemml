/**
 * IBM Confidential
 * OCO Source Materials
 * (C) Copyright IBM Corp. 2010, 2014
 * The source code for this program is not published or otherwise divested of its trade secrets, irrespective of what has been deposited with the U.S. Copyright Office.
 */

package com.ibm.bi.dml.hops.globalopt.enumerate;

import com.ibm.bi.dml.hops.globalopt.enumerate.InterestingProperty.InterestingPropertyType;


/**
 * 
 */
public class RewriteConfigBlocksize extends RewriteConfig
{
	@SuppressWarnings("unused")
	private static final String _COPYRIGHT = "Licensed Materials - Property of IBM\n(C) Copyright IBM Corp. 2010, 2014\n" +
                                             "US Government Users Restricted Rights - Use, duplication  disclosure restricted by GSA ADP Schedule Contract with IBM Corp.";
	
	//valid instance configurations
	private static int[] _defValues = GlobalEnumerationOptimizer.BLOCK_SIZES;  
	
	public RewriteConfigBlocksize()
	{
		super( RewriteConfigType.BLOCK_SIZE, -1 );
	}
	
	public RewriteConfigBlocksize(int value)
	{
		super( RewriteConfigType.BLOCK_SIZE, value );
	}
	
	@Override
	public int[] getDefinedValues()
	{
		return _defValues;
	}

	@Override
	public InterestingProperty getInterestingProperty()
	{
		//direct mapping from rewrite config to interesting property
		return new InterestingProperty(InterestingPropertyType.BLOCK_SIZE, getValue());
	}

}