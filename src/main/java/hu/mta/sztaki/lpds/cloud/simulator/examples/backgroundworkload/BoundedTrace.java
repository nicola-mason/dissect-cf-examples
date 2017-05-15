/*
 *  ========================================================================
 *  DISSECT-CF Examples
 *  ========================================================================
 *  
 *  This file is part of DISSECT-CF Examples.
 *  
 *  DISSECT-CF Examples is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or (at
 *  your option) any later version.
 *  
 *  DISSECT-CF Examples is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with DISSECT-CF Examples.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  (C) Copyright 2014-2015, Gabor Kecskemeti (kecskemeti.gabor@sztaki.mta.hu)
 *  (C) Copyright 2016-2017, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 */
package hu.mta.sztaki.lpds.cloud.simulator.examples.backgroundworkload;

import java.util.ArrayList;
import java.util.List;

import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.DCFJob;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.TraceManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.TraceProducerFoundation;

/**
 * Limits the jobs accessible to those that are between the given limits in its
 * constructor.
 */
public class BoundedTrace extends TraceProducerFoundation {
	/**
	 * The list of jobs to be sent over.
	 */
	private final ArrayList<Job> myJobArray;

	public BoundedTrace(List<Job> completeList, int startFrom, int finishAt)
			throws NoSuchMethodException, SecurityException {
		super(DCFJob.class);
		myJobArray = new ArrayList<Job>(completeList.subList(startFrom, finishAt));
	}

	@Override
	public List<Job> getAllJobs() throws TraceManagementException {
		return myJobArray;
	}

	@Override
	public List<Job> getJobs(int num) throws TraceManagementException {
		throw new TraceManagementException("Not implemented", null);
	}
}
