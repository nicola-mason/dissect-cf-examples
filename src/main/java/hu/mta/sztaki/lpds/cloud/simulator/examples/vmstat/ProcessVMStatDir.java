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
 *  (C) Copyright 2017, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 */
package hu.mta.sztaki.lpds.cloud.simulator.examples.vmstat;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.GenericTraceProducer;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.TraceManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.TraceProducerFoundation;

/**
 * This reader expects a directory name as its input. The directory is expected
 * to have two files for each virtual/physical machine the trace contains data
 * about. File names with the prefix "vmsstat" will be loaded with the
 * VMStatParser class. For all jobs that were loaded from the "vmstat" prefixed
 * files, this class also loads additional network related information with the
 * help of the NetSpeedParser class.
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2017"
 */
public class ProcessVMStatDir extends TraceProducerFoundation {
	private final ArrayList<Job> jobs = new ArrayList<Job>();

	public ProcessVMStatDir(String dirName, Class<? extends Job> jobType)
			throws NoSuchMethodException, SecurityException {
		super(jobType);

		File dir = new File(dirName);
		if (!dir.isDirectory()) {
			throw new IllegalStateException(dirName + " is not a directory! Cannot process further.");
		}
		for (File f : dir.listFiles()) {
			// Maximum 10M lines are loaded per file
			GenericTraceProducer trace = new VMStatParser(f.getAbsolutePath(), 0, 10000000, false, jobType);
			try {
				// All loaded jobs are saved in the job cache of this class
				jobs.addAll(trace.getAllJobs());
			} catch (Exception e) {
				// if not really a trace
				System.err.println("Trace " + f.getAbsolutePath() + " did not produce jobs.");
			}
		}

		for (Job job : jobs) {
			ComplexJob pj = (ComplexJob) job;
			if (pj.rx < 0) {
				// This branch of the if only executes for the first jobs of a given trace.
				NetSpeedParser.populateNetData(pj, dirName, jobs);
			}
		}
	}

	public List<Job> getAllJobs() throws TraceManagementException {
		return jobs;
	}

	public List<Job> getJobs(int num) throws TraceManagementException {
		throw new UnsupportedOperationException("Only the entire trace can be queried from ProceddVMStatDir");
	}

}
