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
import java.lang.reflect.InvocationTargetException;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.TraceFileReaderFoundation;

/**
 * This trace file reader allows processing the output of the vmstat command.
 * This is typically showing the utisation of a single virtual or physical
 * machine. Note that only memory and processor utilisation details are recorded
 * by vmstat. If additional information is needed then that needs to be
 * separately recorded. E.g., simultaneously to running vmstat one can record
 * the read and sent data from/to the network. This would provide a more
 * comprehensive representation of the machine's workload. If such trace is
 * collected, then one can use the class NetSpeedParser to add networking
 * related information to the loaded trace.
 * 
 * Jobs are loaded so that each second's worth of workload is set to be
 * dependent on the completion of the previous second's workload. This makes it
 * easy to identify jobs that belong to the same virtual/physical machine as
 * these jobs are all dependent on each other.
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2017"
 */
public class VMStatParser extends TraceFileReaderFoundation {
	private final String jobidPrefix;

	public VMStatParser(String fileName, int from, int to, boolean allowReadingFurther, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("Raw VMStat format", fileName, from, to, allowReadingFurther, jobType);
		jobidPrefix = new File(fileName).getName() + "-";
	}

	private String[] splitLine(final String line) {
		final String lT = line.trim();
		String[] s = lT.split(",");
		if (s.length != 17) {
			s = lT.split("\\s+");
		}
		return s;
	}

	@Override
	protected boolean isTraceLine(final String line) {
		String[] pieces = splitLine(line);
		if (pieces.length == 17) {
			for (String piece : pieces) {
				if (piece.matches("\\p{Alpha}")) {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	protected void metaDataCollector(String line) {
	}

	private Job previousJob = null;
	private long jobseq = 0;
	private long timeInc = 1;
	private final long onegiginkb = 1024 * 1024;

	// Assumptions: 4 cores! data collection every second, 1 G mem
	@Override
	protected Job createJobFromLine(String line)
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		String[] jobdata = splitLine(line);
		// 4 ==> core count
		int procs = 4;
		double perProcTime = ((double) (Integer.parseInt(jobdata[12]) + Integer.parseInt(jobdata[13])
				+ Integer.parseInt(jobdata[15]))) / 100.0;
		long usedMem = (onegiginkb - Long.parseLong(jobdata[3]) - Long.parseLong(jobdata[4])
				- Long.parseLong(jobdata[5])) / procs;
		previousJob = jobCreator.newInstance(
				// id
				jobidPrefix + jobseq,
				// submit time:
				jobseq,
				// queueing time:
				0,
				// execution time:
				1,
				// Number of processors
				procs,
				// average execution time
				perProcTime,
				// amount of memory used at the moment
				usedMem,
				// User name:
				"USER",
				// Group membership:
				"GROUP",
				// executable name:
				"EXEC",
				// No preceding job
				previousJob, 0);
		jobseq += timeInc;
		return previousJob;
	}

}
