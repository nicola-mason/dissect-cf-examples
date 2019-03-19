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
 *  (C) Copyright 2019, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 */
package uk.ac.ljmu.fet.cs.cloud.examples.autoscaler;

/**
 * State exchange mechanism between JobLaunchers, JobArrivalHandlers
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2019"
 */
public class Progress {
	/**
	 * The number of jobs that have reached the infrastructure's VMs
	 */
	private int jobsDispatched = 0;
	/**
	 * The number of jobs that have actually completed their tasks
	 */
	private int jobsDone = 0;
	/**
	 * The total number of jobs this simulation has
	 */
	private int jobCount = -1;
	/**
	 * The party interested in knowing when there are no more jobs in the trace
	 */
	private final TraceExhaustionCallback callback;

	/**
	 * Initiates the exchange mechanism and remembers who to send the callback.
	 * 
	 * @param callback When the trace is completed according to the records of this
	 *                 class, this object will get a message.
	 */
	public Progress(TraceExhaustionCallback callback) {
		this.callback = callback;
	}

	/**
	 * Remembers how many jobs were dispatched to their corresponding VMs. If all
	 * jobs have been dispatched, a message is printed on the screen.
	 */
	public void registerDispatch() {
		jobsDispatched++;
		if (jobsDispatched == jobCount) {
			System.out.println("Last job reached a VM");
		}
	}

	/**
	 * Remembers how many jobs finished their execution on their VMs. If no more
	 * jobs are coming, it will send the completion notification to the callback
	 * object.
	 */
	public void registerCompletion() {
		jobsDone++;
		// Finally let's see if there is any more need for the support mechanisms of the
		// simulation (e.g., autoscaler)
		if (jobsDone == jobCount) {
			callback.allJobsFinished();
		}
	}

	/**
	 * This tells the exchange mechanism about how many jobs it should expect
	 * 
	 * @param count the total number of jobs that we need to register
	 *              completion/dispatch for.
	 */
	public void setTotalJobCount(final int count) {
		if (jobCount != -1) {
			throw new IllegalStateException("Total job count was already set before");
		}
		jobCount = count;
	}

	/**
	 * Allows querying the number of jobs that were done so far.
	 * 
	 * @return count of jobs that finished their executions on their corresponding
	 *         VMs.
	 */
	public int getDoneJobCount() {
		return jobsDone;
	}
}
