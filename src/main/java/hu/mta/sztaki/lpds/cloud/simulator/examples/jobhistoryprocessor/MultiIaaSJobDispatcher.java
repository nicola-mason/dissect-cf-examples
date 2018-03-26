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
 *  (C) Copyright 2016, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 *  (C) Copyright 2013-15, Gabor Kecskemeti (gkecskem@dps.uibk.ac.at,
 *   									  kecskemeti.gabor@sztaki.mta.hu)
 */
package hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.JobListAnalyser;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.GenericTraceProducer;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.TraceManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;

/**
 * A simple trace processor that creates as many VMs in the cloud as many is
 * required to host single jobs (e.g., if the job requires 1024 processors then
 * it will create 16 VMs with 64 cores if the PM with the largest size in the
 * cloud could host 64 core VMs). After the VMs are created the jobs are sent to
 * them. After the jobs terminate their hosting VMs are also terminated. The
 * VMIs for the VMs are assumed to be capable of running all the jobs in the
 * trace.
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2016-7"
 * @author "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems,
 *         MTA SZTAKI (c) 2012-5"
 */
public class MultiIaaSJobDispatcher extends Timed {

	/**
	 * Shows if the verbosity is switched on for the simulation run. Allows some
	 * entities in the dispatcher to produce more output.
	 */
	public static final boolean verbosity = System
			.getProperty("hu.mta.sztaki.lpds.cloud.simulator.examples.verbosity") != null;

	public static final long baseBillingPeriod = 3600000;

	/**
	 * Allows the termination of the processing of the trace
	 */
	private boolean isStopped = false;
	/**
	 * The list of jobs (i.e., the trace) in a more rapidly processable form
	 */
	protected Job[] jobs;
	/**
	 * The first unprocessed job in the trace
	 */
	protected int minindex = 0;
	/**
	 * the iaas services to be used for executing the jobs
	 */
	protected List<IaaSService> target;
	/**
	 * the list of repositories that belong to the target iaas services.
	 */
	protected List<Repository> repo;
	/**
	 * the list of Virtual Machine keepers - the list of VMs that might be alive
	 */
	protected Set<VMKeeper> pooledVMs = new TreeSet<VMKeeper>(VMKeeper.compareKeepers);
	/**
	 * the virtual appliance that will be used as the generic image for each VM in
	 * the clouds
	 */
	protected VirtualAppliance va;
	/**
	 * the first submission time
	 */
	protected long minsubmittime;
	/**
	 * maximum number of cores in the biggest physical machine
	 */
	protected long maxmachinecores = 0;
	/**
	 * maximum number of physical machines
	 */
	protected long maxIaaSmachines = 0;
	/**
	 * number of jobs ignored
	 */
	protected long ignorecounter = 0;
	/**
	 * number of VMs destroyed by this dispatcher - i.e., number of jobs completed
	 */
	protected long destroycounter = 0;
	/**
	 * the default processing power share to be requested during the resource
	 * allocation for the VMs - allows under-provisioning
	 */
	protected double useThisProcPower = Double.MAX_VALUE;
	/**
	 * the processing power specified before for the single core of the VM should be
	 * guaranteed
	 */
	protected boolean isMinimumProcPower = false;
	/**
	 * current IaaS service to be used for VM creation
	 */
	private int targetIndex = 0;

	public int reuseCounter = 0;

	public int actualVMCount = 0;

	/**
	 * Dispatcher setup. Fetches all jobs from the given trace producer and analyzes
	 * them to fill out min- and max-submit-times. Finally it analyzes and prepares
	 * the target IaaS services. The analysis is targeted at finding the PM capacity
	 * characteristics of each IaaS, while the preparatory step ensures the
	 * availability of the VA to be used for instantiating the VMs for the jobs.
	 * 
	 * WARNING: only uniformly prepared IaaS systems are supported right now (i.e.
	 * all of them having the same amount of PMs and all of their PMs are
	 * constructed with the same amount of resources)
	 * 
	 * @param producer
	 *            the trace
	 * @param target
	 *            the iaas systems to be used for submitting the trace to
	 */
	public MultiIaaSJobDispatcher(GenericTraceProducer producer, List<IaaSService> target)
			throws TraceManagementException {
		this.target = target;
		// Collecting the jobs
		List<Job> jobs = producer.getAllJobs();

		// Ensuring they are listed in submission order
		Collections.sort(jobs, JobListAnalyser.submitTimeComparator);
		// Analyzing the jobs for min and max submission time
		minsubmittime = JobListAnalyser.getEarliestSubmissionTime(jobs);
		// Transforming the job list for rapid access arrays:
		this.jobs = jobs.toArray(new Job[jobs.size()]);
		jobs.clear();

		// Preparing the repositories with VAs
		repo = new ArrayList<Repository>(target.size());
		va = new VirtualAppliance("test", 30, 0, false, 100000000);
		for (IaaSService iaas : target) {
			Repository currentRepo = iaas.repositories.get(0);
			repo.add(currentRepo);
			// actually registering the VA
			currentRepo.registerObject(va);
			// determining the maximum number of CPU cores available in a PM
			for (PhysicalMachine pm : iaas.machines) {
				double cores = pm.getCapacities().getRequiredCPUs();
				double pp = pm.getCapacities().getRequiredProcessingPower();
				if (cores > maxmachinecores) {
					maxmachinecores = (long) cores;
				}
				if (pp < useThisProcPower) {
					useThisProcPower = pp;
				}
			}
			if (iaas.machines.size() > maxIaaSmachines) {
				maxIaaSmachines = iaas.machines.size();
			}
		}

		// Ensuring we will receive a notification once the first job should be
		// submitted

		final long currentTime = Timed.getFireCount();
		final long msTime = minsubmittime * 1000;
		if (currentTime > msTime) {
			final long adjustTime = (long) Math.ceil((currentTime - msTime) / 1000f);
			minsubmittime += adjustTime;
			for (Job job : this.jobs) {
				job.adjust(adjustTime);
			}
		}

		subscribe(minsubmittime * 1000 - currentTime);
		if (verbosity) {
			new Thread() {
				private void printLog(String s) {
					System.err.println("MIJD ===> realTime=" + new Date() + " " + s);
				}

				private void printStats() {
					printLog("subscibed=" + MultiIaaSJobDispatcher.this.isSubscribed() + " simTime="
							+ Timed.getFireCount() + " destroys=" + getDestroycounter() + " startedjobs=" + minindex);
				}

				public void run() {
					printLog("Starting monitoring thread!");
					boolean keepThread = true;
					while (keepThread) {
						printStats();
						final long cont = System.currentTimeMillis() + 15000;
						int qlen = -1;
						while (cont > System.currentTimeMillis() && keepThread) {
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {

							}
							if (!MultiIaaSJobDispatcher.this.isSubscribed()) {
								keepThread = false;
								for (IaaSService s : MultiIaaSJobDispatcher.this.target) {
									keepThread |= !s.listVMs().isEmpty();
									if (keepThread) {
										qlen = s.sched.getQueueLength();
										break;
									}
								}
							}
						}
						if (qlen > 0) {
							printLog("queue len: " + qlen);
						}
					}
					printStats();
					printLog("Exiting monitoring thread!");
				};
			}.start();
		}
	}

	/**
	 * Handling the jobs one by one when they are due.
	 */
	@Override
	public void tick(final long currTime) {
		// One ore more jobs must be submitted as we received this event
		for (int i = minindex; i < jobs.length; i++) {
			final Job toprocess = jobs[i];
			long submittime = toprocess.getSubmittimeSecs() * 1000;
			if (currTime == submittime) {
				// the ith job is due now
				boolean retry;
				do {
					retry = false;
					// to fulfill the ith job's cpu core requirements we need the
					// following set of VMs with the following number of CPUs
					int requestedTotalInstances = maxmachinecores >= toprocess.nprocs ? 1
							: (toprocess.nprocs / ((int) maxmachinecores))
									+ ((toprocess.nprocs % (int) maxmachinecores) == 0 ? 0 : 1);
					final double requestedprocs = (double) toprocess.nprocs / requestedTotalInstances;
					ConstantConstraints reqRC = new ConstantConstraints(requestedprocs, useThisProcPower,
							isMinimumProcPower, 512000000);
					// For simplicity, here we have an assumption that our clouds
					// are uniform...
					int requestedClouds = (int) Math.ceil(requestedTotalInstances > maxIaaSmachines
							? (double) requestedTotalInstances / maxIaaSmachines
							: 1);
					if (requestedClouds <= target.size()) {
						// We have a chance to fit the job request in

						// Clean up the VM keeper list of ours
						Iterator<VMKeeper> it = pooledVMs.iterator();
						while (it.hasNext()) {
							VMKeeper curr = it.next();
							if (!curr.isAlive()) {
								it.remove();
							}
						}

						int vmpointer = 0;
						VMKeeper[] vms = new VMKeeper[requestedTotalInstances];

						// Make sure the smallest VMs are listed first
						// This ensures we leave the smallest amount of unused resources in the VMs
						it = pooledVMs.iterator();
						while (it.hasNext() && requestedTotalInstances > 0) {
							VMKeeper current = it.next();
							if (current.isFree() && current.wouldFit(reqRC)) {
								reuseCounter++;
								it.remove();
								vms[vmpointer++] = current;
								requestedTotalInstances--;
								retry = true;
							}
						}

						if (requestedTotalInstances > 0) {
							// recalculate requested clouds after reusing VMs
							requestedClouds = (int) Math.ceil(requestedTotalInstances > maxIaaSmachines
									? (double) requestedTotalInstances / maxIaaSmachines
									: 1);

							final int uniformSpread = requestedTotalInstances / requestedClouds;
							int remainder = requestedTotalInstances % requestedClouds;

							for (int j = 0; j < requestedClouds; j++) {
								final int expectedSpread = uniformSpread + remainder;
								final int currentRequestSize = (int) Math.min(maxIaaSmachines, expectedSpread);
								remainder = expectedSpread - currentRequestSize;
								// Starting the VMs for the job
								try {
									IaaSService currentTarget = target.get(targetIndex);
									final VirtualMachine[] vmsTemp = currentTarget.requestVM(va, reqRC,
											repo.get(targetIndex), currentRequestSize);
									for (int k = 0; k < currentRequestSize; k++) {
										VMKeeper newKeeper = new VMKeeper(currentTarget, vmsTemp[k], 3600 * 1000);
										newKeeper.setListener(new VMKeeper.ReleaseListener() {

											@Override
											public void released(VMKeeper me) {
												pooledVMs.add(me);
											}
										});
										vms[vmpointer++] = newKeeper;

									}

									// doing a round robin scheduling for the target
									// infrastructures
									targetIndex++;
									if (targetIndex == target.size()) {
										targetIndex = 0;
									}
								} catch (VMManager.VMManagementException e) {
									// VM cannot be served because of too large resource
									// request
									if (verbosity) {
										System.err
												.println("The oversized job's id: " + toprocess.getId() + " idx: " + i);
									}
									ignorecounter++;
								} catch (Exception e) {
									System.err.println("Unknown VM creation error: " + e.getMessage());
									e.printStackTrace();
									ignorecounter++;
								}
							}
						}
						boolean servability = true;
						for (int j = 0; j < vms.length && servability; j++) {
							// check if the job was not servable because it would
							// have needed more resources than the target clouds
							// could offer in total.
							servability &= vms[j].isServable();
						}
						if (servability) {
							retry = false;
							new SingleJobRunner(toprocess, vms, this);
						} else {
							for (int j = 0; j < vms.length; j++) {
								if (vms[j].isServable()) {
									vms[j].prematureDestroy();
								}
							}
							ignorecounter++;
						}
					} else {
						if (verbosity) {
							System.err
									.println("Bigger job than all clouds. Job id: " + toprocess.getId() + " idx: " + i);
						}
						ignorecounter++;
					}
				} while (retry);
				minindex = i + 1;
			} else if (currTime < submittime) {
				// the ith job is not due yet, we have to ask for a new
				// notification which will arrive when the ith job is due
				updateFrequency(submittime - currTime);
				break;
			}
		}
		if (minindex == jobs.length)

		{
			// No more jobs are listed in the trace, we can just make sure no
			// further events are coming to this dispatcher
			unsubscribe();
		}
	}

	/**
	 * Collects the earilest submission time for the trace
	 * 
	 * @return
	 */
	public long getMinsubmittime() {
		return minsubmittime;
	}

	/**
	 * Tells how many jobs were unservable in the target clouds
	 * 
	 * @return
	 */
	public long getIgnorecounter() {
		return ignorecounter;
	}

	/**
	 * tells how many VMs executed their tasks successfully (and then successively
	 * how many of them got destroyed)
	 * 
	 * @return
	 */
	public long getDestroycounter() {
		return destroycounter;
	}

	/**
	 * Allows single job runners to let us know if they have completed the execution
	 * of their job
	 * 
	 * @param finishedVMs
	 *            the number of VMs that were actually used for the job
	 */
	void increaseDestroyCounter(final int finishedVMs) {
		if (finishedVMs <= 0) {
			throw new IllegalStateException("Tried to reduce the destroy counter!");
		}
		destroycounter += finishedVMs;
	}

	/**
	 * Sets the processing power related requirements for the resource allocation
	 * requests for all VMs.
	 * 
	 * @param usableProcPower
	 *            the CPU's processing power instructions/ms
	 * @param minimum
	 *            is it the minimum required or the total you want to specify
	 */
	public void setUsableProcPower(final double usableProcPower, final boolean minimum) {
		this.useThisProcPower = usableProcPower;
		isMinimumProcPower = minimum;
	}

	/**
	 * Do not continue the trace processing, terminate all activities as soon as
	 * possible.
	 */
	public void stopTraceProcessing() {
		unsubscribe();
		isStopped = true;
	}

	public boolean isStopped() {
		return isStopped;
	}
}
