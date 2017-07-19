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
 *  (C) Copyright 2013-15, Gabor Kecskemeti (gkecskem@dps.uibk.ac.at,
 *   									  kecskemeti.gabor@sztaki.mta.hu)
 */
package hu.mta.sztaki.lpds.cloud.simulator.examples;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.energy.powermodelling.PowerState;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.DCFJob;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.JobListAnalyser;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random.RepetitiveRandomTraceGenerator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.mta.sztaki.lpds.cloud.simulator.util.PowerTransitionGenerator;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Allows the stress testing of a single VM with arbitrary load. The load of the
 * VM can be defined using the RepetitiveRandomTraceGenerator from the
 * DistSystJavaHelpers package.
 * 
 * This class was used to provide the input for Figures 12-13 in the
 * article:<br>
 * <i>Gabor Kecskemeti: "DISSECT-CF: a simulator to foster energy-aware
 * scheduling in infrastructure clouds" . In Simulation Modeling Practice and
 * Theory, 2015, to Appear.<i>
 * 
 * @author "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems,
 *         MTA SZTAKI (c) 2014"
 */
public class SingleVMOverloader {
	static int jobhits = 0;
	static List<Job> jobs;
	static int jobslen;
	static long basetime;

	public static void main(String[] args) throws Exception {
		int machinecores = 1;
		System.err.println("SingleVMOverloader - DISSECT-CF - started at " + System.currentTimeMillis());
		HashMap<String, Integer> latencyMap = new HashMap<String, Integer>();
		final EnumMap<PowerTransitionGenerator.PowerStateKind, Map<String, PowerState>> transitions = PowerTransitionGenerator
				.generateTransitions(20, 280, 490, 25, 35);

		// The definition of the PM
		PhysicalMachine pm = new PhysicalMachine(machinecores, 1, 256000000000l,
				new Repository(5000000000000l, "PMID", 250000000, 250000000, 50000000, latencyMap,
						transitions.get(PowerTransitionGenerator.PowerStateKind.storage),
						transitions.get(PowerTransitionGenerator.PowerStateKind.network)),
				89000, 29000, transitions.get(PowerTransitionGenerator.PowerStateKind.host));
		// The virtual machine image to be used for the future VM
		VirtualAppliance va = new VirtualAppliance("test", 30000, 0, false, 100000000);
		// Placing the VM's image on the PMs local disk
		pm.localDisk.registerObject(va);
		pm.turnon();
		Timed.simulateUntilLastEvent();
		// by this time the PM is on
		final VirtualMachine vm = pm.requestVM(va, pm.getCapacities(), pm.localDisk, 1)[0];
		Timed.simulateUntilLastEvent();
		// by this time the VM is running on the PM
		basetime = Timed.getFireCount();
		System.err.println("PM and VM is prepared " + System.currentTimeMillis());
		// Parsing the trace characteristics for RepetitiveRandomTraceGenerator
		String[] params = args[0].split("/");
		RepetitiveRandomTraceGenerator trgen = new RepetitiveRandomTraceGenerator(DCFJob.class);
		trgen.setJobNum(Integer.parseInt(args[1]));
		trgen.setParallel(Integer.parseInt(params[0]));
		trgen.setMaxStartSpread(Integer.parseInt(params[1]));
		trgen.setExecmin(Integer.parseInt(params[2]));
		trgen.setExecmax(Integer.parseInt(params[3]));
		trgen.setMingap(Integer.parseInt(params[4]));
		trgen.setMaxgap(Integer.parseInt(params[5]));
		trgen.setMinNodeProcs(Integer.parseInt(params[6]));
		trgen.setMaxNodeprocs(Integer.parseInt(params[7]));
		trgen.setMaxTotalProcs(Integer.parseInt(params[8]));
		jobs = trgen.getAllJobs();
		Collections.sort(jobs, JobListAnalyser.submitTimeComparator);
		jobslen = jobs.size();
		// For every job we record its completion in the jobhits. This event
		// adapter is created so it can catch the job completion events.
		final ConsumptionEventAdapter cae = new ConsumptionEventAdapter() {
			@Override
			public void conComplete() {
				jobhits++;
			}
		};
		// JSender starts up the jobs on our single VM once the trace demands it
		class JSender extends Timed {
			int currentcount = 0;

			public JSender() {
				// Makes sure the first event will come upon the first job's
				// arrival.
				subscribe(getConvertedFirecount(currentcount) - Timed.getFireCount());
			}

			// Transforms job arrival times to be in the VMs lifetime
			public long getConvertedFirecount(int count) {
				return count >= jobslen ? -1 : jobs.get(count).getSubmittimeSecs() * 1000 + basetime;
			}

			@Override
			public void tick(long fires) {
				long nextSubscriptionTime;
				do {
					try {
						Job j = jobs.get(currentcount);
						// Injects a new job when its due
						vm.newComputeTask(j.getExectimeSecs(), j.nprocs, cae);
					} catch (Exception e) {
						throw new IllegalStateException(e);
					}
					currentcount++;
				} while ((nextSubscriptionTime = getConvertedFirecount(currentcount)) == fires);
				if (nextSubscriptionTime > fires) {
					// Calculates when the next job is going to be due and
					// orders an event for that occasion
					updateFrequency(nextSubscriptionTime - fires);
				} else {
					// Ensures that the events are not coming after there are no
					// further jobs
					unsubscribe();
				}
			}
		}
		// Starts up the job submission process
		new JSender();
		long before = System.currentTimeMillis();
		System.err.println("Jobs are prepared " + before);
		// The simulation is now ready to be executed
		Timed.simulateUntilLastEvent();
		// Statistics printouts
		long after = System.currentTimeMillis();
		System.err.println("Final job has completed, took " + (after - before) + " ms");
		long afterSimu = Timed.getFireCount();
		System.err.println("Total simulated time " + (afterSimu - basetime) + " ms");
		if (jobhits == jobs.size()) {
			System.err.println("All jobs terminated successfully..");
		} else {
			System.err.println("Not completed all jobs! Successful completions: " + jobhits);
		}
	}

}
