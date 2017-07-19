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

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.energy.powermodelling.PowerState;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.FileBasedTraceProducerFactory;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.GenericTraceProducer;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.TraceFilter;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.filters.RunningAtaGivenTime;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random.RepetitiveRandomTraceGenerator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.consolidation.Consolidator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.consolidation.SimpleConsolidator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.PhysicalMachineController;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.SchedulingDependentMachines;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.FirstFitScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.Scheduler;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.util.CloudLoader;
import hu.mta.sztaki.lpds.cloud.simulator.util.PowerTransitionGenerator;

/**
 * This command line program sets up one or more cloud infrastructures, sends a
 * set of jobs from a trace to them in VMs. Optionally it allows the monitoring
 * of all clouds while the jobs are sent to them. Finally, it writes out
 * statistical data about the execution - including real and simulated runtimes
 * etc. It's help - which is printed out when it receives no command line
 * arguments - provides insights on the parametrization.
 * 
 * This class was used to provide the input for Figures 14-17 in the article:
 * <br>
 * <i>Gabor Kecskemeti: "DISSECT-CF: a simulator to foster energy-aware
 * scheduling in infrastructure clouds" . In Simulation Modeling Practice and
 * Theory, 2015, to Appear.<i> *
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2016"
 * @author "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems,
 *         MTA SZTAKI (c) 2012-5"
 */
public class JobDispatchingDemo {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		// The help
		if (args.length < 2) {
			System.out.println("Expected parameters:");
			System.out.println("1. job trace:");
			System.out.println(
					"1A) if a workload specification (e.g. gwf,swf, srtg) file is used as an input then give its full path here");
			System.out.println(
					"1B) if a synthetic trace is used then specify its properties here as follows (items should be separated with a dash):");
			System.out.println("1Ba) Maximum number of jobs that exist in parallel");
			System.out.println(
					"1Bb) The amount of seconds the job startup times should be spread uniformely in a parallel batch");
			System.out.println("1Bc) Minimum execution time of a single job");
			System.out.println("1Bd) Maximum execution time of a single job");
			System.out.println(
					"1Be) Minimum gap between the last and the first job submission of two consequitve parallel batches");
			System.out.println(
					"1Bf) Maximum gap between the last and the first job submission of two consequitve parallel batches");
			System.out.println("1Bg) Minimum number of processors for a single job");
			System.out.println("1Bh) Maximum number of processors for a single job");
			System.out.println("1Bi) Total number of processors usable by all parallel jobs");
			System.out.println("1B - example) 2/10/10/90/200/200/1/2/4");
			System.out.println("2. Jobs and monitoring:");
			System.out.println(
					"2A) (optional) if the first charachter of the second parameter is a + then the monitoring is switched off (no converted trace files will be written)");
			System.out.println(
					"2B) A range of jobs from the above mentioned trace, if a single number is given then the range is assumed to be starting from 0");
			System.out.println(
					"2C) (optional) if the range is followed by an '@', then one can specify to filter the jobs from the above range so they are all supposed to be running at a specific time instance (the time instance is given after the '@' character)");
			System.out.println("2 - example) +10-20@10000");
			System.out.println("3. Cloud definition");
			System.out.println("3A) one either gives a full path to the description of the cloud to be used");
			System.out.println("3B) or it is possible to specify a the number of hosts and"
					+ "the number of cpus in a host and the number of clouds these"
					+ "hosts should be spread out with the following format:");
			System.out.println("3Bi) - number of cpu nodes");
			System.out.println("3Bii) - number of cpu cores per node separated with an @");
			System.out
					.println("3Biii) - number of IaaS services the nodes should be split equally separated with an @");
			System.out.println(
					"\t Note: if no @ is present in this parameter then the cpu cores are assumed to be 64/node");
			System.out.println("3B - example 1) 500@16 (for 500 pms with 16 cores each");
			System.out.println("3B - example 2) 5000@16@5 (for 1000 pms with 16 cores each in 5 clouds");
			System.out.println(
					"4. energy monitoring polling frequency (only needed when the second parameter does not start with +)");
			System.out.println("4 - example) 5000");
			System.out.println();
			System.out.println("Supported system properties:");
			System.out.println("hu.mta.sztaki.lpds.cloud.simulator.examples.consolidator");
			System.out.println(
					"\tThe consolidator class to be used for all clouds, if an unknown class is listed here we fall back to SimpleConsolidator");
			System.out.println("hu.mta.sztaki.lpds.cloud.simulator.examples.consolidator.freq");
			System.out.println("\tThe consolidator frequency to be used for all clouds");
			System.out.println("hu.mta.sztaki.lpds.cloud.simulator.examples.verbosity");
			System.out.println("\tTurn on additional logging information");
			System.exit(0);
		}

		String consolidatorClass = System.getProperty("hu.mta.sztaki.lpds.cloud.simulator.examples.consolidator");
		Class<? extends Consolidator> consolidator = null;
		if (consolidatorClass != null) {
			try {
				@SuppressWarnings("rawtypes")
				Class trial = Class.forName(consolidatorClass);
				if (Scheduler.class.isAssignableFrom(trial)) {
					consolidator = trial;
				} else {
					consolidator = SimpleConsolidator.class;
				}
			} catch (Exception e) {
				consolidator = SimpleConsolidator.class;
			}
		}

		// The preparation of the clouds
		List<IaaSService> iaasList = new ArrayList<IaaSService>();
		if (new File(args[2]).exists()) {
			// Loading a single cloud from file (so far only a single cloud is
			// possible to load)
			iaasList.add(CloudLoader.loadNodes(args[2]));
		} else {
			// Defaults for cloud and core counts
			int numofCores = 64;
			int numofClouds = 1;
			// Parsing the 3rd argument.
			String[] hostSpec = args[2].split("@");
			Class<? extends Scheduler> vmSched = FirstFitScheduler.class;
			Class<? extends PhysicalMachineController> pmSched = SchedulingDependentMachines.class;
			if (hostSpec.length > 3) {
				throw new IllegalStateException("Host specification string '" + args[2] + "' is incorrect:");
			}
			int totNumofNodes = Integer.parseInt(hostSpec[0]);
			if (hostSpec.length >= 2) {
				numofCores = Integer.parseInt(hostSpec[1]);
			}
			if (hostSpec.length == 3) {
				String[] cloudSpec = hostSpec[2].split(":");
				numofClouds = Integer.parseInt(cloudSpec[0]);
				if (cloudSpec.length > 1) {
					@SuppressWarnings("rawtypes")
					Class trial = Class.forName(cloudSpec[1]);
					if (Scheduler.class.isAssignableFrom(trial)) {
						vmSched = trial;
					} else {
						throw new IllegalStateException(
								"The VM Scheduler specified in the parameters is not a subclass of Scheduler");
					}
					trial = Class.forName(cloudSpec[2]);
					if (PhysicalMachineController.class.isAssignableFrom(trial)) {
						pmSched = trial;
					} else {
						throw new IllegalStateException(
								"The PM Scheduler specified in the parameters is not a subclass of PhysicalMachineController");
					}
				}

			}
			System.err.println(
					"Using schedulers: " + vmSched.getName() + " for VMs and " + pmSched.getName() + " for PMs");
			// Creating the each cloud requested
			for (int clid = 0; clid < numofClouds; clid++) {
				int numofNodes = totNumofNodes / numofClouds;
				System.err.println(
						"Scaling datacenter to " + numofNodes + " nodes with " + numofCores + " cpu cores each");
				if (numofNodes * numofClouds != totNumofNodes) {
					System.err.println(
							"WARNING: with equally sized clouds we cannot reach the total number of nodes specified!");
				}
				// Default constructs

				HashMap<String, Integer> latencyMapRepo = new HashMap<String, Integer>(numofNodes + 2);
				HashMap<String, Integer> latencyMapMachine = new HashMap<String, Integer>(numofNodes + 2);
				IaaSService iaas = new IaaSService(vmSched, pmSched);
				final String repoid = clid + "VHStorageDell";
				final String machineid = clid + "VHNode";
				// Specification of the default power behavior
				final EnumMap<PowerTransitionGenerator.PowerStateKind, Map<String, PowerState>> transitions = PowerTransitionGenerator
						.generateTransitions(20, 296, 493, 50, 108);
				final Map<String, PowerState> cpuTransitions = transitions
						.get(PowerTransitionGenerator.PowerStateKind.host);
				final Map<String, PowerState> stTransitions = transitions
						.get(PowerTransitionGenerator.PowerStateKind.storage);
				final Map<String, PowerState> nwTransitions = transitions
						.get(PowerTransitionGenerator.PowerStateKind.network);

				// Creating the Repositories for the cloud

				// scaling the bandwidth accroding to the size of the cloud
				final double bwRatio = (numofCores * numofNodes) / (7f * 64f);
				// A single repo will hold 36T of data
				iaas.registerRepository(
						new Repository(36000000000000l, repoid, (long) (bwRatio * 1250000), (long) (bwRatio * 1250000),
								(long) (bwRatio * 250000), latencyMapRepo, stTransitions, nwTransitions));
				latencyMapMachine.put(repoid, 5); // 5 ms latency towards the
													// repos

				// Creating the PMs for the cloud

				ArrayList<PhysicalMachine> completePMList = new ArrayList<PhysicalMachine>(numofNodes);
				for (int i = 1; i <= numofNodes; i++) {
					String currid = machineid + i;
					final double pmBWRatio = Math.max(numofCores / 7f, 1);
					PhysicalMachine pm = new PhysicalMachine(numofCores, 0.001, 256000000000l,
							new Repository(5000000000000l, currid, (long) (pmBWRatio * 250000),
									(long) (pmBWRatio * 250000), (long) (pmBWRatio * 50000), latencyMapMachine,
									stTransitions, nwTransitions),
							89000, 29000, cpuTransitions);
					latencyMapRepo.put(currid, 5);
					latencyMapMachine.put(currid, 3);
					completePMList.add(pm);
				}

				// registering the hosts and the IaaS services
				iaas.bulkHostRegistration(completePMList);
				iaasList.add(iaas);
			}
		}
		// Wait until the PM Controllers finish their initial activities
		Timed.simulateUntilLastEvent();

		// Further processing of the CLI arguments
		boolean doMonitoring = !args[1].startsWith("+");
		if (!doMonitoring) {
			args[1] = args[1].substring(1);
		}
		int from = 0;
		int to = 0;
		String filterSpec = null;
		if (args[1].contains("@")) {
			String[] splitJobSpec = args[1].split("@");
			args[1] = splitJobSpec[0];
			filterSpec = splitJobSpec[1];
		}
		if (args[1].contains("-")) {
			String[] range = args[1].split("-");
			from = Integer.parseInt(range[0]);
			to = Integer.parseInt(range[1]);
		} else {
			to = Integer.parseInt(args[1]);
		}

		// Loading the trace
		GenericTraceProducer producer;
		if (new File(args[0]).exists()) {
			// The trace comes from a file, we need to see what kind to pick the
			// right loader
			int maxTotalProcs = 0;
			for (IaaSService curr : iaasList) {
				maxTotalProcs += curr.getCapacities().getRequiredCPUs();
			}
			producer = FileBasedTraceProducerFactory.getProducerFromFile(args[0], from, to, false, maxTotalProcs,
					DCFJob.class);
		} else {
			// The trace comes in the form of generic random trace
			// characteristics.
			String[] params = args[0].split("/");
			RepetitiveRandomTraceGenerator trgen = new RepetitiveRandomTraceGenerator(DCFJob.class);
			producer = trgen;
			trgen.setJobNum(to - from);
			trgen.setParallel(Integer.parseInt(params[0]));
			trgen.setMaxStartSpread(Integer.parseInt(params[1]));
			trgen.setExecmin(Integer.parseInt(params[2]));
			trgen.setExecmax(Integer.parseInt(params[3]));
			trgen.setMingap(Integer.parseInt(params[4]));
			trgen.setMaxgap(Integer.parseInt(params[5]));
			trgen.setMinNodeProcs(Integer.parseInt(params[6]));
			trgen.setMaxNodeprocs(Integer.parseInt(params[7]));
			trgen.setMaxTotalProcs(Integer.parseInt(params[8]));
		}

		if (filterSpec != null) {
			producer = new TraceFilter(producer, new RunningAtaGivenTime(Long.parseLong(filterSpec)));
		}

		// Preparing for sending the jobs to the clouds with the dispatcher
		MultiIaaSJobDispatcher dispatcher = new MultiIaaSJobDispatcher(producer, iaasList);
		if (args.length > (doMonitoring ? 4 : 3)) {
			Thread.sleep(50000);
		}
		ArrayList<Consolidator> consolidators = new ArrayList<Consolidator>();
		if (consolidator != null) {
			String consFreq = System.getProperty("hu.mta.sztaki.lpds.cloud.simulator.examples.consolidation.freq");
			int freq = 5 * 60 * 1000;
			if (consFreq != null) {
				freq = Integer.parseInt(consFreq);
			}
			System.err.println(
					"Consolidation is switched on using class " + consolidator.getName() + " with freq " + freq);
			for (IaaSService iaas : iaasList) {
				Constructor<? extends Consolidator> constructor = consolidator.getConstructor(IaaSService.class,
						long.class);
				consolidators.add(constructor.newInstance(iaas, freq));
			}
		}
		long beforeSimu = Calendar.getInstance().getTimeInMillis();
		System.err.println(
				"Job dispatcher (with " + dispatcher.jobs.length + " jobs)  is completely prepared at " + beforeSimu);
		// Moving the simulator's time just before the first event would come
		// from the dispatcher
		Timed.skipEventsTill(dispatcher.getMinsubmittime() * 1000);
		System.err.println("Current simulation time: " + Timed.getFireCount());
		if (doMonitoring) {
			// Final monitoring related CLI arguments parsing
			final int interval = Integer.parseInt(args[3]);
			if (interval == 0) {
				System.err.println("ERROR: Improperly specified energy consumption monitoring interval!");
				System.exit(1);
			}
			// Creation of the state monitor object (it will register and
			// deregister itself with timed once there are no more activites
			// expected in the cloud, so we don't need to keep its reference)
			new StateMonitor(args[0], dispatcher, iaasList, interval);
		}
		// Now everything is prepared for launching the simulation

		// The actual simulation
		Timed.simulateUntilLastEvent();
		// The simulation is complete all activities have finished by the
		// dispatcher and monitor
		long afterSimu = Calendar.getInstance().getTimeInMillis();
		long duration = afterSimu - beforeSimu;

		// Printing out generic timing and performance statistics:
		System.err.println("Simulation terminated " + afterSimu + " (took " + duration + "ms in realtime)");
		System.err.println("Current simulation time: " + Timed.getFireCount());
		System.err.println("Simulated timespan: " + (Timed.getFireCount() - dispatcher.getMinsubmittime() * 1000));
		System.err.println("Final number of: Ignored jobs - " + dispatcher.getIgnorecounter() + " Destroyed VMs - "
				+ dispatcher.getDestroycounter());
		if (consolidator != null) {
			System.err.println("Total migrations done: " + SimpleConsolidator.migrationCount);
		}
		long vmcount = 0;
		for (IaaSService lociaas : iaasList) {
			for (PhysicalMachine pm : lociaas.machines) {
				vmcount += pm.getCompletedVMs();
			}
		}
		System.err.println("Performance: " + (((double) vmcount) / duration) + " VMs/ms ");
	}
}
