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
package hu.mta.sztaki.lpds.cloud.simulator.examples.util;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import hu.mta.sztaki.lpds.cloud.simulator.energy.powermodelling.PowerState;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.PhysicalMachineController;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.Scheduler;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.util.PowerTransitionGenerator;

/**
 * Helper class to create simple but artbitrary sized data centre configurations
 * automatically
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2016-9"
 * @author "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems,
 *         MTA SZTAKI (c) 2012-5"
 */
public class DCCreation {
	// Creates a uniform DC with 36T VA store and as many PMs as needed, but all PMs
	// will have the exact same specs
	public static IaaSService createDataCentre(Class<? extends Scheduler> vmsch,
			Class<? extends PhysicalMachineController> pmcont, int numofNodes, int numofCores) throws Exception {
		System.err.println("Scaling datacenter to " + numofNodes + " nodes with " + numofCores + " cpu cores each");
		// Default constructs
		// Specification of the default power behavior
		final EnumMap<PowerTransitionGenerator.PowerStateKind, Map<String, PowerState>> transitions = PowerTransitionGenerator
				.generateTransitions(20, 296, 493, 50, 108);
		final Map<String, PowerState> stTransitions = transitions.get(PowerTransitionGenerator.PowerStateKind.storage);
		final Map<String, PowerState> nwTransitions = transitions.get(PowerTransitionGenerator.PowerStateKind.network);

		IaaSService iaas = new IaaSService(vmsch, pmcont);

		// Creating the VA store for the cloud

		final String repoid = "Storage";
		// scaling the bandwidth accroding to the size of the cloud
		final double bwRatio = (numofCores * numofNodes) / (7f * 64f);
		// A single repo will hold 36T of data
		HashMap<String, Integer> latencyMapRepo = new HashMap<String, Integer>(numofNodes + 2);
		Repository mainStorage = new Repository(36000000000000l, repoid, (long) (bwRatio * 1250000),
				(long) (bwRatio * 1250000), (long) (bwRatio * 250000), latencyMapRepo, stTransitions, nwTransitions);
		iaas.registerRepository(mainStorage);

		// Creating the PMs for the cloud

		final Map<String, PowerState> cpuTransitions = transitions.get(PowerTransitionGenerator.PowerStateKind.host);
		ArrayList<PhysicalMachine> completePMList = new ArrayList<PhysicalMachine>(numofNodes);
		HashMap<String, Integer> latencyMapMachine = new HashMap<String, Integer>(numofNodes + 2);
		latencyMapMachine.put(repoid, 5); // 5 ms latency towards the repos
		final String machineid = "Node";
		for (int i = 1; i <= numofNodes; i++) {
			String currid = machineid + i;
			final double pmBWRatio = Math.max(numofCores / 7f, 1);
			PhysicalMachine pm = new PhysicalMachine(numofCores, 0.001, 256000000000l,
					new Repository(5000000000000l, currid, (long) (pmBWRatio * 250000), (long) (pmBWRatio * 250000),
							(long) (pmBWRatio * 50000), latencyMapMachine, stTransitions, nwTransitions),
					89000, 29000, cpuTransitions);
			latencyMapRepo.put(currid, 5);
			latencyMapMachine.put(currid, 3);
			completePMList.add(pm);
		}

		// registering the hosts and the IaaS services
		iaas.bulkHostRegistration(completePMList);
		return iaas;
	}

}
