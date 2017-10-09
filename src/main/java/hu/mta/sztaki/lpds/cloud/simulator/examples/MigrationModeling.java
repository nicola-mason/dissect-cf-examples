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
import hu.mta.sztaki.lpds.cloud.simulator.energy.specialized.PhysicalMachineEnergyMeter;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.mta.sztaki.lpds.cloud.simulator.util.PowerTransitionGenerator;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Demonstrates simple non-live migration and energy reading facilities in
 * DISSECT-CF
 * 
 * Starts up two PMs, instantiates a VM on one of them. Runs a task on the VM.
 * Then during the task execution issues a migration request. Writes out the
 * simulated energy behavior during the migration process.
 * 
 * This class was created to demonstrate the simulator's alignment with the
 * findings of the following paper:<br>
 * <i>Vincenzo De Maio, Gabor Kecskemeti, Radu Prodan: "A Workload-Aware Energy
 * Model for Virtual Machine Migration". In the proceedings of the 2015 IEEE
 * International conference on Cluster Computing.</i>
 * 
 * @author "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems,
 *         MTA SZTAKI (c) 2015"
 */
public class MigrationModeling {

	public static void main(String[] args) throws Exception {
		final long disksize = 100000000000l;
		final long freq = 100;
		final PhysicalMachineEnergyMeter pmm1, pmm2;
		final ArrayList<Long> readingtime = new ArrayList<Long>();
		final ArrayList<Double> readingpm1 = new ArrayList<Double>();
		final ArrayList<Double> readingpm2 = new ArrayList<Double>();
		HashMap<String, Integer> latencyMap = new HashMap<String, Integer>();
		latencyMap.put("pm1", 3);
		latencyMap.put("pm2", 3);
		latencyMap.put("repo", 3);
		final EnumMap<PowerTransitionGenerator.PowerStateKind, Map<String, PowerState>> transitions = PowerTransitionGenerator
				.generateTransitions(10, 200, 300, 5, 5);
		final Map<String, PowerState> cpuTransitions = transitions.get(PowerTransitionGenerator.PowerStateKind.host);
		final Map<String, PowerState> stTransitions = transitions.get(PowerTransitionGenerator.PowerStateKind.storage);
		final Map<String, PowerState> nwTransitions = transitions.get(PowerTransitionGenerator.PowerStateKind.network);

		// Repository setup
		Repository r = new Repository(disksize, "repo", 10000, 10000, 10000, latencyMap, stTransitions, nwTransitions);
		VirtualAppliance va = new VirtualAppliance("va", 100, 0, false, 100000000l);
		r.registerObject(va);
		r.setState(NetworkNode.State.RUNNING);

		// Basic PM construction
		PhysicalMachine pm1, pm2;
		pm1 = new PhysicalMachine(1, 1, 1000,
				new Repository(disksize, "pm1", 10000, 10000, 10000, latencyMap, stTransitions, nwTransitions), 10, 10,
				cpuTransitions);
		pm2 = new PhysicalMachine(1, 1, 1000,
				new Repository(disksize, "pm2", 10000, 10000, 10000, latencyMap, stTransitions, nwTransitions), 10, 10,
				cpuTransitions);

		// Meter setup for the PMs
		pmm1 = new PhysicalMachineEnergyMeter(pm1);
		pmm2 = new PhysicalMachineEnergyMeter(pm2);
		class MeteredDataCollector extends Timed {
			// Call when we need to initiate data collection
			public void start() {
				subscribe(freq);
			}

			// Call when we need to terminate data collection
			public void stop() {
				unsubscribe();
			}

			// Automatically called with the frequency specified above
			@Override
			public void tick(final long fires) {
				// Actual periodic data collection
				readingtime.add(fires);
				readingpm1.add(pmm1.getTotalConsumption());
				readingpm2.add(pmm2.getTotalConsumption());
			}
		}
		final MeteredDataCollector mdc = new MeteredDataCollector();

		// Start the PMs
		pm1.turnon();
		pm2.turnon();
		Timed.simulateUntilLastEvent();

		// Ask for a VM that fills one of the PMs completely
		VirtualMachine vm = pm1.requestVM(va, pm1.getCapacities(), r, 1)[0];
		// Start the VM
		Timed.simulateUntilLastEvent();

		ConsumptionEventAdapter ce = new ConsumptionEventAdapter() {
			// Once our VM completed its tasks we should terminate all
			// monitoring
			@Override
			public void conComplete() {
				super.conComplete();
				pmm1.stopMeter();
				pmm2.stopMeter();
				mdc.stop();
			}
		};
		// Inject the task
		vm.newComputeTask(100000, ResourceConsumption.unlimitedProcessing, ce);
		// Initiate metering
		pmm1.startMeter(freq, true);
		pmm2.startMeter(freq, true);
		// displacement of readings (otherwise the order of metering and data
		// collection is not always correct!)
		Timed.fire();
		// Initiate monitor for collecting meter data
		mdc.start();

		// Delay of 1000 ticks
		Timed.simulateUntil(Timed.getFireCount() + 1000);
		// Initiation of the migration after some processing was done during the
		// delay
		vm.migrate(pm2.allocateResources(vm.getResourceAllocation().allocated, true, 100000));
		// Ensuring the migration is done and the processing of the VM's only
		// task is completed
		Timed.simulateUntilLastEvent();

		// Writing out the collected results into migrmodel.csv
		RandomAccessFile raf = new RandomAccessFile("MigrModel.csv", "rw");

		for (int i = 0; i < readingtime.size(); i++) {
			// CSV format: time, pm1 energy reading, pm2 energy reading
			raf.writeBytes(readingtime.get(i) + "," + readingpm1.get(i) + "," + readingpm2.get(i) + "\n");
		}
		raf.close();
	}
}
