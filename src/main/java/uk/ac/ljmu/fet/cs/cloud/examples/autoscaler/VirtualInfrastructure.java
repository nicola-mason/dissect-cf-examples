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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;

/**
 * Represents a virtual infrastructure that is scaled automatically based on the
 * needs of the application running on the infrastructure. The class applies a
 * simple threshold based scaling mechanism: it removes VMs which are not
 * utilised to a certain level; and adds VMs to the VI if most of the VMs are
 * too heavily utilised.
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2019"
 */
public class VirtualInfrastructure extends Timed implements VirtualMachine.StateChange {
	/**
	 * Minimum CPU utilisation percentage of a single VM that is still acceptable to
	 * keep in the virtual infrastructure.
	 */
	public static final double minUtilisationLevelBeforeDestruction = .2;
	/**
	 * Maximum average CPU utilisation percentage of all VMs of a particular kind of
	 * executable that is still considered acceptable (i.e., under this utilisation,
	 * the virtual infrastructure does not need a new VM with the same kind of
	 * executable).
	 */
	public static final double maxUtilisationLevelBeforeNewVM = .75;

	/**
	 * The virtual infrastructure for each executable. The keys of the map are the
	 * executable types for which we have a virtual infrastructure. The values of
	 * the map are the actual VMs that we have at hand for a given executable.
	 */
	public final HashMap<String, ArrayList<VirtualMachine>> vmSetPerKind = new HashMap<String, ArrayList<VirtualMachine>>();

	/**
	 * If a kind of VM is under preparation already, we remember it here so we can
	 * tell which VM kind should not have more VMs instantiated/destructued for the
	 * time being.
	 */
	public final HashMap<String, VirtualMachine> underPrepVMPerKind = new HashMap<String, VirtualMachine>();

	/**
	 * The cloud on which we will execute our virtual machines
	 */
	protected final IaaSService cloud;
	/**
	 * The cloud's VMI storage where we register the images of our executables
	 * allowing the instantiation of specific VMs that host the particular
	 * executable.
	 */
	protected final Repository storage;
	/**
	 * The number of cores the first PM (of the cloud) has. This is used to
	 * determine the maximum size of the VMs we will request.
	 */
	private final int pmCores;
	/**
	 * The processing capability of the first PM in the cloud.
	 */
	private final double pmProcessing;
	/**
	 * The amount of memory the first PM (of the cloud) has. This is used to
	 * determine the maximum size of the VMs we will request.
	 */
	private final long pmMem;

	/**
	 * All VMs that we deploy to the cloud are monitored and their CPU utilisation
	 * is used for scaling the virtual infrastructure of a particular application
	 */
	private final HashMap<VirtualMachine, HourlyVMMonitor> vmmonitors = new HashMap<VirtualMachine, HourlyVMMonitor>();
	/**
	 * We keep track of how many times we found the last VM completely unused for an
	 * particular executable
	 */
	private final HashMap<VirtualMachine, Integer> unnecessaryHits = new HashMap<VirtualMachine, Integer>();

	/**
	 * Allows us to remember which VM kinds fell out of use
	 */
	private final ArrayDeque<String> obsoleteVAs = new ArrayDeque<String>();

	/**
	 * Initialises the auto scaling mechanism
	 * 
	 * @param cloud the physical infrastructure to use to rent the VMs from
	 */
	public VirtualInfrastructure(final IaaSService cloud) {
		this.cloud = cloud;
		storage = cloud.repositories.get(0);
		ResourceConstraints rcForMachine = cloud.machines.get(0).getCapacities();
		pmCores = (int) rcForMachine.getRequiredCPUs();
		pmProcessing = rcForMachine.getRequiredProcessingPower();
		pmMem = rcForMachine.getRequiredMemory();
	}

	/**
	 * If a new executable is encountered for which we might need a set of VMs, we
	 * can communicate this here.
	 * 
	 * @param kind the type of the executable that we need the VMs for
	 */
	public void regNewVMKind(final String kind) {
		if (vmSetPerKind.get(kind) == null) {
			vmSetPerKind.put(kind, new ArrayList<VirtualMachine>());
		}
	}

	/**
	 * Arranges a new VM request to be sent to the cloud. If needed it registers the
	 * corresponding VMI with the cloud. It also prepares all internal data
	 * structures used for monitoring the VM.
	 * 
	 * <b>Warning:</b> The VM's resource requirements are not determined
	 * realistically. If the biggest PM in the infrastructure is having less than 4
	 * CPU cores, then this could lead to VMs that cannot be run on the cloud.
	 * 
	 * @param vmKind The executable for which we want a new VM for.
	 */
	protected void requestVM(final String vmKind) {
		if (underPrepVMPerKind.containsKey(vmKind)) {
			// Ignore the request as we have a VM in preparation already
			return;
		}
		// VMI handling
		VirtualAppliance va = (VirtualAppliance) storage.lookup(vmKind);
		if (va == null) {
			// A random sized VMI with a short boot procedure. The approximate size of the
			// VMI will be 1GB.
			va = new VirtualAppliance(vmKind, 15, 0, true, 1024 * 1024 * 1024);
			boolean regFail;
			do {
				regFail = !storage.registerObject(va);
				if (regFail) {
					// Storage ran out of space, remove the VA that became obsolete the longest time
					// in the past
					if (obsoleteVAs.isEmpty()) {
						throw new RuntimeException(
								"Configured with a repository not big enough to accomodate a the following VA: " + va);
					}
					String vaRemove = obsoleteVAs.pollFirst();
					storage.deregisterObject(vaRemove);
				}
			} while (regFail);
		}

		// VM size is a bit dependent on the VA used
		// This guarantees a deterministic VM resource set for each kind of application
		// The core count of a VM will be between 1-4.
		final int vmScaler = vmKind.length() % 4 + 1;
		try {
			VirtualMachine vm = cloud.requestVM(va,
					new ConstantConstraints(vmScaler, pmProcessing, vmScaler * pmMem / pmCores), storage, 1)[0];
			HourlyVMMonitor vmMonitor = new HourlyVMMonitor(vm);
			vmMonitor.startMon();
			vmmonitors.put(vm, vmMonitor);
			ArrayList<VirtualMachine> vmset = vmSetPerKind.get(vmKind);
			if (vmset.isEmpty()) {
				// VA became used again, no longer obsolete
				obsoleteVAs.remove(vmKind);
			}
			vmset.add(vm);
			underPrepVMPerKind.put(vmKind, vm);
			vm.subscribeStateChange(this);
		} catch (Exception vmm) {
			vmm.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Makes sure that the given VM is no longer having any local monitoring
	 * mechanism running and also that it is no longer present at the cloud.
	 * 
	 * @param vm The VM to be destroyed.
	 */
	protected void destroyVM(final VirtualMachine vm) {
		vmmonitors.remove(vm).finishMon();
		try {
			final String vmKind = vm.getVa().id;
			vmSetPerKind.get(vmKind).remove(vm);
			underPrepVMPerKind.remove(vmKind);
			if (VirtualMachine.State.DESTROYED.equals(vm.getState())) {
				// The VM was not even running when the decision about its destruction was made
				cloud.terminateVM(vm, true);
			} else {
				// The VM was initiated on the cloud, but we no longer need it
				vm.destroy(true);
			}
		} catch (VMManagementException e) {
			// Should not really happen
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * The auto scaling mechanism that is run regularly to determine if the virtual
	 * infrastructure needs some changes. The logic is the following:
	 * <ul>
	 * <li>if a VM has less than a given utilisation, then it is destroyed (unless
	 * it is the last VM for a given executable)</li>
	 * <li>if a VM is the last for a given executable, it is given an hour to
	 * receive a new job before it is destructed. <i>After this, one has to
	 * re-register the VM kind to receive new VMs.</i></li>
	 * <li>if an executable was just registered, it will receive a single new
	 * VM.</li>
	 * <li>if all the VMs of a given executable experience an average utilisation of
	 * a given minimum value, then a new VM is created.</li>
	 * </ul>
	 */
	@Override
	public void tick(long fires) {
		// Regular operation, the actual "autoscaler"

		// Determining if we need to get rid of a VM:
		Iterator<String> kinds = vmSetPerKind.keySet().iterator();
		while (kinds.hasNext()) {
			String kind = kinds.next();
			ArrayList<VirtualMachine> vmset = vmSetPerKind.get(kind);
			// Determining if we need a brand new kind of VM:
			if (vmset.isEmpty()) {
				// No VM with this kind yet, we need at least one for each so let's create one
				requestVM(kind);
				continue;
			} else if (vmset.size() == 1) {
				final VirtualMachine onlyMachine = vmset.get(0);
				// We will try to not destroy the last VM from any kind
				if (onlyMachine.underProcessing.isEmpty() && onlyMachine.toBeAdded.isEmpty()) {
					// It has no ongoing computation
					Integer i = unnecessaryHits.get(onlyMachine);
					if (i == null) {
						unnecessaryHits.put(onlyMachine, 1);
					} else {
						i++;
						if (i < 30) {
							unnecessaryHits.put(onlyMachine, i);
						} else {
							// After an hour of disuse, we just drop the VM
							unnecessaryHits.remove(onlyMachine);
							destroyVM(onlyMachine);
							// Last use of the VA, make it obsolete now => enable it to be removed from the
							// central storage
							obsoleteVAs.add(kind);
							kinds.remove();
						}
					}
					// We don't need to check if we need more VMs as it has no computation
					continue;
				}
				// The VM now does some stuff now so we make sure we don't try to remove it
				// prematurely
				unnecessaryHits.remove(onlyMachine);
				// Now we allow the check if we need more VMs.
			} else {
				boolean destroyed = false;
				for (int i = 0; i < vmset.size(); i++) {
					final VirtualMachine vm = vmset.get(i);
					if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
						// The VM has no task on it at the moment, good candidate
						if (vmmonitors.get(vm).getHourlyUtilisationPerc() < minUtilisationLevelBeforeDestruction) {
							// The VM's load was under 20% in the past hour, we might be able to get rid of
							// it
							destroyVM(vm);
							destroyed = true;
							i--;
						}
					}
				}
				if (destroyed) {
					// No need to check the average workload now, as we just destroyed a VM..
					continue;
				}
			}

			// We will not make new VM request decisions on this kind of VM before the
			// previous VM request of ours comes alive
			if (underPrepVMPerKind.containsKey(kind))
				continue;

			// We need to check if we need more VMs than we have at the moment
			double subHourUtilSum = 0;
			for (VirtualMachine vm : vmset) {
				subHourUtilSum += vmmonitors.get(vm).getHourlyUtilisationPerc();
			}
			if (subHourUtilSum / vmset.size() > maxUtilisationLevelBeforeNewVM) {
				// Average utilisation of VMs are over threshold, we need a new one
				requestVM(kind);
			}
		}
	}

	/**
	 * Asks Timed to notify us every two minutes. The auto scaling mechanism will be
	 * running with this frequency.
	 */
	public void startAutoScaling() {
		subscribe(120 * 1000);
	}

	/**
	 * If there are no further tasks for the virtual infrastructure, it is
	 * completely dismantled and the two minute notifications from Timed are
	 * cancelled.
	 */
	public void terminateScalingMechanism() {
		// No VMs have tasks running, all jobs finished, we don't have to work on the
		// infrastructure anymore.
		for (ArrayList<VirtualMachine> vmset : vmSetPerKind.values()) {
			while (!vmset.isEmpty()) {
				destroyVM(vmset.get(vmset.size() - 1));
			}
		}
		unsubscribe();
		System.out.println("Autoscaling mechanism terminated.");
	}

	/**
	 * If a VM starts to run, we should enable further VMs to start or destruct
	 */
	@Override
	public void stateChanged(final VirtualMachine vm, final State oldState, final State newState) {
		if (VirtualMachine.State.RUNNING.equals(newState)) {
			underPrepVMPerKind.remove(vm.getVa().id);
			vm.unsubscribeStateChange(this);
		} else if (VirtualMachine.State.NONSERVABLE.equals(newState)) {
			underPrepVMPerKind.remove(vm.getVa().id);
			vm.unsubscribeStateChange(this);
		}
	}
}
