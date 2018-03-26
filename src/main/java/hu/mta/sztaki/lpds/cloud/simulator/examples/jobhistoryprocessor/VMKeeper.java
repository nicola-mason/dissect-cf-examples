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
package hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor;

import java.util.Comparator;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;

/**
 * This class could receive VMs to be kept for longer periods of time even if
 * they are not used at the moment. Useful to match billing periods
 * 
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2017"
 */
public class VMKeeper extends Timed implements VirtualMachine.StateChange {
	/**
	 * Allows ordering the keeper objects based on the VM's size they host.
	 * 
	 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
	 *         Moores University, (c) 2017"
	 *
	 */
	private static class KeeperComparator implements Comparator<VMKeeper> {
		@Override
		public int compare(VMKeeper o1, VMKeeper o2) {
			PhysicalMachine.ResourceAllocation ra1 = o1.vm.getResourceAllocation();
			PhysicalMachine.ResourceAllocation ra2 = o2.vm.getResourceAllocation();
			if (ra1 == null) {
				if (ra2 == null) {
					return 0;
				} else {
					return -1;
				}
			} else {
				if (ra2 == null) {
					return 1;
				} else {
					return ra1.allocated.compareTo(ra2.allocated);
				}
			}
		}
	}

	public static interface ReleaseListener {
		void released(VMKeeper me);
	}

	public static final boolean keepVMs;
	public static int prematureVMs = 0;
	public static int expiredVMs = 0;

	static {
		keepVMs = System.getProperty("hu.mta.sztaki.lpds.cloud.simulator.examples.keepVMs") != null;
		if (keepVMs)
			System.err.println("VMKeeper is switched on!");
	}

	/**
	 * Allows sorting keepers based on their VM's resource size
	 */
	public static KeeperComparator compareKeepers = new KeeperComparator();

	/**
	 * The VM kept by this VMKeeper
	 */
	private final VirtualMachine vm;
	/**
	 * the IaaS that hosts the VM
	 */
	private final IaaSService onCloud;
	/**
	 * The length of the billing period
	 */
	private final long billingPeriod;
	/**
	 * When did we start the VM's billing period
	 */
	private final long startTime;

	private boolean alive;

	private ReleaseListener listener;

	public VMKeeper(IaaSService onCloud, VirtualMachine vm, long billingPeriod) {
		this.onCloud = onCloud;
		this.vm = vm;
		this.billingPeriod = billingPeriod;
		alive = isServable();
		startTime = Timed.getFireCount();
		startSubscription();
	}

	/**
	 * Tells if the VM is ready to be used
	 * 
	 * @return true if the VM could be acquired
	 */
	public boolean isAlive() {
		return alive;
	}

	/**
	 * Helps to determine if the VM will ever come running
	 * 
	 * @return true if the VM is queued by a scheduler or it is already running,
	 *         false otherwise
	 */
	public boolean isServable() {
		return !VirtualMachine.State.NONSERVABLE.equals(vm.getState());
	}

	/**
	 * Determines if the kept VM would be able to host a VM request with a resource
	 * set specified in the parameter.
	 * 
	 * @param rc
	 *            The resource set that should fit in the VM held by this keeper
	 * @return true if the VM could host such resource set, false otherwise
	 */
	public boolean wouldFit(ResourceConstraints rc) {
		PhysicalMachine.ResourceAllocation ra = vm.getResourceAllocation();
		return ra != null && rc.compareTo(vm.getResourceAllocation().allocated) <= 0;
	}

	/**
	 * Provides access to the VM kept by this keeper. The VM will not be destroyed
	 * by this VMKeeper before it is released.
	 * 
	 * @return the virtual machine kept by this keeper (or null if the VM is already
	 *         in use by someone else)
	 */
	public VirtualMachine acquire() {
		if (isSubscribed()) {
			unsubscribe();
			return vm;
		} else {
			return null;
		}
	}

	/**
	 * Determines if the VM behind this keeper is used by someone else or not
	 * 
	 * @return true if the VM is not used, false otherwise
	 */
	public boolean isFree() {
		return isSubscribed();
	}

	/**
	 * Let the VMKeeper know that its VM is no longer in use, and thus we should be
	 * ready to terminate it once the billing period is over and noone is willing to
	 * use it
	 * 
	 * @param vm
	 *            the VM to be dropped
	 */
	public void release(VirtualMachine vm) {
		if (vm == this.vm) {
			if (VMKeeper.keepVMs) {
				startSubscription();
				if (listener != null) {
					listener.released(this);
				}
			} else {
				destroyMyVM();
			}
		} else {
			throw new RuntimeException("Tried to release a VM which is not kept by this VMKeeper");
		}
	}

	/**
	 * Keeps the VM so it stays alive until the latest billing period is over
	 */
	private void startSubscription() {
		subscribe(Math.max(0, billingPeriod - (Timed.getFireCount() - startTime) % billingPeriod - 1));
	}

	/**
	 * Allows VMs to be terminated independently from their billing period - e.g.,
	 * to release some resources for other VM requests
	 * 
	 * Note: this operation is only possible if the VM is not acquired at the moment
	 */
	public void prematureDestroy() {
		if (isSubscribed()) {
			prematureVMs++;
			tick(Timed.getFireCount());
		} else {
			throw new RuntimeException("The VM is in use, it must be released before destruction");
		}
	}

	/**
	 * Helper for the VM's destroy function, ensuring there are no tasks running on
	 * a VM when a destroy is called
	 */
	private void destroyMyVM() {
		try {
			if (VirtualMachine.preStartupStates.contains(vm.getState())) {
				// Not yet scheduled
				onCloud.terminateVM(vm, true);
				vm.subscribeStateChange(this);
			} else {
				// We are on a PM
				vm.destroy(true);
			}
			alive = false;
		} catch (VMManager.NoSuchVMException e) {
			throw new RuntimeException(e);
		} catch (VMManagementException e) {
			// Ignore
		}
	}

	@Override
	public void stateChanged(VirtualMachine vm, State oldState, State newState) {
		if (VirtualMachine.State.SHUTDOWN.equals(newState)) {
			try {
				vm.destroy(false);
			} catch (VMManagementException e) {
				// Should not happen, as we terminated the VM before
				throw new RuntimeException(e);
			}
			vm.unsubscribeStateChange(this);
		}
	}

	/**
	 * We receive this tick just before the billing period expires and terminate the
	 * VM immediately as the VM is unused at the moment.
	 */
	@Override
	public void tick(long fires) {
		expiredVMs++;
		destroyMyVM();
		unsubscribe();
	}

	public void setListener(ReleaseListener listener) {
		this.listener = listener;
	}
}
