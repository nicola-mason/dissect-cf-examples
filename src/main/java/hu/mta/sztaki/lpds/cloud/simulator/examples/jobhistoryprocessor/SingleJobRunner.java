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
 *  (C) Copyright 2015, Gabor Kecskemeti (kecskemeti.gabor@sztaki.mta.hu)
 */
package hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption.ConsumptionEvent;

public class SingleJobRunner implements VirtualMachine.StateChange, ConsumptionEvent {
	private Job toProcess;
	private VirtualMachine[] vmSet;
	private MultiIaaSJobDispatcher parent;
	private int readyVMCounter = 0;
	private int completionCounter = 0;

	public SingleJobRunner(final Job runMe, final VirtualMachine[] onUs, MultiIaaSJobDispatcher forMe) {
		toProcess = runMe;
		vmSet = onUs;
		parent = forMe;
		// Ensuring we receive state dependent events about the new VMs
		for (int i = 0; i < vmSet.length; i++) {
			vmSet[i].subscribeStateChange(this);
		}
		// Increasing ignorecounter in order to sign that the job in this runner
		// is not yet finished (so the premature termination of the simulation
		// will show the job ignored)
		parent.ignorecounter++;
	}

	@Override
	public void stateChanged(final VirtualMachine vm, final VirtualMachine.State oldState,
			final VirtualMachine.State newState) {
		// If the dispatching process was cancelled
		if (parent.isStopped()) {
			switch (newState) {
			case NONSERVABLE:
			case DESTROYED:
			case INITIAL_TR:
				// OK
				break;
			default:
				try {
					vm.unsubscribeStateChange(this);
					vm.destroy(true);
				} catch (VMManager.VMManagementException ex) {
					// Ignore as we want to get rid of the VM
				}
			}
			return;
		}

		// Now to the real business of having a VM that is actually capable of
		// running the job
		if (newState.equals(VirtualMachine.State.RUNNING)) {
			// Ensures that jobs inteded for parallel execution are really run
			// in parallel
			if (++readyVMCounter == vmSet.length) {
				// Mark that we start the job / no further queuing
				toProcess.started();
				try {
					// vmset could get null if the compute task is rapidly terminating!
					for (int i = 0; vmSet!=null && i < vmSet.length; i++) {
						// run the job's relevant part in the VM
						vmSet[i].newComputeTask(
								toProcess.getExectimeSecs()
										* vmSet[i].getResourceAllocation().allocated.getRequiredCPUs(),
								ResourceConsumption.unlimitedProcessing, this);
					}
				} catch (Exception e) {
					System.err.println(
							"Unexpected network setup issues while trying to send a new compute task to one of the VMs supporting job processing");
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
	}

	/**
	 * Event handler for the completion of the job
	 */
	@Override
	public void conComplete() {

		if (++completionCounter == vmSet.length) {
			// everything went smoothly we mark it in the job
			toProcess.completed();
			try {
				// the VMs are no longer needed
				for (int i = 0; i < vmSet.length; i++) {
					vmSet[i].unsubscribeStateChange(this);
					vmSet[i].destroy(false);
					vmSet[i] = null;
				}
			} catch (VMManager.VMManagementException e) {
				System.err.println("VM could not be destroyed after job completion.");
				e.printStackTrace();
				System.exit(1);
			}
			parent.increaseDestroyCounter(completionCounter);
			parent.ignorecounter--;
			parent = null;
			vmSet = null;
			toProcess = null;
		}
	}

	@Override
	public void conCancelled(ResourceConsumption problematic) {
		// just ignore this has happened :)
		// In the current setup we are not supposed to have failing jobs
	}
}
