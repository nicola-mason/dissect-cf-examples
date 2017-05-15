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
 *  (C) Copyright 2014-2015, Gabor Kecskemeti (kecskemeti.gabor@sztaki.mta.hu)
 *  (C) Copyright 2016-2017, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 */
package hu.mta.sztaki.lpds.cloud.simulator.examples.backgroundworkload;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import java.util.ArrayList;

class JobSequence extends ConsumptionEventAdapter implements VirtualMachine.StateChange {

	public static final int seqMarker = 1000000000;
	VirtualMachine vm;
	double[] seq;
	int loc;
	ArrayList<Long> statusReports;
	Repository dataFrom, dataTo;

	public JobSequence(VirtualMachine vm, double[] seq, Repository dataStorage) {
		this.vm = vm;
		this.seq = seq;
		loc = 0;
		statusReports = new ArrayList<Long>();
		dataFrom = dataStorage;
		vm.subscribeStateChange(this);
	}

	@Override
	public void conComplete() {
		super.conComplete();
		statusReports.add(Timed.getFireCount());
		loc++;
		if (loc < seq.length) {
			submit();
		} else {
			try {
				vm.destroy(false);
				vm.unsubscribeStateChange(this);
			} catch (VMManager.VMManagementException e) {
				e.printStackTrace();
			}
		}
	}

	void submit() {
		try {
			if (seq[loc] >= seqMarker) {
				NetworkNode.initTransfer((int) (seq[loc] - seqMarker), ResourceConsumption.unlimitedProcessing,
						dataFrom, dataTo, this);
			} else {
				vm.newComputeTask(seq[loc], ResourceConsumption.unlimitedProcessing, this);
			}
		} catch (NetworkNode.NetworkException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stateChanged(VirtualMachine vm, VirtualMachine.State oldState, VirtualMachine.State newState) {
		if (newState.equals(VirtualMachine.State.RUNNING)) {
			dataTo = vm.getResourceAllocation().getHost().localDisk;
			statusReports.add(Timed.getFireCount());
			submit();
		}
	}
}
