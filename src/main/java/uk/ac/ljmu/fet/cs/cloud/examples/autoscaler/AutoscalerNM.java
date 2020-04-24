package uk.ac.ljmu.fet.cs.cloud.examples.autoscaler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;

/**
 * This autoscaler builds on the PoolingVI concept of keeping some spare virtual
 * machines. However the key difference is that the number of spare VMs needed
 * is increased or decreased according to the number of VMs requested or
 * destroyed.
 * 
 * @author Nicola Mason
 *
 */

public class AutoscalerNM extends VirtualInfrastructure {
	/**
	 * Number of VMs to keep spare
	 */
	public static int numSpareVMs = 4;

	/**
	 * Initialises autoscaling mechanism
	 * 
	 * @param cloud Physical infrastructure to take VMs from
	 */
	public AutoscalerNM(IaaSService cloud) {
		super(cloud);
	}

	/**
	 * Autoscaling mechanism to determine if changes need to be made.
	 * 
	 * If the number of VMs in a set is less than the no. of VMs to be kept spare,
	 * request a VM.
	 * 
	 * If a VM is not being used, add it to a list of unused VMs.
	 * 
	 * If the number of unused VMs reaches the number of VMs to be kept spare,
	 * destroy the unused VMs.
	 * 
	 * Otherwise: If there are more VMs than necessary, destroy a VM and decrease
	 * the number of VMs to be kept spare.
	 * 
	 * If there are not more VMs than necessary, request a VM and increase the
	 * number of VMs to be kept spare.
	 * 
	 */
	@Override
	public void tick(long fires) {
		// a list of applications that need a virtual infrastructure
		final Iterator<String> vmKinds = vmSetPerKind.keySet().iterator();
		while (vmKinds.hasNext()) {
			final String vmKind = vmKinds.next();
			final ArrayList<VirtualMachine> vmSet = vmSetPerKind.get(vmKind);
			if (vmSet.size() < numSpareVMs) {
				requestVM(vmKind);
			}
			// list of VMs that are not being used
			final ArrayList<VirtualMachine> unusedVMs = new ArrayList<VirtualMachine>();
			for (VirtualMachine vm : vmSet) {
				// logic to determine if VM is unused
				if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
					unusedVMs.add(vm);
				}
			}
			if (unusedVMs.size() == numSpareVMs) {
				if (unusedVMs.size() == vmSet.size()) {
					// all VMs are unused so destroy the VMs
					destroyVM(vmSet.get(vmSet.size() - 1));
				}
			} else {
				if (unusedVMs.size() > numSpareVMs) {
					// We have more VMs than we need so drop one and decrease number of VMs to keep
					// spare
					destroyVM(vmSet.get(0));
					numSpareVMs--;
				} else {
					requestVM(vmKind);
					numSpareVMs++;
				}
			}
		}
	}
}
