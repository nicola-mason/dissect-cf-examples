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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SplittableRandom;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;

/**
 * An alternative implementation to the threshold based virtual infrastructure
 * prioritising VM creation instead of destruction.
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2019"
 */
public class VMCreationPriorityVI extends VirtualInfrastructure {

	/**
	 * A random generator to allow us the removal of the unnecessary VMs in a random
	 * order
	 */
	private static final SplittableRandom rnd = new SplittableRandom(0);

	/**
	 * We keep track of how many times we found the last VM completely unused for an
	 * particular executable
	 */
	private final HashMap<VirtualMachine, Integer> unnecessaryHits = new HashMap<VirtualMachine, Integer>();

	/**
	 * Initialises the auto scaling mechanism
	 * 
	 * @param cloud the physical infrastructure to use to rent the VMs from
	 */
	public VMCreationPriorityVI(final IaaSService cloud) {
		super(cloud);
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
		final Iterator<String> kinds = vmSetPerKind.keySet().iterator();
		while (kinds.hasNext()) {
			final String kind = kinds.next();
			final ArrayList<VirtualMachine> vmset = vmSetPerKind.get(kind);
			// Determining if we need a brand new kind of VM:
			if (vmset.isEmpty()) {
				// No VM with this kind yet, we need at least one for each so let's create one
				requestVM(kind);
			} else {

				final ArrayList<VirtualMachine> underUtil = new ArrayList<VirtualMachine>();
				double subHourUtilSum = 0;
				for (VirtualMachine vm : vmset) {
					double currVMUtil = getHourlyUtilisationPercForVM(vm);
					if (currVMUtil < ThresholdBasedVI.minUtilisationLevelBeforeDestruction
							&& vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
						//
						underUtil.add(vm);
					}
					subHourUtilSum += currVMUtil;
				}
				// We need to check if we need more VMs than we have at the moment
				if (subHourUtilSum / vmset.size() > ThresholdBasedVI.maxUtilisationLevelBeforeNewVM) {
					// Average utilisation of VMs is over threshold, we need a new one
					requestVM(kind);
				} else if (vmset.size() == 1) {
					// No we don't need more VMs, in fact we only have one VM at the moment
					final VirtualMachine onlyMachine = vmset.get(0);
					if (onlyMachine.underProcessing.isEmpty() && onlyMachine.toBeAdded.isEmpty()) {
						// Our single VM has no ongoing computation
						// We will try to not destroy the last VM from any kind
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
								kinds.remove();
							}
						}
					} else {
						// The single VM now does some stuff now so we make sure we don't try to remove
						// it prematurely
						unnecessaryHits.remove(onlyMachine);
					}
				} else if (!underUtil.isEmpty()) {
					// There are VMs with no tasks on them, but we have less than ideal utilisation,
					// we can drop one of them, and we do so in a random order
					destroyVM(underUtil.get(rnd.nextInt(underUtil.size())));
				}
			}
		}
	}
}
