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

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;

/**
 * This autoscaler tries to keep a given number of virtual machines for every
 * executable completely unused (i.e., prepares a few to be always ready to
 * accept new jobs).
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2019"
 */
public class PoolingVI extends VirtualInfrastructure {
	/**
	 * The number of VMs we should keep unused.
	 */
	public static final int poolHeadRoom = 4;

	/**
	 * We keep track of how many times we found the last VMs completely unused for
	 * an particular executable
	 */
	private final HashMap<String, Integer> unnecessaryHits = new HashMap<String, Integer>();

	/**
	 * Initialises the auto scaling mechanism
	 * 
	 * @param cloud the physical infrastructure to use to rent the VMs from
	 */
	public PoolingVI(final IaaSService cloud) {
		super(cloud);
	}

	/**
	 * The auto scaling mechanism that is run regularly to determine if the virtual
	 * infrastructure needs some changes. The logic is the following:
	 * <ul>
	 * <li>if we have fewer VMs than the minimum pool headroom, we will create a
	 * VM</li>
	 * <li>if we have fewer unused VMs than the minimum pool headroom, we will
	 * create a VM</li>
	 * <li>if all VMs are unused, we will consider the pool to be completely
	 * destroyed after an hour of disuse</li>
	 * <li>if we have more VMs unused than the pool headroom we will destroy one of
	 * the unused ones.</li>
	 * </ul>
	 */
	@Override
	public void tick(long fires) {
		// Regular operation, the actual "autoscaler"

		final Iterator<String> kinds = vmSetPerKind.keySet().iterator();
		while (kinds.hasNext()) {
			final String kind = kinds.next();
			final ArrayList<VirtualMachine> vmset = vmSetPerKind.get(kind);
			// Determining if we need a brand new kind of VM:
			if (vmset.size() < poolHeadRoom) {
				// Not enough VMs with this kind yet, we need at least the X for each so let's
				// create one (X being the headroom defined as a constant for the class)
				requestVM(kind);
			} else {
				// Let's detect the current VM utilisation pattern
				final ArrayList<VirtualMachine> unusedVMs = new ArrayList<VirtualMachine>();
				for (final VirtualMachine vm : vmset) {
					if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
						unusedVMs.add(vm);
					}
				}
				if (unusedVMs.size() < poolHeadRoom) {
					// Too many VMs are used in the pool, we need to increase the VM count so new
					// tasks can already arrive for ready and unused VMs
					requestVM(kind);
				} else if (vmset.size() == unusedVMs.size()) {
					// All VMs are unused, the pool might be unnecessary after all.
					Integer i = unnecessaryHits.get(kind);
					if (i == null) {
						unnecessaryHits.put(kind, 1);
					} else {
						i++;
						if (i < 30) {
							// It is not unnecessary yet, we just keep count on how many times we seen this
							// pool unused
							unnecessaryHits.put(kind, i);
						} else {
							// After an hour of disuse, we just drop the VMs
							unnecessaryHits.remove(kind);
							while (!vmset.isEmpty()) {
								destroyVM(vmset.get(vmset.size() - 1));
							}
							kinds.remove();
						}
					}
				} else {
					// We have some of our VMs doing stuff, so we don't want the current round to
					// count towards the unnecessary hits.
					unnecessaryHits.remove(kind);
					if (unusedVMs.size() > poolHeadRoom) {
						// We have more VMs than we need at the moment, we will drop one
						destroyVM(unusedVMs.get(0));
					}
				}
			}
		}
	}
}
