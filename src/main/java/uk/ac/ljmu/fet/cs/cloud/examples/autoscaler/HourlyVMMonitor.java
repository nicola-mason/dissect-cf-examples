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

import java.util.Arrays;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;

/**
 * Can monitor a VM's CPU utilisation in an hour (implements a similar behaviour
 * as MonitorConsumption). It does so by querying the VM's total processed
 * activities every five minutes. So the utilisation details this class gives
 * out have a resolution of 5 minutes. The five minute polling interval allows a
 * much higher performance simulation with a little less accuracy compared to
 * how the MonitorConsumption class works from DISSECT-CF.
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2019"
 */
public class HourlyVMMonitor extends Timed implements VirtualMachine.StateChange {
	/**
	 * The VM to monitor
	 */
	private final VirtualMachine toCheck;
	/**
	 * The utilisation records of the past hour. This array is overwritten in a
	 * circular fashion. For details see {@link HourlyVMMonitor#currIndex}
	 */
	private final double[] utilisations = new double[12];
	/**
	 * The maximum utilisation possible in a given hour, note that this value is
	 * determined only once the VM is running as the VM might not even have any
	 * processing capabilities beforehand.
	 */
	private double hourlyPossibleUtil = Double.MAX_VALUE;
	/**
	 * The location of the next utilisation array entry. Note that this index can be
	 * bigger than the size of the utilisations array. The modulo of this index
	 * value is used to actually address the utilisations array.
	 */
	private int currIndex;

	/**
	 * If one called to finish the monitoring, this turns true.
	 */
	private boolean finished = false;

	/**
	 * Prepares the monitoring of a particular VM
	 * 
	 * @param toCheck The VM that can be monitored by this class.
	 */
	public HourlyVMMonitor(final VirtualMachine toCheck) {
		toCheck.subscribeStateChange(this);
		this.toCheck = toCheck;
	}

	/**
	 * Initiates the periodic monitoring with the help of Timed. Reinitialises the
	 * utilisations and currindex data members.
	 */
	public void startMon() {
		if (subscribe(5 * 60 * 1000)) {
			Arrays.fill(utilisations, toCheck.getTotalProcessed());
			currIndex = 0;
			finished = false;
		}
	}

	/**
	 * Cancels the monitoring of the VM.
	 */
	public void finishMon() {
		finished = true;
	}

	/**
	 * Updates the utilisations array by querying the total processed value of the
	 * VM.
	 */
	@Override
	public void tick(final long fires) {
		if (finished) {
			unsubscribe();
		} else {
			utilisations[currIndex % utilisations.length] = toCheck.getTotalProcessed();
			currIndex++;
		}
	}

	/**
	 * Allows the user to determine what was the average utilisation level in the
	 * past hour. Calling this method repeatedly within the next five minutes makes
	 * no sense as it will only report an updated value in every five minutes.
	 * 
	 * @return The percentage of CPU utilisation of this VM.
	 */
	public double getHourlyUtilisationPerc() {
		if (isSubscribed()) {
			return currIndex == 0 ? 0
					: (utilisations[(currIndex - 1) % utilisations.length]
							- utilisations[currIndex % utilisations.length]) / hourlyPossibleUtil;
		} else {
			throw new IllegalStateException("Cannot get the hourly utilisation for a non-monitored VM");
		}
	}

	/**
	 * Calculates the hourly possible utilisation of the VM after it is actually
	 * running (and thus has a chance to do any activities)
	 */
	@Override
	public void stateChanged(final VirtualMachine vm, final State oldState, final State newState) {
		if (VirtualMachine.State.RUNNING.equals(newState)) {
			hourlyPossibleUtil = toCheck.getPerTickProcessingPower() * 3600000;
			vm.unsubscribeStateChange(this);
		}
	}
}
