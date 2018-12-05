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
package hu.mta.sztaki.lpds.cloud.simulator.examples.vmstat;

import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.DCFJob;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceSpreader;

/**
 * This extension of the job allows representing network activities alongside
 * the regular properties of the original job classes. It also enables to store
 * temporary information to provide some insight about where the job is
 * executed.
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2017" *
 */
public class ComplexJob extends DCFJob {
	public ResourceSpreader target;

	public int rx = -1, tx = -1;

	public ComplexJob(String id, long submit, long queue, long exec, int nprocs, double ppCpu, long ppMem, String user,
			String group, String executable, Job preceding, long delayAfter) {
		super(id, submit, queue, exec, nprocs, ppCpu, ppMem, user, group, executable, preceding, delayAfter);
	}
}