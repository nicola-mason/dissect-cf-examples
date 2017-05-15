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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.DCFJob;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.GWFReader;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.util.CloudLoader;

public class CLIExample {

	public static String[] readInputStreamAsStringSet(InputStream fileToRead) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(fileToRead));
		ArrayList<String> result = new ArrayList<String>();
		for (String line; (line = br.readLine()) != null; result.add(line))
			;
		br.close();
		return result.toArray(new String[result.size()]);
	}

	/**
	 * Params:
	 * <ol>
	 * <li>The GWF trace file to put as background load
	 * <li>The cloud which will be hosting the background load and the workflow
	 * <li>The first job to be included in the background load from the
	 * tracefile
	 * <li>The file which contains the workflow's description to run on top of
	 * the background load
	 * <li>The wall time for the workflow (it will be terminated if it does not
	 * finish its execution)
	 * </ol>
	 */
	public static void main(String[] args) throws Exception {
		IaaSService iaas = CloudLoader.loadNodes(args[1]);
		Timed.simulateUntilLastEvent();
		int jobindex = Integer.parseInt(args[2]);
		ArrayList<ArrayList<VMUseSpecification>> wf = CombineWorkloadwithWorkflow.parseWorkflow(iaas,
				readInputStreamAsStringSet(new FileInputStream(args[3])));
		long before = System.currentTimeMillis();
		CombineWorkloadwithWorkflow combiner = new CombineWorkloadwithWorkflow(iaas,
				new GWFReader(args[0], 0, 2000000, false, DCFJob.class), jobindex, wf, Long.parseLong(args[4]));
		ArrayList<HashMap<VirtualMachine, ArrayList<Long>>> runtimeResults = combiner.putWorkflowOnLoad();
		if (runtimeResults != null) {
			int counter = 0;
			for (HashMap<VirtualMachine, ArrayList<Long>> parallelsection : runtimeResults) {
				System.out.println(counter + ". parallel section");
				for (VirtualMachine vm : parallelsection.keySet()) {
					System.out.println("VM level runtime details: " + parallelsection.get(vm));
				}
				System.out.println("=====");
				counter++;
			}
		} else {
			System.err.println("Workflow ran too long, did not get enough trace data!");
		}
		System.out.println(System.currentTimeMillis() - before);
	}
}
