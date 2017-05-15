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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.MultiIaaSJobDispatcher;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.JobListAnalyser;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.GenericTraceProducer;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;

public class CombineWorkloadwithWorkflow implements VirtualMachine.StateChange {

	final private IaaSService iaas;
	private int notCompletedCount = 0;
	final private MultiIaaSJobDispatcher jp;
	private int maxPreparedJobNum;
	private int currentParallelBlock = 0;
	/**
	 * <ul>
	 * <li>Workflow: {Parallel Block -> Parallel Block -> ... -> Parallel Block
	 * }
	 * <li>Parallel Block: { Parallel Single VM Block, Parallel Single VM Block,
	 * ... Parallel Single VM Block }
	 * <li>Parallel Single VM Block: { VMWorkload, VMWorkload, ... VMWorkload }
	 * <li>VMWorkload: { Processed by a single VM, as seen in JobSequence.java }
	 * </ul>
	 */
	final private ArrayList<ArrayList<VMUseSpecification>> workflow;
	/**
	 * For each parallel block's VMs we have a sequence of task completion times
	 * on the VM
	 */
	final private ArrayList<HashMap<VirtualMachine, ArrayList<Long>>> completionSequence = new ArrayList<HashMap<VirtualMachine, ArrayList<Long>>>();
	private boolean workflowPumped = false;

	/**
	 * Initiates the workload combination procedure (launches the worlkoad!).
	 *
	 * Please note that this class assumes exclusive access to the simulator!
	 * (i.e., if some other uses are also present then this class might ruin the
	 * expected behavior of them).
	 *
	 * @param iaastoUse
	 *            to which IaaS we need to send the Workflow ( it is assumed
	 *            that the IaaS's sole purpose is the testing of this workload)
	 * @param jobs
	 *            what kind of workload should be used behind the workflow
	 * @param startIndex
	 *            the initial position in the workload where its execution
	 *            should start
	 * @param workflowToExecute
	 *            the workflow that this cloud needs to execute (with the
	 *            background load added). In the format of: ParallelSection,
	 *            ParallelSection..., where each parallel section consists of a
	 *            list of a parallel blocks where the kind of VM is the same.
	 * @param maxLen
	 *            the maximum expected execution time of the workflow. (if the
	 *            workflow runs for longer, then the putWorkflowOnLoad function
	 *            will not give useful result.)
	 * @throws Exception
	 *             when there are problems with the trace data.
	 */
	public CombineWorkloadwithWorkflow(IaaSService iaastoUse, GenericTraceProducer jobProducer, int startIndex,
			ArrayList<ArrayList<VMUseSpecification>> workflowToExecute, long maxLen) throws Exception {
		resetIaaS(iaastoUse);
		List<Job> jobs = jobProducer.getAllJobs();
		Collections.sort(jobs, JobListAnalyser.startTimeComparator);
		long endTime = jobs.get(startIndex).getSubmittimeSecs() * 1000 + maxLen;
		int endIdx = startIndex + 50;
		while (endIdx < jobs.size() && (jobs.get(endIdx).getSubmittimeSecs() * 1000) < endTime) {
			endIdx++;
		}
		if (endIdx >= jobs.size() || (jobs.get(endIdx).getSubmittimeSecs() * 1000) < endTime
				|| endIdx - startIndex < 500) {
			throw new Exception("Not enough trace data");
		}
		iaas = iaastoUse;
		maxPreparedJobNum = endIdx - startIndex;
		workflow = workflowToExecute;

		double minProcPower = Double.MIN_VALUE;
		for (ArrayList<VMUseSpecification> parallelSection : workflow) {
			// initializes completion sequences for every parallel block
			completionSequence.add(new HashMap<VirtualMachine, ArrayList<Long>>());
			for (VMUseSpecification vmuse : parallelSection) {
				minProcPower = Math.min(minProcPower, vmuse.vmReqs.getRequiredProcessingPower());
				vmuse.seqExecutors.clear();
			}
		}

		// Starts the background load
		jp = new MultiIaaSJobDispatcher(new BoundedTrace(jobs, startIndex, endIdx),
				Collections.singletonList(iaastoUse));
		jp.setUsableProcPower(minProcPower, true);
	}

	/**
	 * Reverts the IaaS service to a state that is matching a just created one.
	 *
	 * WARINING: use this function carefully, once called all VMs and their
	 * tasks will be lost.
	 *
	 * @param iaastoUse
	 *            the IaaS service that needs to be set to its pristine state.
	 * @throws hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException
	 * @throws hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException
	 * @throws hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService.IaaSHandlingException
	 */
	public static void resetIaaS(IaaSService iaastoUse)
			throws VMManager.VMManagementException, NetworkNode.NetworkException, IaaSService.IaaSHandlingException {
		do {
			for (PhysicalMachine pm : iaastoUse.machines) {
				for (VirtualMachine vm : pm.publicVms) {
					vm.destroy(true);
				}
			}
			Timed.simulateUntilLastEvent();
		} while (iaastoUse.sched.getQueueLength() != 0);
		for (PhysicalMachine pm : iaastoUse.machines) {
			if (!PhysicalMachine.ToOfforOff.contains(pm.getState())) {
				pm.switchoff(null);
			}
		}
		Timed.simulateUntilLastEvent();
		ArrayList<PhysicalMachine> myMachineList = new ArrayList<PhysicalMachine>(iaastoUse.machines);
		for (PhysicalMachine pm : myMachineList) {
			iaastoUse.deregisterHost(pm);
		}
		Timed.simulateUntilLastEvent();
		Timed.resetTimed();
		for (PhysicalMachine pm : myMachineList) {
			iaastoUse.registerHost(pm);
		}
	}

	/**
	 * Ensures that all VMs will run that should be executed in parallel
	 * according to the workflow definition
	 *
	 * It only executes a single parallel section! Further sections are only
	 * executed when all the currently started VMs complete.
	 *
	 * @throws Exception
	 */
	private void executeSingleParallelSection() throws Exception {
		HashMap<VirtualMachine, ArrayList<Long>> statusreports = completionSequence.get(currentParallelBlock);
		for (VMUseSpecification parallelSingleVMBlock : workflow.get(currentParallelBlock)) {
			int parallelVMCount = parallelSingleVMBlock.parallelSeqs.size();
			notCompletedCount += parallelVMCount;
			VirtualMachine[] vms = iaas.requestVM(parallelSingleVMBlock.va, parallelSingleVMBlock.vmReqs,
					parallelSingleVMBlock.vaStore, parallelVMCount);
			for (int j = 0; j < vms.length; j++) {
				parallelSingleVMBlock.seqExecutors.add(vms[j]);
				JobSequence seq = new JobSequence(vms[j], parallelSingleVMBlock.parallelSeqs.get(j),
						parallelSingleVMBlock.dataStore);
				statusreports.put(vms[j], seq.statusReports);
				vms[j].subscribeStateChange(this);
			}
		}
		currentParallelBlock++;
		workflowPumped = currentParallelBlock == workflow.size();
	}

	/**
	 * Monitors the currently controlled parallel VMs (assumes that the VMs will
	 * be destroyed automatically by the class JobSequence once they executed
	 * all their sequential tasks). If all the parallel VMs are destroyed, then
	 * it initiates a new parallel section, or if there are no further parallel
	 * sections, then it stops the simulation.
	 *
	 * @param oldState
	 * @param newState
	 */
	@Override
	public void stateChanged(VirtualMachine vm, State oldState, State newState) {
		if (newState.equals(VirtualMachine.State.DESTROYED)) {
			notCompletedCount--;
			if (notCompletedCount == 0) {
				if (workflowPumped) {
					jp.stopTraceProcessing();
				} else {
					try {
						executeSingleParallelSection();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	/**
	 *
	 * @param iaas
	 *            the IaaS in which the repositories referred by the workflow
	 *            are present. <i>Please note, that the function registers the
	 *            virtual appliances described in this workflow!</i>
	 * @param wflines
	 *            a textual representation of the workflow with the following
	 *            format:
	 *            <ul>
	 *            <li># comment lines start with a '#' mark at the beginning
	 *            <li>Parallel sections start with a line containing the
	 *            following string "PSSTART"
	 *            <li>A virtual machine definition looks like the following:
	 *            <ul>
	 *            <li>The line starts with the string "VMDEF "
	 *            <li>The line is delimited with spaces.
	 *            <li>The line must contain the following four definitions:
	 *            "VA=", "RC=", "VAST=", "DATA=".
	 *            <p>
	 *            Where "VA=" defines the virtual appliance that will be used as
	 *            the base of the VM. The VA's definition will consist of the
	 *            following comma separated data:
	 *            <ul>
	 *            <li>the name of the VA
	 *            <li>the boot time of the VM with such VA (in ms)
	 *            <li>if the VM will be hosted on a disk shared over the
	 *            network, then the average amount of bytes the network
	 *            transfers the VM needs to transfer between the VA's repository
	 *            and the Physical machine that will host the VM. If the VM is
	 *            locally hosted on the physical machine's disk, then this value
	 *            is 0.
	 *            <li>The size of the VA in bytes.
	 *            </ul>
	 *            Please note, there are no spaces (" ") possible within VA
	 *            definitions!
	 *            <p>
	 *            Then "RC=" defines the resource demands of the VM to be
	 *            defined. It consists of the following comma separated data:
	 *            <ul>
	 *            <li>number of VCPU cores needed by this kind of VM.
	 *            <li>minimum processing power a VCPU core should prosess (in
	 *            million instructions/ms)
	 *            <li>the size of the VM's memory in bytes.
	 *            </ul>
	 *            <p>
	 *            Afterwards, the "VAST=" defines the virtual appliance's
	 *            storage repository's name. Please note that by the time the
	 *            workflow parsing is started the repository must be already
	 *            registered with the IaaS passed in the first parameter to this
	 *            function.
	 *            <p>
	 *            Finally, the "DATA=" defines the data storage location (a
	 *            repository) for the input data that the this kind of VM will
	 *            download from. This repository could be the same as the VA
	 *            store. If not then the IaaS should have the repository already
	 *            registered.
	 *            </ul>
	 *            </ul>
	 * @return Returns with a workflow description processable by this class.
	 * @throws Exception
	 *             if the parsing went wrong. (describing what went wrong and at
	 *             which line)
	 */
	public static ArrayList<ArrayList<VMUseSpecification>> parseWorkflow(IaaSService iaas, String[] wflines)
			throws Exception {
		ArrayList<ArrayList<VMUseSpecification>> parsedWorkflow = new ArrayList<ArrayList<VMUseSpecification>>();
		ArrayList<VMUseSpecification> currentParallelBlock = null;
		VMUseSpecification currentVMU = null;
		int lineCounter = 0;
		try {
			for (String line : wflines) {
				lineCounter++;
				if (line.startsWith("#")) {
					continue;
				}
				if (line.equals("PSSTART")) {
					parsedWorkflow.add((currentParallelBlock = new ArrayList<VMUseSpecification>()));
					currentVMU = null;
				}
				if (line.startsWith("VMDEF ")) {
					if (currentParallelBlock == null) {
						throw new Exception("Workflow parse error, no parallel section so far! Line: " + line);
					}
					currentVMU = new VMUseSpecification();
					String[] defSegments = line.split(" ");
					for (String defSegment : defSegments) {
						String[] definition = defSegment.split("=");
						if (definition[0].equals("VA")) {
							String[] vaDef = definition[1].split(",");
							currentVMU.va = new VirtualAppliance(vaDef[0], Double.parseDouble(vaDef[1]),
									Long.parseLong(vaDef[2]), false, Long.parseLong(vaDef[3]));
						} else if (definition[0].equals("RC")) {
							String[] rcDef = definition[1].split(",");
							currentVMU.vmReqs = new ConstantConstraints(Double.parseDouble(rcDef[0]),
									Double.parseDouble(rcDef[1]), true, Long.parseLong(rcDef[2]));
						} else if (definition[0].equals("VAST")) {
							currentVMU.vaStore = RepoLookup.lookupInIaaS(iaas, definition[1]);
						} else if (definition[0].equals("DATA")) {
							currentVMU.dataStore = RepoLookup.lookupInIaaS(iaas, definition[1]);
						}
					}
					if (currentVMU.vaStore == null) {
						throw new Exception("No VAstore defined");
					} else if (currentVMU.dataStore == null) {
						throw new Exception("No datastore defined");
					} else if (currentVMU.vmReqs == null) {
						throw new Exception("No VM requirements defined");
					} else if (currentVMU.va == null) {
						throw new Exception("No VA defined");
					}
					currentVMU.vaStore.registerObject(currentVMU.va);
					currentParallelBlock.add(currentVMU);
				}
				if (line.startsWith("VMSEQ ")) {
					if (currentVMU == null) {
						throw new Exception(
								"Workflow parse error, no VM defined so far in the parallel section! " + line);
					}
					ArrayList<Double> seqSpec = new ArrayList<Double>();
					String[] seqDefs = line.split(" ");
					for (String seqDef : seqDefs) {
						if (seqDef.startsWith("N")) {
							seqSpec.add(JobSequence.seqMarker + Double.parseDouble(seqDef.substring(1)));
						}
						if (seqDef.startsWith("C")) {
							seqSpec.add(Double.parseDouble(seqDef.substring(1)));
						}
					}
					double[] seq = new double[seqSpec.size()];
					int seqLoc = 0;
					for (Double seqpiece : seqSpec) {
						seq[seqLoc++] = seqpiece;
					}
					currentVMU.parallelSeqs.add(seq);
				}
			}
		} catch (Exception e) {
			throw new Exception("Error at line " + lineCounter + " cause: " + e.getMessage());
		}
		return parsedWorkflow;
	}

	/**
	 *
	 * @return the actual runtimes of the workflow's jobs
	 * @throws Exception
	 *             if there were problems during the runtime of the workflow
	 */
	public ArrayList<HashMap<VirtualMachine, ArrayList<Long>>> putWorkflowOnLoad() throws Exception {
		Timed.jumpTime(Timed.getNextFire() - 1);
		// Warmup. Let's wait until a few jobs already utilize the IaaS
		while (jp.getDestroycounter() < 50) {
			Timed.simulateUntil(Timed.getFireCount() + 120000);
		}
		executeSingleParallelSection();
		Timed.simulateUntilLastEvent();

		// Evaluation of the simulation results
		if (jp.getDestroycounter() != maxPreparedJobNum) {
			return completionSequence;
		} else {
			// Not a useful result, we did not get enough trace data
			return null;
		}
	}
}
