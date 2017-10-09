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
 *  (C) Copyright 2013-15, Gabor Kecskemeti (gkecskem@dps.uibk.ac.at,
 *   									  kecskemeti.gabor@sztaki.mta.hu)
 */
package hu.mta.sztaki.lpds.cloud.simulator.examples;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.energy.powermodelling.PowerState;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.StorageObject;
import hu.mta.sztaki.lpds.cloud.simulator.util.PowerTransitionGenerator;
import hu.mta.sztaki.lpds.cloud.simulator.util.SeedSyncer;

import java.util.Calendar;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * A demonstrator for the simulator's network transfer operations through the
 * repository interfaces.
 * 
 * This class was used to provide the input for Figure 4 in the article:<br>
 * <i>Gabor Kecskemeti:
 * "HALOZATI FORGALOMKEZELES SZAMITASI FELHO SZIMULATOROKBAN". In
 * Multidiszciplinaris tudomanyok Vol 4 Issue 1 pp 147-158. Sept 2014.</i>
 * 
 * @author 
 *         "Gabor Kecskemeti, Distributed and Parallel Systems Group, University of Innsbruck (c) 2013"
 */
public class TransferDemo extends ConsumptionEventAdapter {

	String id;
	int fr, to;

	/**
	 * Upon instantiation, this constructor starts a network transfer and
	 * registers the new object to be the event handler for the transfer related
	 * issues (e.g. completion cancellation)
	 */
	public TransferDemo(String id, int from, int to) throws NetworkException {
		this.id = id;
		this.fr = from;
		this.to = to;
		repos[from].requestContentDelivery(id, repos[to], this);
	}

	/**
	 * When the network transfer initiated by the constructor gets completed,
	 * this function is called. The function records the event in a static
	 * counter, allowing the main event loop to check if all its required
	 * transfers are done.
	 */
	@Override
	public void conComplete() {
		if (verbose) {
			System.out.println("\tTrComp(" + id + " ~> Source: " + fr
					+ " -> Target: " + to + ")@" + Timed.getFireCount());
		}
		completeCount++;
	}

	public static int completeCount = 0;
	public static Repository[] repos;
	public static boolean verbose;

	/**
	 * This command line program simulates a set of network transfers amongst
	 * several networked entities. The kinds of transfers and the networked
	 * entities are defined in the command line arguments.
	 * 
	 * @param args
	 *            <ol>
	 *            <li>[[Number of transfers]/[Number of network nodes]]
	 *            <ul>
	 *            <li>Number of transfers = how many transfers should the
	 *            program simulate. This parameter must be specified for all use
	 *            cases of the program. The transfers are generated randomly
	 *            between the network nodes.
	 *            <li>The separator between the transfers and the network nodes
	 *            is '/'. The separator is optional, only necessary if network
	 *            nodes are also specified.
	 *            <li>Number of network nodes = how many network nodes should be
	 *            present during the simulation. This parameter is optional. If
	 *            not specified the default value of 3 is assumed.<br>
	 *            <i>Note:</i> the network node count can start with the
	 *            modifier '@' which ensures circular transfers: e.g., in case
	 *            of 3 network nodes and 3 transfers the following setup will be
	 *            created: nn1 =tr1=> nn2 =tr2=> nn3 =tr3=> nn1. Where nn1 is
	 *            the first generated network node.
	 *            </ul>
	 *            <li>[[+]The size of the transfer]<br>
	 *            This parameter is optional.
	 *            <ul>
	 *            <li>The size of the transfer in bytes. This is an optional
	 *            parameter, if not specified the size will be picked randomly
	 *            between 500MB-20GB.
	 *            <li>If a '+' sign is present in front of the transfer size,
	 *            then the size will not be constant but will be randomized
	 *            between the 2x the given size and 1 byte.
	 *            <li>If a size is given but no '+' is present in front of the
	 *            size, then there will be no size randomization applied, all
	 *            transfers will be generated with uniform size.
	 *            </ul>
	 *            <li>[[wait]/[verbose]]<br>
	 *            This parameter is optional.
	 *            <ul>
	 *            <li>If the third parameter contains the term 'wait' then after
	 *            the JVM launches the program, it immediately starts a 50
	 *            second sleep allowing a profiler to be attached that will not
	 *            evaluate JVM related activities. This delay is especially
	 *            useful for simulations with sub second runtimes as in those
	 *            cases the JVM's activities might bias the profiler outputs.
	 *            <li>If the third parameter contains the term 'verbose' then
	 *            all kinds of useful logging information is printed on the
	 *            program's standard output allowing the simple evaluation of
	 *            the simulation.
	 *            </ul>
	 *            </ol>
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String[] trdetails = args[0].split("/");
		int trNum = Integer.parseInt(trdetails[0]);
		int repoCount = 3; // the default repository count
		boolean fixedRepoorder = false;
		if (trdetails.length > 1) {
			fixedRepoorder = trdetails[1].startsWith("@");
			repoCount = Integer.parseInt(fixedRepoorder ? trdetails[1]
					.substring(1) : trdetails[1]);
		}
		long fixedSize = -1; // the default transfer size comes from the
								// StorageObject class.
		boolean varyVASize = false;
		if (args.length > 2) {
			if (args[2].contains("wait")) {
				Thread.sleep(50000);
			}
			verbose = args[2].contains("verbose");
		}
		if (args.length > 1) {
			varyVASize = args[1].startsWith("+");
			fixedSize = Long.parseLong(varyVASize ? args[1].substring(1)
					: args[1]);
			if (fixedSize < 0 || fixedSize * trNum < 0) {
				System.err.println("ERROR: Too big fixed size!");
				System.exit(1);
			}
		}
		if (verbose) {
			System.out.println("Creating the repositories");
		}
		final long bandwidth = 111111111; // bytes per ms
		HashMap<String, Integer> latencyMap = new HashMap<String, Integer>();
		for (int i = 0; i < repoCount; i++) {
			latencyMap.put("Repo" + i, 6); // between all networked entities we
											// will have a 6 ms latency
		}
		final EnumMap<PowerTransitionGenerator.PowerStateKind, Map<String, PowerState>> transitions = PowerTransitionGenerator
				.generateTransitions(20, 296, 493, 50, 108);

		long msstart = Calendar.getInstance().getTimeInMillis();
		// We will represent networked entities with repositories.
		repos = new Repository[repoCount];
		for (int i = 0; i < repoCount; i++) {
			// Each repository is capable of storing 111PBs of data.
			repos[i] = new Repository(111111111111111111L, "Repo" + i,
					bandwidth, bandwidth, bandwidth, latencyMap,transitions
					.get(PowerTransitionGenerator.PowerStateKind.storage),transitions
					.get(PowerTransitionGenerator.PowerStateKind.network));
			repos[i].setState(NetworkNode.State.RUNNING);
		}
		System.out.print((verbose ? "Repositories created. Timing (ms):  "
				: "R (ms): ")
				+ (Calendar.getInstance().getTimeInMillis() - msstart)
				+ (verbose ? "\n" : ""));
		msstart = Calendar.getInstance().getTimeInMillis();
		// The transferred items will be stored in the repositories created
		// above
		StorageObject[] sos = new StorageObject[trNum];
		/**
		 * This class allows an easy pairing of the repositories based on the
		 * repos array.
		 * 
		 * The pairing is used to determine from which repository one should
		 * initiate the transfer and which repository will be the target of the
		 * transfer.
		 */
		class Pair {
			int fromIndex;
			int toIndex;
		}
		Pair[] transferDirections = new Pair[trNum];
		long totSize = 0;
		if (verbose) {
			System.out.println("Generating Storage objects");
		}
		// The main loop for generating the transferrable items and the way they
		// are going to be transferred between repos
		for (int i = 0; i < trNum; i++) {
			if (fixedSize > 0) {
				sos[i] = new StorageObject("Test" + i, fixedSize, varyVASize);
			} else {
				sos[i] = new StorageObject("Test" + i);
			}
			totSize += sos[i].size;
			transferDirections[i] = new Pair();
			// Determining the source and target for the transfer
			if (fixedRepoorder) {
				transferDirections[i].fromIndex = i % repoCount;
				transferDirections[i].toIndex = (i + 1) % repoCount;
			} else {
				transferDirections[i].fromIndex = SeedSyncer.centralRnd
						.nextInt(repoCount);
				while ((transferDirections[i].toIndex = SeedSyncer.centralRnd
						.nextInt(repoCount)) == transferDirections[i].fromIndex) {
				}
			}
			if (verbose) {
				System.out.println("\tSO: " + sos[i].id + " F:"
						+ transferDirections[i].fromIndex + ": T:"
						+ transferDirections[i].toIndex + ": S:" + sos[i].size
						+ ":");
			}
			// Ensuring that the source has the storage object to be transferred
			// to the target later on in the simulation
			repos[transferDirections[i].fromIndex].registerObject(sos[i]);
		}
		System.out
				.println((verbose ? "SO Generation Complete, timing - in ms: "
						: ", SO (ms): ")
						+ (Calendar.getInstance().getTimeInMillis() - msstart));
		if (verbose) {
			System.out.println("Preparing "
					+ (fixedRepoorder ? "unidirectional/circular" : "random")
					+ " transfers");
		}
		msstart = Calendar.getInstance().getTimeInMillis();
		// Initiating the transfers, all transfers will start at the same time
		// (0th time instance).
		for (int i = 0; i < trNum; i++) {
			new TransferDemo(sos[i].id, transferDirections[i].fromIndex,
					transferDirections[i].toIndex);
		}
		if (verbose) {
			System.out.println("Starting the main event loop");
		}
		// Doing the simulation iteratively until the completed transfer count
		// reaches the total transfers generated.
		while (completeCount != trNum) {
			// "Manually" advancing the simulation time
			Timed.jumpTime(Long.MAX_VALUE);
			Timed.fire();
			if (verbose) {
				System.out.print("!");
			}
		}
		// Simulation is done, let's print some useful data
		if (verbose) {
			System.out
					.println("\nMain event loop finished. Run details (starting with row headers):");
			System.out
					.println("Runtime since tr preparation, Simulated time, Transferred bytes");
		}
		System.out.println((Calendar.getInstance().getTimeInMillis() - msstart)
				+ "," + Timed.getFireCount() + "," + totSize);
	}
}
