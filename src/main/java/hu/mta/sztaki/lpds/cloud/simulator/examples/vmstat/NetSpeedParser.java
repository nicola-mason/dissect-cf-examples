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

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Calendar;
import java.util.List;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

/**
 * This file works in collaboration with VMStatParser and adds networking
 * related information to each job the previous produced. The file read by this
 * reader can parse two formats. In both cases, it expects a new line of data to
 * be listed for each second (similarly how VMStatParser works).
 * 
 * The first (compact) line format:
 * <ul>
 * <li>Follows a CSV form (separated by commas)
 * <li>The first column contains the TX values (kBs of data sent in a given
 * second)
 * <li>The second column contains the RX values (kBs of data received in a given
 * second)
 * </ul>
 * 
 * The second (verbose) line format:
 * <ul>
 * <li>Items in the line are separated by space</li>
 * <li>List of items
 * <ol>
 * <li>The text "TX"</li>
 * <li>The NIC's name (e.g., "eth0")*</li>
 * <li>The amount of data sent in the corresponding second</li>
 * <li>OPTIONAL! Units for the previous item, if this is missing then it is
 * assumed to be kB/s*</li>
 * <li>The text "RX"</li>
 * <li>The NIC's name (e.g., "eth0")*</li>
 * <li>The amount of data received in the corresponding second</li>
 * <li>OPTIONAL! Units for the previous item, if this is missing then it is
 * assumed to be kB/s*</li>
 * </ol>
 * Note that data marked with * is not processed by the loader.</li>
 * </ul>
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2017"
 */
public class NetSpeedParser {

	public static void populateNetData(ComplexJob startingJob, String dir, List<Job> jobs) {
		try {
			String baseId = startingJob.getId().split("-")[0];
			File nwdata = new File(dir, baseId.replace("vmstat", "netspeed"));
			System.err.println("NetSpeed trace file reader starts for: " + nwdata.getAbsolutePath() + " at "
					+ Calendar.getInstance().getTime());
			baseId = baseId + "-";
			RandomAccessFile raf = new RandomAccessFile(nwdata, "r");
			String line;
			int seq = 0;
			boolean unknownFormat = true;
			boolean verboseFormat = false;
			while ((line = raf.readLine()) != null) {
				if (unknownFormat) {
					verboseFormat = line.startsWith("TX");
					unknownFormat = false;
				}
				int tx;
				int rx;
				String[] rawdata;
				if (verboseFormat) {
					rawdata = line.split("\\s+");
					tx = Integer.parseInt(rawdata[2]);
					try {
						// No kb/s present
						rx = Integer.parseInt(rawdata[5]);
					} catch (NumberFormatException e) {
						// Kb/s present
						rx = Integer.parseInt(rawdata[6]);
					}
				} else {
					rawdata = line.split(",");
					tx = Integer.parseInt(rawdata[0]);
					rx = Integer.parseInt(rawdata[1]);
				}
				for (Job j : jobs) {
					if (j.getId().equals(baseId + seq)) {
						ComplexJob pj = (ComplexJob) j;
						pj.rx = rx;
						pj.tx = tx;
						break;
					}
				}
				seq++;
			}
			raf.close();
			System.err.println("NetSpeed trace file reader stops for: " + nwdata.getAbsolutePath() + " at "
					+ Calendar.getInstance().getTime());
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
