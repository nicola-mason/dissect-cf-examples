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

import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;

public class VMUseSpecification {

    public VirtualAppliance va=null;
    public Repository vaStore=null;
    public ConstantConstraints vmReqs=null;
    public Repository dataStore=null;
    public ArrayList<double[]> parallelSeqs=new ArrayList<double[]>();
    public ArrayList<VirtualMachine> seqExecutors=new ArrayList<VirtualMachine>();
}
