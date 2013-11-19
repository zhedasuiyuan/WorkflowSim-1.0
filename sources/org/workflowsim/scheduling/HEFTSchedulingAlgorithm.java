/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.scheduling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.WorkflowSimTags;

/**
 * HEFT algorithm. Even though we have the Workflow HEFTPlanner to set 
 * the mapping relationship. But HEFTSchedulingAlgorithm would reassign the task to 
 * the VMs in this phase.
 *
 *Even though we have finished the mapping in the planning phase, we still need to maintain the state of vm
 *and the scheduled list, I guess these information are necessary for the the scheduling engine.
 *
 * @author Zhiming Hu
 * @since WorkflowSim Toolkit 1.0
 * @date Nov 17, 2013
 */
public class HEFTSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    public HEFTSchedulingAlgorithm() {
        super();
    }

    private List hasChecked = new ArrayList<Boolean>();
    
    @Override
    public void run() {
        int size = getCloudletList().size();
        hasChecked.clear();
        Log.printLine("In HEFTScheduling-------------------------"+size);
        for (int t = 0; t < size; t++) {
            hasChecked.add(false);
            Task tmp = (Task) getCloudletList().get(t);
            Log.print(tmp.getCloudletId()+" ");
        }
        Log.printLine();
        for (int i = 0; i < size; i++) {
            int maxIndex = 0;
            Task maxCloudlet = null;
            double maxRank=0.0;
            
            for (int j = 0; j < size; j++) {
            	Task cloudlet = (Task) getCloudletList().get(j);
                boolean chk = (Boolean) (hasChecked.get(j));
                if (!chk) {
                    maxCloudlet = cloudlet;
                    maxIndex = j;
                    maxRank=cloudlet.getRank();
                    break;
                }
            }
            if (maxCloudlet == null) { //There is at least one cloudlet that haven't been checked.
                break;
            }


            /**
             * get the cloudlet with the maximum rank.
             */
            for (int j = 0; j < size; j++) {
                Task cloudlet = (Task) getCloudletList().get(j);
                boolean chk = (Boolean) (hasChecked.get(j));

                if (chk) {
                    continue;
                }

                double rank = cloudlet.getRank();

                if (rank > maxCloudlet.getRank()) {
                    maxCloudlet = cloudlet;
                    maxIndex = j;
                    maxRank=rank;
                }
            }
            hasChecked.set(maxIndex, true);

            int vmSize = getVmList().size();
            CondorVM firstIdleVm = null;//(CondorVM)getVmList().get(0);
            for (int j = 0; j < vmSize; j++) {
                CondorVM vm = (CondorVM) getVmList().get(j);
                if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                    firstIdleVm = vm;
                    break;
                }
            }
            if (firstIdleVm == null) {
                break;
            }
            for (int j = 0; j < vmSize; j++) {
                CondorVM vm = (CondorVM) getVmList().get(j);
                if ((vm.getState() == WorkflowSimTags.VM_STATUS_IDLE)
                        && vm.getCurrentRequestedTotalMips() > firstIdleVm.getCurrentRequestedTotalMips()) {
                    firstIdleVm = vm;

                }
            }
            firstIdleVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
            maxCloudlet.setVmId(firstIdleVm.getId());// Here we set the vmId again, will override the vmId in the planning phase.
            getScheduledList().add(maxCloudlet);//Noted that we have changes the status of the vms but didn't remove the cloudlets.
            Log.printLine("Schedules " + maxCloudlet.getCloudletId() + " with "
                    + maxCloudlet.getCloudletLength() + " to VM " + firstIdleVm.getId()
                    + " with " + firstIdleVm.getCurrentRequestedTotalMips()+" And the rank is "+maxCloudlet.getRank()+"\n");

        }
    }
}
