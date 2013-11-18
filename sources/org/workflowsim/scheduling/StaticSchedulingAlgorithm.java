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

import java.util.HashMap;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.WorkflowSimTags;

/**
 * Static algorithm. Do not schedule at all and reply on Workflow Planner to set 
 * the mapping relationship. But StaticSchedulingAlgorithm would check whether a job has been
 * assigned a VM in this stage (in case your implementation of planning algorithm 
 * forgets it)
 *
 *Even though we have finished the mapping in the planning phase, we still need to maintain the state of vm
 *and the scheduled list, I guess these information are necessary for the the scheduling engine.
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Jun 17, 2013
 */
public class StaticSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    public StaticSchedulingAlgorithm() {
        super();
    }

    @Override
    public void run() throws Exception{
        
        Map mId2Vm = new HashMap<Integer, CondorVM> ();
        
        for(int i = 0; i < getVmList().size();i++ ) {
            CondorVM vm = (CondorVM)getVmList().get(i);
            if(vm!=null){
                mId2Vm.put(vm.getId(), vm);
//                Log.printLine("vm.getId "+vm.getId());  vmId start from 0
            }
        }
        
        int size = getCloudletList().size();

        Log.printLine("The size of cloudlet is "+size);
        for(int i = 0; i < size; i++) {
            Task cloudlet = (Task) getCloudletList().get(i);
            /**
             * Make sure cloudlet is matched to a VM. It should be done in the
             * Workflow Planner. If not, throws an exception because StaticSchedulingAlgorithm
             * itself does not do the mapping. 
             */

            if(cloudlet.getVmId() < 0 || ! mId2Vm.containsKey(cloudlet.getVmId())){// it means something was wrong.
//                throw(new Exception("Cloudlet " + cloudlet.getCloudletId() + " is not matched."
//                        + "Please configure scheduler_method in your config file"));
                Log.printLine("Cloudlet " + cloudlet.getCloudletId() + " is not matched."
                        + "It is possible a stage-in job");
                cloudlet.setVmId(0);
                
            }
            Log.printLine("Try to schedule "+cloudlet.getCloudletId()+" with "+cloudlet.getCloudletLength()+" to VM "+cloudlet.getVmId());
            CondorVM vm = (CondorVM)mId2Vm.get(cloudlet.getVmId());
            if(vm.getState() == WorkflowSimTags.VM_STATUS_IDLE){   
               vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
               getScheduledList().add(cloudlet);
               Log.printLine("Schedules " + cloudlet.getCloudletId() + " with "
                    + cloudlet.getCloudletLength() + " to VM " + cloudlet.getVmId()+" rank "+cloudlet.getRank());
            }

         }
        Log.printLine("------------------------------------------------------------------------------\n\n");




    }
}
