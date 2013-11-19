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
package org.workflowsim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.scheduling.DataAwareSchedulingAlgorithm;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.scheduling.FCFSSchedulingAlgorithm;
import org.workflowsim.scheduling.HEFTSchedulingAlgorithm;
import org.workflowsim.scheduling.MCTSchedulingAlgorithm;
import org.workflowsim.scheduling.MaxMinSchedulingAlgorithm;
import org.workflowsim.scheduling.MinMinSchedulingAlgorithm;
import org.workflowsim.scheduling.StaticSchedulingAlgorithm;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.SchedulingAlgorithm;

/**
 * WorkflowScheduler represents a algorithm acting on behalf of a user. It hides
 * VM management, as vm creation, sumbission of jobs to this VMs and destruction
 * of VMs.
 * It picks up a scheduling algorithm based on the configuration
 * 
 * Use createVmsInDatacenter(nextDatacenterId) to create the vm 
 * Use sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm); to destory the vm
 * Vm list is the list that we want to create, and the getVmsCreatedList() is the list that contains the vms already created.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class WorkflowScheduler extends DatacenterBroker {

    /**
     * The workflow engine id associated with this workflow algorithm.
     */
    private int workflowEngineId;

    /**
     * Created a new WorkflowScheduler object.
     *
     * @param name name to be associated with this entity (as required by
     * Sim_entity class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public WorkflowScheduler(String name) throws Exception {
        super(name);
    }

    /**
     * Binds this scheduler to a datacenter
     */
    public void bindSchedulerDatacenter(int datacenterId) {
        if (datacenterId <= 0) {
            Log.printLine("Error in data center id");
            return;
        }
        this.datacenterIdsList.add(datacenterId);
    }
    
    /**
     * Sets the workflow engine id
     *
     * @param workflowEngineId the workflow engine id
     */
    public void setWorkflowEngineId(int workflowEngineId) {
        this.workflowEngineId = workflowEngineId;
    }

    /**
     * Process an event
     *
     * @param ev a simEvent obj
     */
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // VM Creation answer to know whether the creation is sucessful.
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev);
                break;
            
            case WorkflowSimTags.CLOUDLET_CHECK:
                processCloudletReturn(ev);
                break;
             // A finished cloudlet returned
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);  //change some staus of the job lists and notify the workflow Engine
                break;
            // if the simulation finishes
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            case CloudSimTags.CLOUDLET_SUBMIT:             //it should handle the submitted cloudlet
                processCloudletSubmit(ev);                 //Nearly did nothing and then cloudlet update
                break;                                    //setup the getCloudletList()

            case WorkflowSimTags.CLOUDLET_UPDATE:
                processCloudletUpdate(ev);     //Get the scheduler and set the task list and vm list.
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    /**
     * Switch between multiple schedulers. Based on algorithm.method
     *
     * @param name the SchedulingAlgorithm name
     * @return the algorithm that extends BaseSchedulingAlgorithm
     */
    private BaseSchedulingAlgorithm getScheduler(SchedulingAlgorithm name) {
        BaseSchedulingAlgorithm algorithm = null;

        // choose which algorithm to use. Make sure you have add related enum in
        //Parameters.java
        switch (name) {
            //by default it is FCFS_SCH
            case FCFS_SCH:
                algorithm = new FCFSSchedulingAlgorithm();
                break;
            case MINMIN_SCH:
                algorithm = new MinMinSchedulingAlgorithm();
                break;
            case MAXMIN_SCH:
                algorithm = new MaxMinSchedulingAlgorithm();
                break;
            case MCT_SCH:
                algorithm = new MCTSchedulingAlgorithm();
                break;
            case DATA_SCH:
                algorithm = new DataAwareSchedulingAlgorithm();
                break;
            case STATIC_SCH:
                algorithm = new StaticSchedulingAlgorithm();
//                algorithm= new HEFTSchedulingAlgorithm();
                break;
            case HEFT_SCH:
            	algorithm= new HEFTSchedulingAlgorithm();
            	break;
            default:
                algorithm = new StaticSchedulingAlgorithm();
//                algorithm= new HEFTSchedulingAlgorithm();
                break;

        }

        return algorithm;
    }

    /**
     * Process the ack received due to a request for VM creation.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    @Override
    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            /**
             * Fix a bug of cloudsim Don't add a null to getVmsCreatedList()
             * June 15, 2013
             */
            if (VmList.getById(getVmList(), vmId) != null) {
                getVmsCreatedList().add(VmList.getById(getVmList(), vmId));

                Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
                        + " has been created in Datacenter #" + datacenterId + ", Host #"
                        + VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
            }
        } else {
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
                    + " failed in Datacenter #" + datacenterId);
        }

        incrementVmsAcks();

        // all the requested VMs have been created
        if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
            submitCloudlets();
        } else {
            // all the acks received, but some VMs were not created
            if (getVmsRequested() == getVmsAcks()) {
                // find id of the next datacenter that has not been tried
                for (int nextDatacenterId : getDatacenterIdsList()) {
                    if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
                        createVmsInDatacenter(nextDatacenterId);
                        return;
                    }
                }

                // all datacenters already queried
                if (getVmsCreatedList().size() > 0) { // if some vm were created
                    submitCloudlets();
                } else { // no vms created. abort
                    Log.printLine(CloudSim.clock() + ": " + getName()
                            + ": none of the required VMs could be created. Aborting");
                    finishExecution();
                }
            }
        }
    }

    
    /**
     * Update a cloudlet (job) why should we update?
     *
     * @param ev a simEvent object
     */
    protected void processCloudletUpdate(SimEvent ev) {

        BaseSchedulingAlgorithm scheduler = getScheduler(Parameters.getSchedulingAlgorithm());
        scheduler.setCloudletList(getCloudletList());
               
        //create the number of vm based on the number of cloudlets.
        int tmpNum=getCloudletList().size()-getVmsCreatedList().size();
        if(tmpNum>0){
        	createVms(getDatacenterIdsList().get(0),0.0,WorkflowSimTags.TINY,tmpNum);
        	Log.printLine("The value of tmpNum is "+tmpNum);
        }else{
//        	destoryVm();
        }
        scheduler.setVmList(getVmsCreatedList());   //Remember the VMList.
        try {
            scheduler.run();
        } catch (Exception e) {
            Log.printLine("Error in configuring scheduler_method");
            e.printStackTrace();
        }

        List scheduledList = scheduler.getScheduledList();
        for (Iterator it = scheduledList.iterator(); it.hasNext();) {
            Cloudlet cloudlet = (Cloudlet) it.next();
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if(Parameters.getOverheadParams().getQueueDelay()!=null){
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);//Go to the certain datacenter

        }
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();

    }

    /**
     * Process a cloudlet (job) return event.
     * CloudletReturn from the datacenter.
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    @Override
    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();

        Job job = (Job) cloudlet;

        /**
         * Generate a failure if failure rate is not zeros.
         */
        FailureGenerator.generate(job);

        getCloudletReceivedList().add(cloudlet);// received from the data center??
        getCloudletSubmittedList().remove(cloudlet); // submitted to the data center??

        CondorVM vm = (CondorVM) getVmsCreatedList().get(cloudlet.getVmId());
        //so that this resource is released
        vm.setState(WorkflowSimTags.VM_STATUS_IDLE);

        double delay = 0.0;
        if(Parameters.getOverheadParams().getPostDelay()!=null){
            delay = Parameters.getOverheadParams().getPostDelay(job);
        }
        schedule(this.workflowEngineId, delay, CloudSimTags.CLOUDLET_RETURN, cloudlet);//determine new available cloudlets. Check the parents Nodes.

        cloudletsSubmitted--;
 
        //not really update right now, should wait 1 s until many jobs have returned
        //Here if we update right now, maybe we will get nothing from the scheduling engine.
//TODO  why we need to start the update process here? Still cannot understand why we need to call this method.
        schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE); //Running the scheduling algorithm

    }

    /**
     * process cloudlet (job) check (not supported yet)
     *
     * @param ev a simEvent object
     */
    protected void processCloudletCheck(SimEvent ev) {
        /**
         * Left for future use.
         */
    }

    /**
     * Start this entity (WorkflowScheduler)
     */
    @Override
    public void startEntity() {
        Log.printLine(getName() + " is starting...");
        // this resource should register to regional GIS.
        // However, if not specified, then register to system GIS (the
        // default CloudInformationService) entity.
        //int gisID = CloudSim.getEntityId(regionalCisName);
        int gisID = -1;
        if (gisID == -1) {
            gisID = CloudSim.getCloudInfoServiceEntityId();
        }

        // send the registration to GIS
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
        //the below sentence is executed in workflow engine
        //schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);

    }

    /**
     * Terminate this entity (WorkflowScheduler)
     */
    @Override
    public void shutdownEntity() {

        clearDatacenters();
        Log.printLine(getName() + " is shutting down...");

    }

    /**
     * Submit cloudlets (jobs) to the created VMs. Scheduling is here
     *
     * @pre $none
     * @post $none
     */
    @Override
    protected void submitCloudlets() {

        sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, null);
    }
    /**
     * A trick here. Assure that we just submit it once
     */
    private boolean processCloudletSubmitHasShown = false;

    /**
     * Submits cloudlet (job) list
     *
     * @param ev a simEvent object
     */
    protected void processCloudletSubmit(SimEvent ev) {
        List<Job> list = (List) ev.getData();
        getCloudletList().addAll(list);

        sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
        if (!processCloudletSubmitHasShown) {
            //Log.printLine("Pay Attention that the actual vm size is " + getVmsCreatedList().size());
            processCloudletSubmitHasShown = true;
        }
    }

    /**
     * Process a request for the characteristics of a PowerDatacenter.
     *    I don't know why we need it.
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    @Override
    protected void processResourceCharacteristicsRequest(SimEvent ev) {

        setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());

        Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
                + getDatacenterIdsList().size() + " resource(s)");

        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
    }
    
    /**
     * Destroy the Vm with certain delay.
     * @param vmId
     * @param delay
     */
    protected void destoryVm(int vmId, double delay) {			
    	Log.printLine(CloudSim.clock() +delay+": " + getName() + ": Destroying VM #" + vmId+" in workflowScheduler");
		send(getVmsToDatacentersMap().get(vmId), delay,CloudSimTags.VM_DESTROY, VmList.getById(getVmsCreatedList(), vmId));
		getVmsCreatedList().remove(VmList.getById(getVmsCreatedList(), vmId));
		vmsDestroyed++;//We delete one vm		
	}

    
    /**
     * Destroy the vm as you need right now.
     * @param vmId
     */
    protected void destoryVm(int vmId) {			
    	destoryVm(vmId,0.0);
	}
    /**
     * wfEngine.getSchedulerId(0) is the userID. That is the workflowscheduler ID.
     * @param userId
     * @param vms
     * @return
     */
    protected List<CondorVM> createVM(int type,int vms) {

        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<CondorVM> list = new LinkedList<CondorVM>();
      //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name
        
        if(type==WorkflowSimTags.TINY){
        	
        }else if(type==WorkflowSimTags.SMALL){
        	ram*=4;
        }else if(type==WorkflowSimTags.MEDIUM){
        	ram*=8;
        	pesNumber*=2;
        }else if(type==WorkflowSimTags.LARGE){
        	ram*=16;
        	pesNumber*=4;
        }else if(type==WorkflowSimTags.XLARGE){
        	ram*=32;
        	pesNumber*=8;
        }
        

        //create VMs
        CondorVM[] vm = new CondorVM[vms];

        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(getVmList().size()+i, this.getId(), mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }

        return list;
    }

    /**
	 * Create the virtual machines in a datacenter.
	 * 
	 * @param datacenterId Id of the chosen PowerDatacenter
	 * @pre $none
	 * @post $none
	 */
	protected void createVms(int datacenterId,double delay,int type,int vms) { //The ID of the first datacenter is 0
		// send as much vms as possible for this datacenter before trying the next one
		int requestedVms = 0;
		String datacenterName =CloudSim.getEntityName(datacenterId);
		List<CondorVM> newVMList=createVM(type,vms);
		for (Vm vm : newVMList) {
			if (!getVmsToDatacentersMap().containsKey(vm.getId())) {
				Log.printLine(CloudSim.clock() +delay+ ": " + getName() + ": Trying to Create VM #" + vm.getId()
						+ " in " + datacenterName);
				send(datacenterId,delay, CloudSimTags.VM_CREATE_ACK, vm);//ACK means that we need the acknowledgment.
				requestedVms++;
			}
		}
		getVmList().addAll(newVMList);//Add the new list to the vmList.
		getDatacenterRequestedIdsList().add(datacenterId);
		setVmsRequested(requestedVms+getVmsRequested());
//		setVmsAcks(getVmsAcks());
	}
}
