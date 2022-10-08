package src.src.broker;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.VmList;


public class SJF_DatacenterBroker extends DatacenterBroker {

    public SJF_DatacenterBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void submitCloudlets() {
        int vmIndex = 0;
        
        ArrayList<Cloudlet> sortedList = new ArrayList<>();
        ArrayList<Cloudlet> tempList = new ArrayList<>();
        
        for (Cloudlet cloudlet : getCloudletList()) {
            tempList.add(cloudlet);
        }
        
        int n = tempList.size();
        
        for (int i = 0; i < n; i++) {
            Cloudlet smallest = tempList.get(0);
            
            for(Cloudlet check : tempList) {
                if (smallest.getCloudletLength() > check.getCloudletLength()) {
                    smallest = check;
                }
            }
            sortedList.add(smallest);
            tempList.remove(smallest);
        }
        
        List<Cloudlet> successfullySubmitted = new ArrayList<Cloudlet>();
        for (Cloudlet cloudlet : sortedList) {
            Vm vm;
            
            if (cloudlet.getVmId() == -1) {
                vm = getVmsCreatedList().get(vmIndex);
            } else { 
                vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
                if (vm == null) { 
                    if (!Log.isDisabled()) {
                        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Postponing execution of cloudlet ",
                                cloudlet.getCloudletId(), ": bount VM not available");
                    }
                    continue;
                }
            }

            if (!Log.isDisabled()) {
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Sending cloudlet ",
                        cloudlet.getCloudletId(), " to VM #", vm.getId());
            }

            cloudlet.setVmId(vm.getId());
            sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
            cloudletsSubmitted++;
            vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
            getCloudletSubmittedList().add(cloudlet);
            successfullySubmitted.add(cloudlet);
        }

        
        getCloudletList().removeAll(successfullySubmitted);
    }

}