package src.src;

import src.src.broker.MappingBroker;
import src.src.broker.SJF_DatacenterBroker;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class Simulation {

    private int cloudletSchedulerType;
    private int brokerType;
    private int numOfCloudlets;
    private int numOfVMs;
    private Random rng;
    private boolean silent;
    private int fitnessType;

    private final double[] VM_MIPS_POWERS;
    private final int[] CLOUDLET_LENGTHS;
    private final double[][] ETC_MATRIX;

    public Simulation(int cloudletSchedulerType, int numOfCloudlets, int numOfVMs, int brokerType, int fitnessType,
            Random rng, boolean silent, int highHeterogeneity) {
        this.cloudletSchedulerType = cloudletSchedulerType;
        this.numOfCloudlets = numOfCloudlets;
        this.numOfVMs = numOfVMs;
        this.brokerType = brokerType;
        this.fitnessType = fitnessType;
        this.rng = rng;
        this.silent = silent;

        VM_MIPS_POWERS = new double[numOfVMs];
        for (int i = 0; i < numOfVMs; i++) {
            if (highHeterogeneity == 1) {
                VM_MIPS_POWERS[i] = rng.nextInt(901) + 100;
            } else {
                VM_MIPS_POWERS[i] = rng.nextInt(101) + 500;
            }
        }

        CLOUDLET_LENGTHS = new int[numOfCloudlets];
        for (int i = 0; i < this.numOfCloudlets; i++) {
            CLOUDLET_LENGTHS[i] = 1000 + rng.nextInt(1001);
        }

        ETC_MATRIX = new double[numOfCloudlets][numOfVMs];
        for (int i = 0; i < numOfCloudlets; i++) {
            for (int j = 0; j < numOfVMs; j++) {
                ETC_MATRIX[i][j] = (double) CLOUDLET_LENGTHS[i] / VM_MIPS_POWERS[j];
            }
        }
    }

    private List<Cloudlet> cloudletList;

    private List<Vm> vmlist;

    private List<Vm> createVM(int userId, int numOfVMs) {
        LinkedList<Vm> list = new LinkedList<Vm>();

        long size = 1024;
        int ram = 1024;
        double mips;
        long bw = 1024;
        int pesNumber = 1;
        String vmm = "Xen";

        Vm[] vm = new Vm[numOfVMs];

        for (int i = 0; i < numOfVMs; i++) {

            CloudletScheduler cs = null;
            switch (cloudletSchedulerType) {
                case 0:
                    cs = new CloudletSchedulerSpaceShared();
                    break;
                case 1:
                    cs = new CloudletSchedulerTimeShared();
                    break;
                default:
                    break;
            }

            mips = VM_MIPS_POWERS[i];
            Log.printLine("VM " + i + " MIPS: " + mips);
            vm[i] = new Vm(i, userId, mips, pesNumber, ram, bw, size, vmm, cs);

            list.add(vm[i]);
        }

        return list;
    }

    private List<Cloudlet> createCloudlet(int userId, int numOfCloudlets) {
        LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

        long length;
        long fileSize = 300;
        long outputSize = 300;
        int pesNumber = 1;
        UtilizationModel utilizationModel = new UtilizationModelFull();

        Cloudlet[] cloudlet = new Cloudlet[numOfCloudlets];

        for (int i = 0; i < numOfCloudlets; i++) {
            length = CLOUDLET_LENGTHS[i];
            Log.printLine("Cloudlet " + i + " length: " + length);
            cloudlet[i] = new Cloudlet(i, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel,
                    utilizationModel);
            cloudlet[i].setUserId(userId);
            list.add(cloudlet[i]);
        }

        return list;
    }

    private Datacenter createDatacenter(String name) {
        List<Host> hostList = new ArrayList<Host>();

        List<Pe> peList1 = new ArrayList<Pe>();

        int mips = 1000;

        for (int i = 0; i < numOfVMs; i++) {
            peList1.add(new Pe(i, new PeProvisionerSimple(mips)));
        }

        int hostId = 0;
        int ram = 100 * 1024;
        long storage = 100 * 1024;
        int bw = 100 * 1024;

        hostList.add(
                new Host(
                        hostId,
                        new RamProvisionerSimple(ram),
                        new BwProvisionerSimple(bw),
                        storage,
                        peList1,
                        new VmSchedulerTimeSharedOverSubscription(peList1)));

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.1;
        double costPerBw = 0.1;
        LinkedList<Storage> storageList = new LinkedList<Storage>();

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        Datacenter datacenter = null;
        try {
            datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

    private DatacenterBroker createBroker() {

        DatacenterBroker broker = null;
        try {
            switch (brokerType) {
                case 0:
                    broker = new MappingBroker("Broker");
                    break;
                case 1:
                    broker = new SJF_DatacenterBroker("Broker");
                    break;
                case 2:
                    broker = new DatacenterBroker("Broker");
                    break;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return broker;
    }

    public double runSimulation(int[] mapping) {
        if (silent) {
            Log.disable();
        }
        Log.printLine("Simulation Starts...");

        double fitness = -1;

        try {
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;

            CloudSim.init(num_user, calendar, trace_flag);

            @SuppressWarnings("unused")
            Datacenter datacenter0 = createDatacenter("Datacenter_0");
            @SuppressWarnings("unused")

            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();

            vmlist = createVM(brokerId, numOfVMs);
            cloudletList = createCloudlet(brokerId, numOfCloudlets);

            broker.submitVmList(vmlist);
            broker.submitCloudletList(cloudletList);

            if (brokerType == 0) {
                ((MappingBroker) broker).setMapping(mapping);
            }

            CloudSim.startSimulation();

            List<Cloudlet> newList = broker.getCloudletReceivedList();

            CloudSim.stopSimulation();

            printCloudletList(newList);

            switch (fitnessType) {
                case 0:
                    fitness = calculateActualMakespan(newList);
                    break;
                case 1:
                    fitness = calculateActualResourceUtilization(newList);
                    fitness = 1.0 / fitness;
                    break;
                default:
                    fitness = -1;
            }

            Log.printLine("Simulation Ends!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }

        return fitness;
    }

    private static void printCloudletList(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent
                + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            Log.print(indent + cloudlet.getCloudletId() + indent + indent);

            if (cloudlet.getStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");

                Log.printLine(indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId()
                        + indent + indent + indent + dft.format(cloudlet.getActualCPUTime())
                        + indent + indent + dft.format(cloudlet.getExecStartTime()) + indent + indent + indent
                        + dft.format(cloudlet.getFinishTime()));
            }
        }

    }

    public double calculateActualMakespan(List<Cloudlet> list) {
        double makespan = 0;
        double minStartTime = Double.MAX_VALUE;
        double maxFinishTime = 0;

        for (Cloudlet cloudlet : cloudletList) {

            if (cloudlet.getExecStartTime() < minStartTime) {
                minStartTime = cloudlet.getExecStartTime();
            }

            if (cloudlet.getFinishTime() > maxFinishTime) {
                maxFinishTime = cloudlet.getFinishTime();
            }
        }

        makespan = maxFinishTime - minStartTime;
        return makespan;
    }

    public double calculateActualResourceUtilization(List<Cloudlet> list) {
        double[] ST = new double[numOfVMs];
        double[] CT = new double[numOfVMs];
        for (int i = 0; i < numOfVMs; i++) {
            ST[i] = Double.POSITIVE_INFINITY;
            CT[i] = Double.NEGATIVE_INFINITY;
        }

        for (Cloudlet cloudlet : cloudletList) {
            int vmID = cloudlet.getVmId();
            if (ST[vmID] > cloudlet.getExecStartTime()) {
                ST[vmID] = cloudlet.getExecStartTime();
            }

            if (CT[vmID] < cloudlet.getFinishTime()) {
                CT[vmID] = cloudlet.getFinishTime();
            }
        }

        double utilization = 0;

        for (int i = 0; i < numOfVMs; i++) {
            utilization += CT[i] - ST[i];
        }

        return utilization / (calculateActualMakespan(list) * numOfVMs);

    }

    public double[] getVM_MIPS_POWERS() {
        return VM_MIPS_POWERS;
    }

    public int[] getCLOUDLET_LENGTHS() {
        return CLOUDLET_LENGTHS;
    }

    public double[][] getETC_MATRIX() {
        return ETC_MATRIX;
    }

    public int getNumOfCloudlets() {
        return numOfCloudlets;
    }

    public int getNumOfVMs() {
        return numOfVMs;
    }

    public Random getRng() {
        return rng;
    }

    public double calculatePredictedMakespan(int[] mapping) {
        double[] finishTimes = new double[numOfVMs];

        for (int i = 0; i < numOfCloudlets; i++) {
            if (mapping[i] == numOfVMs) {
                mapping[i] = (int) numOfVMs - 1;
            }
            finishTimes[mapping[i]] += ETC_MATRIX[i][mapping[i]];
        }

        double maxFinishTime = 0;
        for (int i = 0; i < numOfVMs; i++) {
            if (finishTimes[i] > maxFinishTime) {
                maxFinishTime = finishTimes[i];
            }
        }

        return maxFinishTime;
    }

    public double calculatePredictedResourceUtilization(int[] mapping) {
        double[] finishTimes = new double[numOfVMs];

        for (int i = 0; i < numOfCloudlets; i++) {
            if (mapping[i] == numOfVMs) {
                mapping[i] = (int) numOfVMs - 1;
            }
            finishTimes[mapping[i]] += ETC_MATRIX[i][mapping[i]];
        }

        double totalFinishTime = 0;
        for (int i = 0; i < numOfVMs; i++) {
            totalFinishTime += finishTimes[i];
        }

        return totalFinishTime / (calculatePredictedMakespan(mapping) * numOfVMs);
    }

    public double predictFitnessValue(int[] mapping) {
        switch (fitnessType) {
            case 0:
                return calculatePredictedMakespan(mapping);
            case 1:
                return 1.0;
            default:
                return -1; 
        }
    }
}
