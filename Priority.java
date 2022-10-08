package src.src;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;

import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
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
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class Priority {

	private static List<Cloudlet> cloudletList;

	private static List<Vm> vmlist;

	public static void main(String[] args) {

		Log.printLine("Starting Priority...");

		try {

			int num_user = 1;
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;

			CloudSim.init(num_user, calendar, trace_flag);

			@SuppressWarnings("unused")
			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			DatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			vmlist = new ArrayList<Vm>();

			int vms = 10;
			int cloudlets = 700;

			int vmid = 0;
			long size = 1000;
			int ram = 512;
			int mips = 250;
			long bw = 1000;
			int pesNumber = 1;
			String vmm = "Xen";

			int jk = 1;

			Vm vm[] = new Vm[vms];

			for (int i = 0; i < vms; i++) {
				if (i % 2 == 0)
					mips += jk;
				else
					mips -= jk;
				vm[i] = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
				vmid++;
				jk += 2;
				vmlist.add(vm[i]);

			}
			vmid--;

			List<Vm> lstvms = vmlist;
			for (int a = 0; a < lstvms.size(); a++) {
				for (int b = a + 1; b < lstvms.size(); b++) {
					if (lstvms.get(b).getMips() > lstvms.get(a).getMips()) {
						Vm temp = lstvms.get(a);
						lstvms.set(a, lstvms.get(b));
						lstvms.set(b, temp);
					}
				}
			}
			for (Vm mm : lstvms) {
				System.out.println("Vm id = " + mm.getId() + " - MIPS = " + mm.getMips());
			}

			broker.submitVmList(lstvms);

			cloudletList = new ArrayList<Cloudlet>();

			int id = 0;
			long length = 4000;
			long fileSize = 300;
			long outputSize = 300;

			UtilizationModel utilizationModel = new UtilizationModelFull();

			Cloudlet[] cloudlet = new Cloudlet[cloudlets];

			for (int i = 0; i < cloudlets; i++) {
				if (i % 2 == 0 || i < 2)
					length += 6500;
				else
					length -= 3277;

				cloudlet[i] = new Cloudlet(++id, length, pesNumber, fileSize, outputSize, utilizationModel,
						utilizationModel, utilizationModel);

				cloudlet[i].setUserId(brokerId);
				cloudletList.add(cloudlet[i]);
			}

			List<Cloudlet> lstCloudlets = cloudletList;
			for (int a = 0; a < lstCloudlets.size(); a++) {
				for (int b = a + 1; b < lstCloudlets.size(); b++) {
					if (lstCloudlets.get(b).getCloudletLength() > lstCloudlets.get(a).getCloudletLength()) {
						Cloudlet temp = lstCloudlets.get(a);
						lstCloudlets.set(a, lstCloudlets.get(b));
						lstCloudlets.set(b, temp);
					}
				}
			}

			broker.submitCloudletList(lstCloudlets);

			CloudSim.startSimulation();

			List<Cloudlet> newList = broker.getCloudletReceivedList();

			CloudSim.stopSimulation();

			printCloudletList(newList);

			Log.printLine("Priority finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	private static Datacenter createDatacenter(String name) {

		List<Host> hostList = new ArrayList<Host>();

		List<Pe> peList = new ArrayList<Pe>();

		int mips = 302400;

		peList.add(new Pe(0, new PeProvisionerSimple(mips)));

		int hostId = 0;
		int ram = 102400;
		long storage = 1000000;
		int bw = 200000;

		hostList.add(
				new Host(
						hostId,
						new RamProvisionerSimple(ram),
						new BwProvisionerSimple(bw),
						storage,
						peList,
						new VmSchedulerTimeShared(peList)));

		String arch = "x86";
		String os = "Linux";
		String vmm = "Xen";
		double time_zone = 10.0;
		double cost = 3.0;
		double costPerMem = 0.05;
		double costPerStorage = 0.001;
		double costPerBw = 0.0;
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

	private static DatacenterBroker createBroker() {

		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
				"Data center ID" + indent + "VM ID" + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print("SUCCESS");

				Log.printLine(
						indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId() +
								indent + indent + dft.format(cloudlet.getActualCPUTime()) + indent + indent
								+ dft.format(cloudlet.getExecStartTime()) +
								indent + indent + dft.format(cloudlet.getFinishTime()));
			} else {
				Log.print("Failure");
			}
		}

	}
}