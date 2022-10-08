package src.src;

import java.util.*;
import java.util.Map.Entry;

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

public class MaxMin {

	private static int num_vm = 10, num_cloudlets = 20;
	private static double FinalTime;
	private static List<Cloudlet> cloudletList;

	private static List<Cloudlet> sortList = new ArrayList<Cloudlet>();
	private static double resTime[] = new double[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	private static List<Vm> vmList;

	private static List<Vm> createVM(int userId, int vms, int idShift) {

		LinkedList<Vm> list = new LinkedList<Vm>();

		long size = 10000;
		int ram = 512;
		int mips;
		long bw = 1000;
		int pesNumber = 1;
		String vmm = "Xen";

		Vm[] vm = new Vm[vms];

		for (int i = 0; i < vms; i++) {
			mips = 100 + (i * 50);
			vm[i] = new Vm(idShift + i, userId, mips, pesNumber, ram, bw, size, vmm,
					new CloudletSchedulerSpaceShared());
			list.add(vm[i]);
			System.out.println("");
			System.out.println("Vm" + i + "  mips:" + mips);
		}

		return list;
	}

	private static List<Cloudlet> createCloudlet(int userId, int cloudlets, int idShift) {

		LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

		long fileSize = 300;
		long outputSize = 300;
		int pesNumber = 1;
		UtilizationModel utilizationModel = new UtilizationModelFull();

		Cloudlet[] cloudlet = new Cloudlet[cloudlets];

		for (int i = 0; i < cloudlets; i++) {
			cloudlet[i] = new Cloudlet(idShift + i, 4000 + (i * 1000), pesNumber, fileSize, outputSize,
					utilizationModel, utilizationModel, utilizationModel);

			cloudlet[i].setUserId(userId);
			list.add(cloudlet[i]);
		}

		return list;
	}

	public static void main(String[] args) {
		Log.printLine("Starting Max-Min Algorithm Simulation...");

		try {

			int num_user = 2;
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;

			CloudSim.init(num_user, calendar, trace_flag);

			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			DatacenterBroker broker = createBroker("Broker_0");
			int brokerId = broker.getId();

			vmList = createVM(brokerId, num_vm, 0);
			cloudletList = createCloudlet(brokerId, num_cloudlets, 0);

			broker.submitVmList(vmList);
			bindCloudletToVmsMaxMin();

			broker.submitCloudletList(sortList);

			CloudSim.startSimulation();

			List<Cloudlet> newList = broker.getCloudletReceivedList();

			CloudSim.stopSimulation();
			double start = CloudSim.clock();

			printCloudletList(newList);

			System.out.println("");
			System.out.println("");
			System.out.println("");
			calculateThroughput(newList);
			calculateWaitingTime(newList);
			calculateResponseTime(newList);
			System.out.println("");
			Log.printLine("Max-Min Algorithm Simulation finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	private static Datacenter createDatacenter(String name) {

		List<Host> hostList = new ArrayList<Host>();

		List<Pe> peList1 = new ArrayList<Pe>();

		int mips = 10000;

		peList1.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(3, new PeProvisionerSimple(mips)));

		List<Pe> peList2 = new ArrayList<Pe>();

		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(1, new PeProvisionerSimple(mips)));

		int hostId = 0;
		int ram = 16384;
		long storage = 1000000;
		int bw = 100000;

		hostList.add(
				new Host(
						hostId,
						new RamProvisionerSimple(ram),
						new BwProvisionerSimple(bw),
						storage,
						peList1,
						new VmSchedulerTimeShared(peList1)));

		hostId++;

		hostList.add(
				new Host(
						hostId,
						new RamProvisionerSimple(ram),
						new BwProvisionerSimple(bw),
						storage,
						peList2,
						new VmSchedulerTimeShared(peList2)));

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

	private static DatacenterBroker createBroker(String name) {

		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker(name);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "   ";

		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + indent + "STATUS" + indent + indent +
				"Data center ID" + indent + indent + "VM ID" + indent + indent + "    " + "Time" + indent + indent
				+ indent + indent + "Start Time" + indent + indent + indent + indent + "Finish Time");
		String indent2 = "      ";
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			System.out.printf(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				System.out.printf(indent2 + "SUCCESS");
				System.out.printf(indent + indent + indent + cloudlet.getResourceId() + indent + indent + indent
						+ indent + "   " + cloudlet.getVmId() + indent + indent + indent + "  ");
				System.out.printf("%-22.5f", cloudlet.getActualCPUTime());
				System.out.printf("%-22.5f", cloudlet.getExecStartTime());
				System.out.printf("%-22.5f", cloudlet.getFinishTime());
				System.out.println("");

				resTime[cloudlet.getVmId()] += cloudlet.getActualCPUTime();
				if (i == size - 1) {
					FinalTime = cloudlet.getFinishTime();
				}
			}
		}
		System.out.println("");
		for (int k = 0; k < 10; k++) {
			resTime[k] = (resTime[k] / FinalTime) * 100;
			System.out.println("% Resource Time for vm " + (k + 1) + " : " + resTime[k]);
		}

	}

	public static void bindCloudletToVmsMaxMin() {
		int cloudletNum = cloudletList.size();
		int vmNum = vmList.size();

		Double[] readyTime = new Double[vmNum];
		for (int i = 0; i < readyTime.length; i++) {
			readyTime[i] = 0.0;
		}

		List<List<Double>> tasksVmsMatrix = create2DMatrix(cloudletList, vmList);

		int count = 1;

		do {
			System.out.println("===========================");
			System.out.println("This is start of iteration " + count);
			print2DArrayList(tasksVmsMatrix);

			Map<Integer[], Double> map = findMaxMinTimeMap(tasksVmsMatrix);
			printMapForMaxMin(map);

			Integer[] rowAndColIndexAndCloudletId = getRowAndColIndexesAndCloudletId(map);
			Double maxMin = getMinimumTimeValue(map);
			int rowIndex = rowAndColIndexAndCloudletId[0];
			int columnIndex = rowAndColIndexAndCloudletId[1];
			int cloudletId = rowAndColIndexAndCloudletId[2];

			cloudletList.get(cloudletId).setVmId(vmList.get(columnIndex).getId());
			System.out.printf("The cloudlet %d has been assigned to VM %d \n", cloudletId, columnIndex);

			Cloudlet cloudlet = cloudletList.get(cloudletId);
			sortList.add(cloudlet);

			Double oldReadyTime = readyTime[columnIndex];
			readyTime[columnIndex] = maxMin;
			System.out.printf("The ready time array is %s \n", Arrays.toString(readyTime));

			updateTotalTimeMatrix(columnIndex, oldReadyTime, readyTime, tasksVmsMatrix);

			tasksVmsMatrix.remove(rowIndex);

			System.out.println("This is end of iteration " + count);
			System.out.println("===========================");
			++count;
		} while (tasksVmsMatrix.size() > 0);
		calculateThroughputNew(readyTime, cloudletNum);
	}

	private static List<List<Double>> create2DMatrix(List<Cloudlet> cloudletList, List<Vm> vmList) {
		List<List<Double>> table = new ArrayList<List<Double>>();
		for (int i = 0; i < cloudletList.size(); i++) {

			Double originalCloudletId = (double) cloudletList.get(i).getCloudletId();

			List<Double> temp = new ArrayList<Double>();

			for (int j = 0; j < vmList.size(); j++) {
				Double load = cloudletList.get(i).getCloudletLength() / vmList.get(j).getMips();
				temp.add(load);
			}
			temp.add(originalCloudletId);
			table.add(temp);
		}
		return table;
	}

	private static Map<Integer[], Double> findMaxMinTimeMap(List<List<Double>> tasksVmsMatrix) {

		int rowNum = tasksVmsMatrix.size();
		int colNum = tasksVmsMatrix.get(0).size();

		int colNumWithoutLastColumn = colNum - 1;

		Map<Integer[], Double> map = new HashMap<Integer[], Double>();

		for (int row = 0; row < rowNum; row++) {

			Double min = tasksVmsMatrix.get(row).get(0);

			Integer targetCloudletId = tasksVmsMatrix.get(row).get(colNumWithoutLastColumn).intValue();

			Integer[] rowInfo = { row, 0, targetCloudletId };

			for (int col = 0; col < colNumWithoutLastColumn; col++) {
				Double current = tasksVmsMatrix.get(row).get(col);
				if (current < min) {
					min = current;
					rowInfo[1] = col;
				}
			}
			map.put(rowInfo, min);
		}

		HashMap<Integer[], Double> sortedMap = sortMapByValue(map);

		Map<Integer[], Double> firstPair = getFirstPairFromMap(sortedMap);

		return firstPair;
	}

	@SuppressWarnings("unchecked")
	public static <T extends Cloudlet> List<T> getSortList() {
		return (List<T>) sortList;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static HashMap<Integer[], Double> sortMapByValue(Map<Integer[], Double> map) {
		Set<Entry<Integer[], Double>> set = map.entrySet();
		List<Entry<Integer[], Double>> list = new ArrayList<Entry<Integer[], Double>>(set);

		Collections.sort(list, new Comparator<Map.Entry<Integer[], Double>>() {
			public int compare(Map.Entry<Integer[], Double> o1, Map.Entry<Integer[], Double> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		HashMap sortedHashMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedHashMap.put(entry.getKey(), entry.getValue());
		}
		return sortedHashMap;
	}

	private static Integer[] getRowAndColIndexesAndCloudletId(Map<Integer[], Double> map) {
		Integer[] key = new Integer[3];
		for (Entry<Integer[], Double> entry : map.entrySet()) {
			key = entry.getKey();
		}
		return key;
	}

	private static Double getMinimumTimeValue(Map<Integer[], Double> map) {
		Double value = 0.0;
		for (Entry<Integer[], Double> entry : map.entrySet()) {
			value = entry.getValue();
		}
		return value;
	}

	private static void updateTotalTimeMatrix(int columnIndex, Double oldReadyTime, Double[] readyTime,
			List<List<Double>> taskVmsMatrix) {

		Double newReadyTime = readyTime[columnIndex];
		Double readyTimeDifference = newReadyTime - oldReadyTime;
		for (int row = 0; row < taskVmsMatrix.size(); row++) {
			Double oldTotalTime = taskVmsMatrix.get(row).get(columnIndex);
			Double newTotalTime = oldTotalTime + readyTimeDifference;
			taskVmsMatrix.get(row).set(columnIndex, newTotalTime);
		}
	}

	private static void print2DArrayList(List<List<Double>> table) {
		String indent = "           ";
		System.out.println("The current required exceution time matirx is as below,with size of " + table.size()
				+ " by " + table.get(0).size());

		for (int j = 0; j < num_vm; j++) {
			System.out.printf("    Vm" + j + indent);
		}
		System.out.println("cloudletNum");
		for (int i = 0; i < table.size(); i++) {

			String indent2 = "   ";
			for (int j = 0; j < table.get(i).size(); j++) {
				System.out.printf("%-15.5f", table.get(i).get(j));
				System.out.printf(indent2);
			}
			System.out.printf("\n");
		}

	}

	private static void printMapForMaxMin(Map<Integer[], Double> map) {
		for (Entry<Integer[], Double> entry : map.entrySet()) {
			Integer[] key = entry.getKey();
			Double value = entry.getValue();
			System.out.println("");
			System.out.printf("The required values {row,column,cloudlet id} : {%d, %d, %d} ===> ", key[0], key[1],
					key[2]);
			System.out.printf("%.4f(%s), located at row %d column %d, and the cloudlet id is %d \n", value, "max",
					key[0], key[1], key[2]);
		}
	}

	private static Map<Integer[], Double> getFirstPairFromMap(Map<Integer[], Double> map) {
		Map.Entry<Integer[], Double> entry = map.entrySet().iterator().next();
		Integer[] key = entry.getKey();
		Double value = entry.getValue();
		Map<Integer[], Double> firstPair = new HashMap<Integer[], Double>();
		firstPair.put(key, value);
		return firstPair;
	}

	public double calculateAvgTurnAroundTime(List<? extends Cloudlet> cloudletList) {
		double totalTime = 0.0;
		int cloudletNum = cloudletList.size();
		for (int i = 0; i < cloudletNum; i++) {
			totalTime += cloudletList.get(i).getFinishTime();

		}
		double averageTurnAroundTime = totalTime / cloudletNum;
		System.out.printf("The average turnaround time is %.4f\n", averageTurnAroundTime);
		return averageTurnAroundTime;
	}

	public static double calculateThroughput(List<? extends Cloudlet> cloudletList) {
		double maxFinishTime = 0.0;
		int cloudletNum = cloudletList.size();
		for (int i = 0; i < cloudletNum; i++) {
			double currentFinishTime = cloudletList.get(i).getFinishTime();
			if (currentFinishTime > maxFinishTime)
				maxFinishTime = currentFinishTime;
		}
		double throughput = cloudletNum / maxFinishTime;
		System.out.printf("The throughput is %.6f\n", throughput);
		return throughput;
	}

	public static double calculateThroughputNew(Double[] readyTime, int cloudletNum) {
		List<Double> temp = new ArrayList<Double>(Arrays.asList(readyTime));
		Double throughput = cloudletNum / (Collections.max(temp) + 0.1);
		System.out.printf("The throughput is %.6f \n", throughput);
		return throughput;
	}

	public static double calculateWaitingTime(List<Cloudlet> list) {
		double TotalWaitTime = 0.0;
		int size = list.size();
		Cloudlet cloudlet;
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			double indiWaitTime = cloudlet.getExecStartTime() - cloudlet.getSubmissionTime();
			TotalWaitTime = TotalWaitTime + indiWaitTime;
		}
		double avgWaitTime = TotalWaitTime / size;
		System.out.printf("The avg Waiting time is is %.6f\n", avgWaitTime);
		return avgWaitTime;

	}

	public static double calculateResponseTime(List<Cloudlet> list) {
		double TotalResponseTime = 0.0;
		int size = list.size();
		Cloudlet cloudlet;
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			double indiResponseTime = cloudlet.getFinishTime() - cloudlet.getSubmissionTime();
			TotalResponseTime = TotalResponseTime + indiResponseTime;
		}
		double avgResTime = TotalResponseTime / size;
		System.out.printf("The avg response time is is %.6f\n", avgResTime);
		return avgResTime;

	}
}