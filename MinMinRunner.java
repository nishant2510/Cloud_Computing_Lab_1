package src.src;

import java.util.Random;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class MinMinRunner {
    private static int SEED = 0;
    private static int NUM_TRY = 10;

    private static int cloudletSchedulerType = 0; //0: space shared, 1: time shared
    private static int numOfCloudlets;
    private static int highHeterogeneity;
    private static int numOfVMs = 10;
    private static int brokerType; //0: Mapping broker, 1: SJF Broker, 2: FCFS Broker (Standard DatacenterBroker)
    private static boolean silent = true;
    private static int fitnessType = 0; 

    static int MAX_FES;

    private static void calculateStatistics(String algName, double[] results) {
        System.out.println("\n\n------ " + algName + " -----");
        DescriptiveStatistics ds = new DescriptiveStatistics();
        for (double data : results) {
            ds.addValue(data);
        }
        System.out.println("\tAvg: " + ds.getMean());
        System.out.println("\tMin: " + ds.getMin());
        System.out.println("\tMax: " + ds.getMax());
        System.out.println("\tStd: " + ds.getStandardDeviation());
    }

    public static void MinMinExp() {
        double[] results = new double[NUM_TRY];
        for (int i = 0; i < NUM_TRY; i++) {
            Random rng = new Random(SEED + i);
            brokerType = 0;
            Simulation sim = new Simulation(cloudletSchedulerType, numOfCloudlets, numOfVMs, brokerType, fitnessType, rng, silent, highHeterogeneity);
            MinMinScheduler minmins = new MinMinScheduler(sim);
            int[] mapping = minmins.schedule(0);
            double makespan = sim.runSimulation(mapping);
            results[i] = makespan;
        }
        calculateStatistics("MinMin", results);
    }

    public static void main(String[] args) {
        //scenario 0
        System.out.println("\n********************** SCENARIO 0 **************************");
        numOfCloudlets = 100;
        highHeterogeneity = 1;
        MAX_FES = numOfCloudlets * 1000;
        MinMinExp();

        //scenario 1
        System.out.println("\n********************** SCENARIO 1 **************************");
        numOfCloudlets = 100;
        highHeterogeneity = 0;
        MAX_FES = numOfCloudlets * 1000;
        MinMinExp();

        //scenario 2
        System.out.println("\n********************** SCENARIO 2 **************************");
        numOfCloudlets = 1000;
        highHeterogeneity = 1;
        MAX_FES = numOfCloudlets * 1000;
        MinMinExp();

        //scenario 3
        System.out.println("\n********************** SCENARIO 3 **************************");
        numOfCloudlets = 1000;
        highHeterogeneity = 0;
        MinMinExp();

        //scenario 4
        System.out.println("\n********************** SCENARIO 4 **************************");
        numOfCloudlets = 5000;
        highHeterogeneity = 1;
        MAX_FES = numOfCloudlets * 1000;
        MinMinExp();

        //scenario 5
        System.out.println("\n********************** SCENARIO 5 **************************");
        numOfCloudlets = 5000;
        highHeterogeneity = 0;
        MinMinExp();
    }
}
