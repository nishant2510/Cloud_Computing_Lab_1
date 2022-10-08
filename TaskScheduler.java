package src.src;

public abstract class TaskScheduler {
    protected Simulation sim;
    
    public TaskScheduler(Simulation sim) {
        this.sim = sim;
    }
    
    public abstract int[] schedule(int MAX_FES);
}
