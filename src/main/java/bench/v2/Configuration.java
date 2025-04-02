package bench.v2;

import bench.v2.strategy.StrategyType;

import java.util.concurrent.atomic.AtomicLong;

public class Configuration {

	public enum Phase {
		GENERATE,
		EXECUTE
	}

	/* Worker parameters */
	public Integer startPoint;
	public Integer workers;
	public Integer concurrency;
	public Integer loops;
	public Integer timeout;
	public AtomicLong txlimit;

	public Long volume;
	public Phase runType;
	public StrategyType strategy;
}
