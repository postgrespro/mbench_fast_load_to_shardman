package bench.v2.strategy;

import bench.v2.DataContext;
import bench.v2.Database;

public class PinningStrategy implements IDistributionStrategy {
	
	public ThreadLocal<Integer> workerConn = new ThreadLocal<Integer>();
	public IDistributionStrategy sq;
	
	@Override
	public void init(Database db) {
		sq = StrategyType.SEQUENT.getStrategy();
		sq.init(db);
	}

	@Override
	public Integer getDataSourceID(DataContext ctx) {
		if(workerConn.get() == null)
		{
			workerConn.set(sq.getDataSourceID(ctx));
		}
		return workerConn.get();	
	}

}
