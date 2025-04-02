package bench.v2.strategy;

public enum StrategyType {
    NONE(new NoneStrategy()),
    SEQUENT(new SequentStrategy()),
    RANDOM(new RandomStrategy()),
    PINNING(new PinningStrategy()),
    SDMWISE(new ShardmanWiseStrategy());

    private final IDistributionStrategy iDistributionStrategy;

    StrategyType(IDistributionStrategy iDistributionStrategy) {
        this.iDistributionStrategy = iDistributionStrategy;
    }

    public IDistributionStrategy getStrategy() {
        return iDistributionStrategy;
    }
}