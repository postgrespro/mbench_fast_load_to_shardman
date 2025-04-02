package bench.v2;

import bench.V2;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static bench.V2.db;

public class Var {
	public Random rnd = new Random();
	public AtomicLong value = new AtomicLong(0);

	public Long start = 0L;
	public Long end = Long.MAX_VALUE;

	public Long get() {
		if (rnd == null)
			return value.get();

		if (end == Long.MAX_VALUE && start == 0L) {
			return rnd.nextLong();
		}

		return (long) (start + rnd.nextFloat() * (end - start));
	}

	public Long next() {
		if (value.get() < end) {
			return value.getAndIncrement();
		} else {
			throw new RuntimeException("max value is reached");
		}
	}


	/* Variables */
	public static Var var(String sql, V2.RangeOption... options) {
		Var res = new Var();
		for (V2.RangeOption option : options) {
			switch (option) {
				case RANDOM:
					res.rnd = new Random();
					break;
			}
		}


		db.selectSingle((rs) -> {
			res.start = rs.getLong(1);
			res.end = rs.getLong(2);
			return;
		}, sql);

		return res;
	}

	public static Var var(Long min, Long max, V2.RangeOption... options) {
		Var res = new Var();
		for (V2.RangeOption option : options) {
			switch (option) {
				case RANDOM:
					res.rnd = new Random();
					break;
			}
		}

		res.start = min;
		res.end = max;

		return res;
	}

	public static Var var(Integer min, Integer max, V2.RangeOption... options) {
		Var res = new Var();
		for (V2.RangeOption option : options) {
			switch (option) {
				case RANDOM:
					res.rnd = new Random();
					break;
			}
		}

		res.start = min.longValue();
		res.end = max.longValue();

		return res;
	}

	public void set(Long x) {
		value.set(x);
	}

	public Long min() {
		return start;
	}

	public Long max() {
		return end;
	}

	@Override
	public String toString() {
		return get().toString();
	}
}
