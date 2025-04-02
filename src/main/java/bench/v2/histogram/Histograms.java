package bench.v2.histogram;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SynchronizedHistogram;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class Histograms {
	// All times should be stored in nanos.
	private static final long TIME_LOWEST_VALUE = 1_000L; // 1_000 ns is error rate, minimal trackable value
	private static final long TIME_HIGHEST_VALUE = 3_600_000_000_000L; // maximum value in the histogram (1 hour)
	private static final int SIGNIFICANT_DIGITS = 3; // maintain a value accuracy of ~0.1%

	// latency of the iteration
	public static final Histogram iterationLatencyHistogram = new SynchronizedHistogram(TIME_LOWEST_VALUE, TIME_HIGHEST_VALUE, SIGNIFICANT_DIGITS);
	// TPS in each period
	public static final Histogram tpsHistogram = new Histogram(1_000_000_000L, SIGNIFICANT_DIGITS);
	// Contains all trackable histograms.
	// LinkedHashMap save right key order.
	public static final HashMap<String, Histogram> histograms = new LinkedHashMap<>();

	static {
		histograms.put("latency", iterationLatencyHistogram);
		histograms.put("tps", tpsHistogram);
	}

	public static Histogram getUserHistogram(String histogramName) {
		if (!histograms.containsKey(histogramName)) {
			synchronized (histograms) {
				return histograms.computeIfAbsent(histogramName,
						(h) -> new SynchronizedHistogram(TIME_LOWEST_VALUE, TIME_HIGHEST_VALUE, SIGNIFICANT_DIGITS));
			}
		}
		return histograms.get(histogramName);
	}
}
