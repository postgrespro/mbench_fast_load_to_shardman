package bench;

import bench.v2.*;
import bench.v2.Configuration.Phase;
import bench.v2.Database.CallableStatement;
import bench.v2.histogram.HistogramUtils;
import bench.v2.histogram.Histograms;
import bench.v2.strategy.StrategyType;
import org.HdrHistogram.Histogram;
import org.apache.commons.cli.*;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bench.v2.histogram.Histograms.*;

public class V2 {

	static {
		System.setProperty("logback.configurationFile", "logback.xml");
		System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
		System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
	}

	private final static String lineSep = "-------------------------------------------------------------------";

	public static final Logger log = LoggerFactory.getLogger(V2.class);

	public static final String DEFPGPORT = "5432";
	private static final String DEFTIMEOUT = "10";
	private static final String DEFWORKERS = "5";
	private static final String DEFCONCURRENCY = "10";
	private static final String DEFVOLUME = "10";
	private static final String DEFRUNTYPE = Phase.EXECUTE.toString();
	private static final String DEFSTRATEGY = "none";

	public enum RangeOption {
		RANDOM
	}

	public static Database db;
	public static Configuration params;

	public static final int PERIOD_MS = 1000;
	public static Boolean verbosity = false;
	public static Boolean sessionAffinity = true;
	public static Boolean autoCommit = true;
	public static Boolean notStopAfterError = false; //Parallel execution will be interrupted after error, but the program will continue.

	public static Boolean stoppedByErr = false;

	public static AtomicBoolean dbGen = new AtomicBoolean(false);


	static private void iterateWithLatency(WorkerUnit workerUnit, WorkerState workerState) {
		long startTime = System.nanoTime();
		workerUnit.iterate(workerState);
		iterationLatencyHistogram.recordValue(System.nanoTime() - startTime);
	}


	public static boolean sql(String sql, Object... binds) {
		return db.<Boolean>execute((conn) -> {
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				for (int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}

				return pstmt.execute();
			}
		});
	}

	public static int quickInsert(String sql, PrepareStatement ts) {
		return db.<Integer>execute((conn) -> {

			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				ts.bind(pstmt);
				pstmt.execute();
				return pstmt.getUpdateCount();
			}
		});
	}

	public static String generateAlphaNumericString(int n) {
		// exactly 64 symbols
		String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvxyz!@#";
		StringBuilder sb = new StringBuilder(n);
		Random x = new Random();

		for (int i = 0; i < n; i++) {
			int index = x.nextInt() & 63;
			sb.append(AlphaNumericString.charAt(index));
		}

		return sb.toString();
	}

	public static boolean sqlCheckLatency(String histogramName, String sql, Object... binds) {
		Histogram histogram = Histograms.getUserHistogram(histogramName);

		return db.<Boolean>execute((conn) -> {
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				for (int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}

				long startTime = System.nanoTime();
				boolean result = pstmt.execute();
				histogram.recordValue(System.nanoTime() - startTime);

				return result;
			}
		});
	}

	public static void copyIn(String sql, String[] origData) {
		db.<Boolean>execute((conn) -> {
			PGConnection pgConnection = conn.unwrap(org.postgresql.PGConnection.class);
			CopyManager copyAPI = pgConnection.getCopyAPI();
			CopyIn cp = copyAPI.copyIn("COPY " + sql + " FROM STDIN");

			for (String anOrigData : origData) {
				byte[] buf = anOrigData.getBytes();
				cp.writeToCopy(buf, 0, buf.length);
			}

			cp.endCopy();

			return true;
		});
	}

	public static boolean sqlNoPrepare(String sql) {
		return db.<Boolean>execute((conn) -> {
			try (Statement stmt = conn.createStatement()) {
				return stmt.execute(sql);
			}
		});
	}

	public static <V> V sqlCustom(CallableStatement<V> custom) {
		return db.execute(custom);
	}

	@SuppressWarnings("unchecked")
	public static <E> E selectOne(String sql, Object... binds) {
		List<E> x = new ArrayList<>();
		db.selectSingle((rs) -> {
			x.add((E) rs.getObject(1));
		}, sql, binds);
		return x.isEmpty() ? null : x.get(0);
	}

	@SuppressWarnings("unchecked")
	public static <E> List<E> select(String sql, Object... binds) {
		return db.list((rs) -> {
			return (E) rs.getObject(1);
		}, sql, binds);
	}

	public static List<String> explainResults(String sql, Object... binds) {
		return select("explain (analyze, verbose, buffers, costs off) " + sql, binds);
	}

	public static void explain(Logger log, String sql, Object... binds) {
		List<String> lines = select("explain (analyze, verbose, buffers) " + sql, binds);
		if (log != null)
			log.info("Actual plan \n{}\n{}\n{}", lineSep, String.join("\n", lines), lineSep);
	}

	private static void preinit() {
		Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
			log.error("Test stopped due to error:", unwrap(e));
		});
	}

	public static void args(String[] args) {
		preinit();

		Options opt = new Options();

		/* Database options */
		opt.addOption(Option.builder("h").hasArg().argName("host").required()
				.desc("database host name").build());

		opt.addOption(Option.builder("p").hasArg().argName("port")
				.desc("database port. Defaults to " + DEFPGPORT).build());

		opt.addOption(Option.builder("d").hasArg().argName("database")
				.desc("database name. Defaults to 'postgres'").build());

		opt.addOption(Option.builder("U").hasArg().argName("username")
				.desc("user name. Defaults to 'postgres'").build());

		opt.addOption(Option.builder("P").hasArg().argName("password")
				.desc("user password").build());

		/* Workload options */
		opt.addOption(Option.builder("w").hasArg().argName("workers")
				.desc("amount of workers. Defaults to " + DEFWORKERS)
				.build());
		opt.addOption(Option.builder("c").hasArg().argName("concurrency")
				.desc("amount of concurrent workers. Defaults to " + DEFCONCURRENCY)
				.build());
		opt.addOption(Option.builder("o").hasArg().argName("run type")
				.desc("run type (generate|run). Defaults to " + DEFRUNTYPE)
				.build());
		opt.addOption(Option.builder("v").hasArg().argName("volume")
				.desc("volume size. Defaults to " + DEFVOLUME)
				.build());

		opt.addOption(Option.builder("t").hasArg().argName("timeout")
				.desc("test duration. Default to " + DEFTIMEOUT)
				.build());
		opt.addOption(Option.builder("s").hasArg().argName("strategy")
				.desc("work distribution strategy (" +
                        Stream.of(StrategyType.values())
                                .map(Object::toString)
                                .map(String::toLowerCase)
                                .collect(Collectors.joining("|"))+
                        "). Default to '" + DEFSTRATEGY + "'")
				.build());
		opt.addOption(Option.builder("T").hasArg().argName("txLimit")
				.desc("max amount of transactions. Disabled by default")
				.build());
		//must be over 30
		opt.addOption(Option.builder("l").hasArg().argName("cnTimeLimit")
				.desc("max life time of connection in seconds. Disabled by default")
				.build());

		params = new Configuration();
		try {
			CommandLine cmd = new DefaultParser().parse(opt, args);

			params.workers = Integer.parseInt(cmd.getOptionValue("w", DEFWORKERS));
			params.concurrency = Integer.parseInt(cmd.getOptionValue("c", DEFCONCURRENCY));
			params.strategy = StrategyType.valueOf(cmd.getOptionValue("s", DEFSTRATEGY).toUpperCase());
			params.volume = Long.parseLong(cmd.getOptionValue("v", DEFVOLUME));
			params.runType = Phase.valueOf(cmd.getOptionValue("o", DEFRUNTYPE));

			params.timeout = Integer.parseInt(cmd.getOptionValue("t", DEFTIMEOUT));
			params.txlimit = new AtomicLong(Long.parseLong(cmd.getOptionValue("T", "-1")));

			db = new Database(cmd.getOptionValue("h"),
					cmd.getOptionValue("p", DEFPGPORT),
					cmd.getOptionValue("d", "postgres"),
					cmd.getOptionValue("U", "postgres"),
					cmd.getOptionValue("P", "postgres"),
					params.workers,
					Long.parseLong(cmd.getOptionValue("l", "0")) * 1000L
			);
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -Xmx256m -jar pg_selectonly.jar", opt, true);
			System.out.println();
			/* Print exception at end */
			System.out.println("\033[0;1m[ERROR]: " + e.getMessage() + "\033[0m");
			System.exit(-1);
		}
	}

	public static void begin() {
		try {
			db.get().setAutoCommit(false);
		} catch (SQLException e) {
			throw new RuntimeException("Error on setAutoCommit(false)", e);
		}
	}

	public static void commit() {
		try {
			db.get().commit();
		} catch (SQLException e) {
			throw new RuntimeException("Error on commit", e);
		}
	}

	public static void ctx(String tableName,
										 List<Object> keyValues,
										 WorkerState workerState,
										 final WorkerUnit workerUnit) {

			try (Connection cc = db.getDataSource(new DataContext(tableName, keyValues)).getConnection()) {
				db.push(cc);
				iterateWithLatency(workerUnit, workerState);
				db.pop();
			} catch (SQLException e) {
				throw new RuntimeException("Error on commit", e);
			}

	}


	public static void transaction(String tableName,
								   List<Object> keyValues,
								   WorkerState workerState,
								   final WorkerUnit workerUnit) {
		try {
			Connection c = db.get();
			if (c != null) {
				c.setAutoCommit(false);
				iterateWithLatency(workerUnit, workerState);
				c.commit();
			} else {
				try (Connection cc = db.getDataSource(new DataContext(tableName, keyValues)).getConnection()) {
					db.push(cc);
					cc.setAutoCommit(false);
					iterateWithLatency(workerUnit, workerState);
					cc.commit();
					db.pop();
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error on commit", e);
		}
	}


	public static void transaction(WorkerState workerState,
								   final WorkerUnit workerUnit) {
		try {
			Connection c = db.get();
			if (c != null) {
				c.setAutoCommit(false);
				iterateWithLatency(workerUnit, workerState);
				c.commit();
			} else {
				try (Connection cc = db.getDataSource().getConnection()) {
					db.push(cc);
					cc.setAutoCommit(false);
					iterateWithLatency(workerUnit, workerState);
					cc.commit();
					db.pop();
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error on commit", e);
		}
	}

	public static Results parallel(final WorkerUnit workerUnit) {
		List<Snap> metrics = new ArrayList<>(1000);
		long durNs;
		String logResultsIntro;

		switch (params.runType) {
			case GENERATE:
				if (!dbGen.get()) {
					return null;
				}
				log.info("Starting {} workers for generate {} rows", params.workers, params.volume);
				durNs = parallelInternal(workerUnit, params.volume,
						Integer.MAX_VALUE, metrics, false);
				logResultsIntro = "Generation completed after";
				break;

			case EXECUTE:
				log.info("Starting {} workers for {} seconds", params.workers, params.timeout);
				durNs = parallelInternal(workerUnit, 0,
						params.timeout, metrics, verbosity);
				logResultsIntro = "Test completed after";
				break;

			default:
				return null;
		}

		if (!metrics.isEmpty()) {
			Results r = new Results(metrics, PERIOD_MS, durNs, (long) iterationLatencyHistogram.getMean());
			r.logSummary(log, logResultsIntro);

			String curDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss")
					.format(LocalDateTime.now());
			File reportFile = new File("./benchResults/bench_" + curDateTime);
			reportFile.mkdirs();

			for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
				File csv = new File(reportFile, entry.getKey() + ".csv");
				HistogramUtils.saveHistogram2csv(entry.getValue(), csv);
			}

			return r;
		} else {
			log.info("Metrics is empty");
			return null;
		}
	}


	private static long parallelInternal(final WorkerUnit workerUnit,
										 long iterationLimit,
										 int timeout,
										 List<Snap> snaps,
										 boolean monVerbose) {
		ExecutorService pool = Executors.newFixedThreadPool(params.workers);
		ScheduledExecutorService mon = Executors.newScheduledThreadPool(1);
		stoppedByErr = false;

		CyclicBarrier c = new CyclicBarrier(params.workers + 1);

		long mainLimit = 0L;
		long extraUnits = 0L;

		if (iterationLimit > 0) {
			mainLimit = params.volume / params.workers;
			extraUnits = params.volume % params.workers;
		}

		db.getDataSource();

		List<Future<Void>> workersFuture = new ArrayList<>(params.workers);
		ScheduledFuture<?> monFeature = null;

		List<WorkerState> states = new ArrayList<WorkerState>();
		for (int i = 0; i < params.workers; i++) {
			WorkerState st = new WorkerState();

			st.idProc = i;
			st.iterationsDone = 0;
			st.startPoint = c;

			st.iterationLimit = mainLimit;
			st.iterationLimit += (i < extraUnits) ? 1 : 0;

			states.add(st);

			workersFuture.add(pool.submit(() -> {
				try {
					try (Connection conn = db.getDataSource().getConnection()) {
						st.startPoint.await();
					}

					do {
						if (iterationLimit > 0 && st.iterationsDone >= st.iterationLimit)
							break;
						if (sessionAffinity) {
							try (Connection conn = db.getDataSource().getConnection()) {
								db.push(conn);
								iterateWithLatency(workerUnit, st);
							}
							finally {
								db.pop();
							}
						} else {
							iterateWithLatency(workerUnit, st);
						}

						st.iterationsDone++;
					} while (!st.stop.get() && !st.stopByErr.get());
				} catch (BrokenBarrierException e) {
					return null;
				} catch (Throwable e) {
					log.error("Occurred error", e);

					if (notStopAfterError) {
						for (WorkerState st1 : states) {
							st1.stopByErr.set(true);
						}
						stoppedByErr = true;
					}
					else
						throw e;
				}

				return null;
			}));
		}

		log.info("Waiting workers' readiness");

		try {
			/* Let's wait for all threads to be ready */
			c.await();
		} catch (BrokenBarrierException | InterruptedException e) {
			log.error("Occurred error", e);
			return -1;
		}

		Long startTime = System.nanoTime();
		if (snaps != null) {
			AtomicLong curIter = new AtomicLong(0);

			Long start = System.nanoTime();
			Long initialDelay = 1000 - (System.currentTimeMillis() % 1000);
			monFeature = mon.scheduleAtFixedRate(() -> {
				try {
					long iterations = 0;

					Long n = System.nanoTime();
					for (WorkerState st : states) {
						iterations += st.iterationsDone;
					}
					double iterateLatencyMean = iterationLatencyHistogram.getMean();


					long d = System.nanoTime() - n;
					long delta = iterations - curIter.get();

					if (delta > 0)
						tpsHistogram.recordValue(delta);

					if (monVerbose) {
						log.info(wrap2json(
								Long.toString(delta),
								histograms,
								Long.toString(d)));
					}

					curIter.set(iterations);

					Snap p = new Snap();
					p.ts = n - start;
					p.iterations = iterations;
					p.delta = delta;
					p.tookNs = d;

					snaps.add(p);
				} catch (RuntimeException ex) {
					log.error("Error in thread of monitoring", ex);
				}
			}, initialDelay, PERIOD_MS, TimeUnit.MILLISECONDS);
		}

		pool.shutdown();
		try {

			pool.awaitTermination(timeout, TimeUnit.SECONDS);
			for (WorkerState st : states) {
				st.stop.set(true);
			}

			workersFuture.forEach(future -> future.cancel(true));
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		Long endTime = System.nanoTime();

		if (snaps != null) {
			mon.shutdownNow();
			monFeature.cancel(true);
		}

		return endTime - startTime;
	}

	private static String wrap2json(String iterations,
									Map<String, Histogram> histogramMap,
									String took) {
		StringBuilder result = new StringBuilder();
		result.append("{");
		result.append("\"iterations\": ").append(iterations);

		for (Map.Entry<String, Histogram> entry : histogramMap.entrySet()) {
			result.append(", \"").append(entry.getKey()).append("_mean\": ").append((long) entry.getValue().getMean());
		}

		result.append(", \"took\": ").append(took);
		result.append("}");
		return result.toString();
	}

	public static Throwable unwrap(Throwable throwable) {
		Objects.requireNonNull(throwable);
		Throwable rootCause = throwable;
		while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
			rootCause = rootCause.getCause();
		}
		return rootCause;
	}

	private static String getDatabaseSettingValue(String gucName) {
		return selectOne(String.format("SHOW %s;", gucName));
	}

	public static void requireSettings(String gucName, String newValue) {
		String curValue = getDatabaseSettingValue(gucName);
		if (curValue == newValue) {
			return;
		}

		sql(String.format("SET %s TO '%s'", gucName, newValue));
	}

	public static void psql(String filename, Integer hostNum) {
		try (Connection conn = db.getDataSource().getConnection()) {
			log.info("Execute SQL script {} via psql host {}", filename, db.hosts[hostNum]);
			PSQL.executeFile(filename, hostNum);
			log.info("Completed SQL script");
		} catch (Exception e) {
			log.error("Occurred error", e);
		}
	}


	public static void psqlSql(String sql, Integer hostNum) {
		try (Connection conn = db.getDataSource().getConnection()) {
			log.info("Execute SQL {} via psql host {}", sql, db.hosts[hostNum]);
			PSQL.executeSql(sql, db.userName, db.passwd, db.dbName, hostNum, true);
			log.info("Completed SQL script");
		} catch (Exception e) {
			log.error("Occurred error", e);
		}
	}


	public static void psqlSqlNoWait(String sql, Integer hostNum) {
		try (Connection conn = db.getDataSource().getConnection()) {
			log.info("Execute SQL {} via psql host {}", sql, db.hosts[hostNum]);
			PSQL.executeSql(sql, db.userName, db.passwd, db.dbName, hostNum, false);
			log.info("Completed SQL script");
		} catch (Exception e) {
			log.error("Occurred error", e);
		}
	}

	public static void requireData(String checkSQL, String filename) {
		requireData(checkSQL, filename, 0);
	}


	public static void requireData(String checkSQL,
								   String filename,
								   Integer hostNum) {
		Callable<Void> psql = () -> {
			log.info("Execute SQL script {} via psql host {}", filename, db.hosts[hostNum]);
			psql(filename, hostNum);
			log.info("Completed SQL script");
			return null;
		};

		requireData(checkSQL, psql);
	}


	public static void requireData(String checkSQL,
								   String filename,
								   String username,
								   String password,
								   String database,
								   Integer hostNum) {
		Callable<Void> psql = () -> {
			log.info("Execute SQL script {} via psql host {} under user {} in database {}",
					filename, db.hosts[hostNum], username, database);
			PSQL.executeFile(filename, username, password, database, hostNum);
			log.info("Completed SQL script");
			return null;
		};

		requireData(checkSQL, psql);
	}

	public static void requireData(String checkSQL, Callable<Void> psql) {
		// check datasource
		try {
			db.getDataSource();
		} catch (Throwable e) {
			log.error("Some error", e);
			try {
				dbGen.set(true);
				log.info("Can't get DS, safe mode...");
				psql.call();
			} catch (Exception x) {
				log.error("Some error", e);
				throw new RuntimeException("Exception occured during error handling...", x);
			} finally {
				dbGen.set(false);
			}
		}

		CallableStatement<Boolean> checkStmt = (conn) -> {
			try (PreparedStatement pstmt = conn.prepareStatement(checkSQL)) {
				return pstmt.execute();
			}
		};

		Callable<Void> handleOnError = () -> {
			try {
				dbGen.set(true);
				log.info("Handle SQL error, safe mode...");
				psql.call();
			} finally {
				dbGen.set(false);
			}

			return null;
		};

		db.<Boolean>execute(checkStmt, handleOnError);
	}


	public static void assertSimilar(Results a,
									 Results b,
									 String msgFormat,
									 Object... params) {
		if (!a.similar(b)) {
			log.error("TEST FAILED: " + msgFormat, params);
		} else {
			log.info("TEST PASSED: " + msgFormat, params);
		}
	}

	public static void logResults(Results res) {
		log.info("Test results: last 5 sec {} tps, overall {} tps, {} iterations",
				res.tpsLast5sec, res.tps, res.iterations);
	}
}