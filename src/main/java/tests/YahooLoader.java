package tests;

import bench.V2;
import bench.v2.QuickInsertSqlBuilder;
import bench.v2.strategy.shardman.Hash;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static bench.V2.*;

public class YahooLoader {

	public final static String INSERT_SQL = "insert into usertable(ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)";
	public final static Integer ROWS = 500;
	public final static String qiSQL = QuickInsertSqlBuilder.buildSQL(INSERT_SQL, 11, ROWS) + " on conflict do nothing";

	public static Integer SHARD_CNT = 10;
	public static Integer PARTITION_CNT = 120;
	public static Long ROW_LIMIT = 800000000000L;

	public static AtomicLong pos = null;

	static boolean exitMonitoring = false;
	static Thread monitoring = new Thread(){
		public void run(){
			while (!exitMonitoring) {
				try {
					sql("insert into monitoring_insert values (now(), ?, ?)", params.volume.intValue(), pos.get());
					sleep(300000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	};

	public static void main(String[] args) {
		args(args);

		autoCommit = true;
		verbosity = false;

		SHARD_CNT = (System.getenv("SHARD_CNT") != null) ? Integer.parseInt(System.getenv("SHARD_CNT")) : 10;
		PARTITION_CNT = (System.getenv("PARTITION_CNT") != null) ? Integer.parseInt(System.getenv("PARTITION_CNT")) : 120;
		ROW_LIMIT = (System.getenv("ROW_LIMIT") != null) ? Integer.parseInt(System.getenv("ROW_LIMIT")) : 800000000000L;

		requireData("select 1 from monitoring_insert limit 1", ()-> {
			V2.psqlSql(String.format("create table monitoring_insert(\n" +
					"ts timestamp,\n" +
					"shard int,\n" +
					"val bigint\n" +
					");"), 0);

			return null;
		});

		long initAtomic = (System.getenv("BENCH_RUN_AFTER") != null) ? Long.parseLong(System.getenv("BENCH_RUN_AFTER")) : getStartId(params.volume);

		pos = new AtomicLong(initAtomic);
		String[] randomData = new String[ROWS * 10];
		
		for (int i = 0; i < ROWS * 10; i++) {
			randomData[i] = generateAlphaNumericString(100);
		}

		monitoring.start();

		V2.requireData ("select 1 from usertable limit 1", () -> {
			V2.psqlSql(String.format("CREATE TABLE IF NOT EXISTS usertable(\n" +
					"  YCSB_KEY VARCHAR(64) PRIMARY key\n" +
					", FIELD0 VARCHAR(100)\n" +
					", FIELD1 VARCHAR(100)\n" +
					", FIELD2 VARCHAR(100)\n" +
					", FIELD3 VARCHAR(100)\n" +
					", FIELD4 VARCHAR(100)\n" +
					", FIELD5 VARCHAR(100)\n" +
					", FIELD6 VARCHAR(100)\n" +
					", FIELD7 VARCHAR(100)\n" +
					", FIELD8 VARCHAR(100)\n" +
					", FIELD9 VARCHAR(100)\n" +
					") with (distributed_by='ycsb_key', num_parts=%d)", PARTITION_CNT), 0);

			return null;
		});

		parallel((state) -> {
			state.iterationsDone += quickInsert(qiSQL, (p) -> {
				if (pos.get() > ROW_LIMIT) {
					System.out.println("Reached " + ROW_LIMIT);
					System.exit(0);
				}

				int base = 1;
				int rndPos = 0;

				long[] items = new long[ROWS];
				for (int i = 0; i < ROWS;) {
					long x = pos.getAndIncrement();
					//items[i] = fnv1a64(longToBytes(x));
					String id = "user" + x;
					if ((Long.remainderUnsigned(Hash.hashBytes(id.getBytes(), Hash.PartitionSeed) + 5_305_509_591_434_766_563L, PARTITION_CNT))
							% SHARD_CNT == params.volume) {
						items[i] = x;
						i++;
					}
				}
				Arrays.sort(items);

				for (int i = 0; i < ROWS; i++) {
					/*
					 * fnv1a produces duplicates, so nothing on conflict.
					 */
					p.setString(base++, "user" + items[i]);
					p.setString(base++, randomData[rndPos++]);
					p.setString(base++, randomData[rndPos++]);
					p.setString(base++, randomData[rndPos++]);
					p.setString(base++, randomData[rndPos++]);
					p.setString(base++, randomData[rndPos++]);
					p.setString(base++, randomData[rndPos++]);
					p.setString(base++, randomData[rndPos++]);
					p.setString(base++, randomData[rndPos++]);
					p.setString(base++, randomData[rndPos++]);
					p.setString(base++, randomData[rndPos++]);
				}
			});

			state.iterationsDone--;
		});

		exitMonitoring = true;
	}

	private static long getStartId(long shardNum) {
		Long maxId = selectOne("select max(val) from monitoring_insert");

		//We subtract 20 million to avoid missing keys
		return maxId != null ? maxId - 20000000L : 0L;
	}

}
