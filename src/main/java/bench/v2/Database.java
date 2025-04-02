package bench.v2;

import bench.V2;
import bench.v2.strategy.IDistributionStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static bench.V2.DEFPGPORT;


public class Database {
	private static final Logger log = LoggerFactory.getLogger(Database.class);

	public static Boolean pooling = true;

	private final ThreadLocal<Connection> conn = new ThreadLocal<>();

	public volatile List<DataSource> ds;

	private volatile IDistributionStrategy currentStrategy;

	public String[] hosts;
	public int[] ports;
	public String dbName;
	public String userName;
	public String passwd;
	public int poolSize;
	public long maxLifeTime;

	public Database(String host,
					String ports,
					String dbName,
					String userName,
					String passwd,
					int poolSize,
					Long maxLifeTime) {
		this.hosts = host.split(",");
		this.ports =  getPort(ports);
		this.dbName = dbName;
		this.userName = userName;
		this.passwd = passwd;

		this.maxLifeTime = maxLifeTime;
		this.poolSize = poolSize;
	}

	public DataSource getDataSource(DataContext ctx) {
		if (ds == null) {
			synchronized (this) {
				if (ds == null) {
					ds = new ArrayList<>(hosts.length);
					if (pooling) {
						for (int i = 0; i < hosts.length; i++) {
							HikariConfig config = new HikariConfig();
							config.setJdbcUrl("jdbc:postgresql://" + hosts[i] + ":" + ports[i] + "/" + dbName + "?prepareThreshold=1&binaryTransfer=false");
							config.setUsername(userName);
							config.setPassword(passwd);
							config.setAutoCommit(V2.autoCommit);
							config.addDataSourceProperty("cachePrepStmts", "true");
							config.addDataSourceProperty("prepStmtCacheSize", "250");
							config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
							config.setInitializationFailTimeout(1);
							config.setConnectionTimeout(300000);
							config.setConnectionInitSql("set search_path=dev,public");
							config.setMaximumPoolSize(poolSize);
							config.setMaxLifetime(maxLifeTime);
							ds.add(new HikariDataSource(config));
						}

					} else {
						for (int i = 0; i < hosts.length; i++) {
							PGSimpleDataSource pgds = new PGSimpleDataSource();
							pgds.setServerNames(new String[]{hosts[i]});
							pgds.setDatabaseName(dbName);
							pgds.setUser(userName);
							pgds.setPassword(passwd);
							pgds.setPortNumbers(new int[]{ports[i]});
							pgds.setConnectTimeout(1000);
							ds.add(pgds);
						}
					}
				}
			}
		}

		if (currentStrategy == null) {
			synchronized (this) {
				if (currentStrategy == null) {
					IDistributionStrategy x = V2.params.strategy.getStrategy();
					x.init(this);
					currentStrategy = x;
				}
			}
		}

		return ds.get(currentStrategy.getDataSourceID(ctx));
	}

	public DataSource getDataSource() {
		return getDataSource(null);
	}

	public interface CallableStatement<V> {
		/**
		 * Computes a result, or throws an exception if unable to do so.
		 *
		 * @return computed result
		 * @throws Exception if unable to compute a result
		 */
		V call(Connection conn) throws Exception;
	}

	public void push(Connection conn) {
		this.conn.set(conn);
	}

	public void pop() {
		this.conn.set(null);
	}

	public Connection get() {
		return this.conn.get();
	}

	public <V> V execute(CallableStatement<V> c) {
		return execute(c, null);
	}

	public <V> V execute(CallableStatement<V> c, Callable<Void> handlerOnError) {
		try {
			if (conn.get() == null) {
				try (Connection localConn = getDataSource().getConnection()) {
					return c.call(localConn);
				}
			} else {
				return c.call(conn.get());
			}
		} catch (SQLException e) {

			log.info("{}", e.getMessage());
			if (e.getMessage().contains("could not serialize access due to concurrent update") == true)
				return null;
			if (handlerOnError != null)
				try {
					handlerOnError.call();
					return null;
				} catch (Exception x) {
					throw new RuntimeException("Exception occured during error handling...", x);
				}
			else {
				throw new RuntimeException("SQL exception occured during DB...", e);
			}
		} catch (Exception e) {
			log.info("------------exception ...{}-----------------", e.getMessage());
			//log.error("Unknown exception occured during connecting...", e);s
			throw new RuntimeException("Exception occured during DB...", e);
		}

	}


	public interface CallableResultSet<V> {
		/**
		 * Computes a result, or throws an exception if unable to do so.
		 *
		 * @return computed result
		 * @throws Exception if unable to compute a result
		 */
		V call(ResultSet rs) throws Exception;
	}

	public interface VoidResultSet {
		/**
		 * Computes a result, or throws an exception if unable to do so.
		 *
		 * @return computed result
		 * @throws Exception if unable to compute a result
		 */
		void call(ResultSet rs) throws Exception;
	}


	public boolean insert(String sql, Object... binds) {
		return execute((conn) -> {
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				for (int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}

				return pstmt.execute();
			}
		});
	}

	public <V> List<V> list(CallableResultSet<V> rsHandler,
							String sql,
							Object... binds) {
		return execute((conn) -> {
			List<V> res = new ArrayList<V>();
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				for (int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}

				try (ResultSet rs = pstmt.executeQuery()) {
					while (rs.next()) {
						V e = rsHandler.call(rs);
						if (e != null)
							res.add(e);
					}
				}
			}
			return res;
		});
	}

	public boolean selectSingle(VoidResultSet rsHandler,
								String sql,
								Object... binds) {
		return execute((conn) -> {
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				for (int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}

				try (ResultSet rs = pstmt.executeQuery()) {
					if (rs.next()) {
						rsHandler.call(rs);
						return true;
					}
				}
			}
			return false;
		});
	}

	public interface CallableMapResultSet<K, V> {
		/**
		 * Computes a result, or throws an exception if unable to do so.
		 *
		 * @return computed result
		 * @throws Exception if unable to compute a result
		 */
		void call(ResultSet rs, Map<K, V> map) throws Exception;
	}

	public <K, V> Map<K, V> map(CallableMapResultSet<K, V> rsHandler,
								String sql,
								Object... binds) {
		return execute((conn) -> {
			Map<K, V> map = new HashMap<>();
			try (PreparedStatement pstmt = conn.prepareStatement(sql);) {
				for (int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}

				try (ResultSet rs = pstmt.executeQuery()) {
					while (rs.next()) {
						rsHandler.call(rs, map);
					}
				}
			}
			return map;
		});
	}

	private int[] getPort(String ports) {
		int[] port = new int[hosts.length];
		String[] strPort = ports.split(",");

		//For old version pg_microbench
		if (strPort.length == 1) {
			for (int i = 0; i < hosts.length; i++)
				port[i] = Integer.parseInt(strPort[0]);

			return port;
		}

		for (int i = 0; i < hosts.length; i++)
			if (i < strPort.length)
				port[i] = Integer.parseInt(strPort[i]);
			else
				port[i] = Integer.parseInt(DEFPGPORT);

		return port;
	}
}
