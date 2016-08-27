package net.thisptr.flume.reporter.influxdb;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.MonitorService;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class InfluxDBReporter implements MonitorService {
	private static final Logger LOG = LoggerFactory.getLogger(InfluxDBReporter.class);
	private static final MBeanServer MBEANS = ManagementFactory.getPlatformMBeanServer();

	public static final int DEFAULT_INTERVAL = 30;
	public static final int DEFAULT_PORT = 8086;
	public static final String DEFAULT_USER = "root";
	public static final String DEFAULT_PASSWORD = "root";

	private volatile List<HostAndPort> servers;
	private volatile int interval;
	private volatile String database;
	private volatile String user;
	private volatile String password;
	private volatile Map<String, String> tags;
	private volatile String retention;

	private static enum State {
		STARTING, RUNNING, STOPPING, STOPPED
	}

	private final AtomicReference<State> state = new AtomicReference<State>(State.STOPPED);

	private volatile ScheduledExecutorService executor;

	@Override
	public void configure(final Context context) {
		Preconditions.checkState(state.get() == State.STOPPED, "InfluxDB reporter is expected to be in STOPPED state.");

		final String servers = context.getString("servers");
		if (Strings.isNullOrEmpty(servers))
			throw new ConfigurationException("servers must not be null or empty.");
		final List<HostAndPort> hostAndPorts = StreamSupport.stream(Splitter.on(",").trimResults().split(servers).spliterator(), false)
				.map(server -> HostAndPort.fromString(server).withDefaultPort(DEFAULT_PORT))
				.collect(Collectors.toList());

		final int interval = context.getInteger("interval", DEFAULT_INTERVAL);
		if (interval <= 0)
			throw new ConfigurationException("interval must be positive.");

		final String database = context.getString("database");
		if (Strings.isNullOrEmpty(database))
			throw new ConfigurationException("database must not be null or empty.");

		final String user = context.getString("user", DEFAULT_USER);
		if (Strings.isNullOrEmpty(user))
			throw new ConfigurationException("user must not be null or empty.");

		final String password = context.getString("password", DEFAULT_PASSWORD);
		final String retention = context.getString("retention");

		final Map<String, String> tags = context.getSubProperties("tags.");

		// update the fields at last
		this.servers = hostAndPorts;
		this.interval = interval;
		this.database = database;
		this.user = user;
		this.password = password;
		this.tags = tags;
		this.retention = retention;
	}

	@Override
	public void start() {
		if (!state.compareAndSet(State.STOPPED, State.STARTING))
			throw new IllegalStateException("InfluxDB reporter is expected to be in STOPPED state.");

		LOG.info("Starting InfluxDB reporter...");
		try {
			final List<InfluxDB> conns = new ArrayList<>(servers.size());
			for (final HostAndPort server : servers) {
				try {
					final InfluxDB conn = InfluxDBFactory.connect("http://" + server, user, password);
					conn.createDatabase(database);
					conns.add(conn);
				} catch (Exception e) {
					LOG.warn("Failed to create a database " + database + " on {}.", server, e);
				}
			}

			// scheduler
			executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("InfluxDB Reporter %d").build());
			executor.scheduleWithFixedDelay(new SendMetricsCommand(conns, database, tags, retention), 0, interval, TimeUnit.SECONDS);
			state.set(State.RUNNING);
		} catch (final Throwable th) {
			LOG.error("Failed to start InfluxDB reporter.", th);
			state.set(State.STOPPED);
			throw th;
		}
	}

	@Override
	public void stop() {
		if (!state.compareAndSet(State.RUNNING, State.STOPPING))
			throw new IllegalStateException("InfluxDB reporter is expected to be in RUNNING state.");

		LOG.info("Stopping InfluxDB reporter...");
		try {
			executor.shutdown();
			while (!executor.isTerminated()) {
				try {
					LOG.info("Waiting for InfluxDB reporter to stop...");
					executor.awaitTermination(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					LOG.warn("Interrupted while waiting for InfluxDB reporter to shutdown.", e);
					executor.shutdownNow();
				}
			}
		} finally {
			state.set(State.STOPPED);
		}
	}

	private static class SendMetricsCommand implements Runnable {
		private final List<InfluxDB> conns;
		private final String database;
		private final Map<String, String> tags;
		private final String retention;

		public SendMetricsCommand(final List<InfluxDB> conns, final String database, final Map<String, String> tags, final String retention) {
			this.conns = conns;
			this.database = database;
			this.tags = tags;
			this.retention = retention;
		}

		public static enum SupportedType {
			NUMBER(Number.class, (builder, name, value) -> {
				builder.addField(name, (Number) value);
				return 1;
			}),
			STRING(String.class, (builder, name, value) -> {
				builder.addField(name, (String) value);
				return 1;
			}),
			DOUBLE(Double.class, (builder, name, value) -> {
				builder.addField(name, ((Double) value).doubleValue());
				return 1;
			}),
			FLOAT(Float.class, (builder, name, value) -> {
				builder.addField(name, ((Float) value).doubleValue());
				return 1;
			}),
			LONG(Long.class, (builder, name, value) -> {
				builder.addField(name, ((Long) value).longValue());
				return 1;
			}),
			INTEGER(Integer.class, (builder, name, value) -> {
				builder.addField(name, ((Integer) value).longValue());
				return 1;
			}),
			SHORT(Short.class, (builder, name, value) -> {
				builder.addField(name, ((Short) value).longValue());
				return 1;
			}),
			BYTE(Byte.class, (builder, name, value) -> {
				builder.addField(name, ((Byte) value).longValue());
				return 1;
			}),
			BOOLEAN(Boolean.class, (builder, name, value) -> {
				builder.addField(name, ((Boolean) value).booleanValue());
				return 1;
			}),
			CHARACTER(Character.class, (builder, name, value) -> {
				builder.addField(name, ((Character) value).toString());
				return 1;
			}),
			COMPOSITE(CompositeData.class, (builder, name, composite) -> {
				int fields = 0;
				final CompositeData data = (CompositeData) composite;
				for (final String key : data.getCompositeType().keySet()) {
					final Object value = data.get(key);
					if (value == null)
						continue;
					SupportedType stype = SupportedType.find(value.getClass());
					if (stype == null) {
						LOG.debug("Composite {} contains value of unsupported type {}.", name + "." + key, value.getClass().getName());
						continue;
					}
					fields += stype.action.add(builder, name + "." + key, value);
				}
				return fields;
			});

			public interface Action {
				int add(final Point.Builder builder, final String name, final Object value);
			}

			public final Class<?> clazz;
			public final Action action;

			private SupportedType(final Class<?> clazz, Action action) {
				this.clazz = clazz;
				this.action = action;
			}

			public static SupportedType find(final Class<?> clazz) {
				for (final SupportedType type : SupportedType.values()) {
					if (type.clazz.isAssignableFrom(clazz))
						return type;
				}
				return null;
			}

			public static SupportedType find(final String className) {
				switch (className) {
					case "double":
						return DOUBLE;
					case "float":
						return FLOAT;
					case "long":
						return LONG;
					case "short":
						return SHORT;
					case "int":
						return INTEGER;
					case "byte":
						return BYTE;
					case "boolean":
						return BOOLEAN;
					case "char":
						return CHARACTER;
				}
				try {
					final Class<?> clazz = Class.forName(className);
					return find(clazz);
				} catch (ClassNotFoundException e) {
					return null;
				}
			}
		}

		@Override
		public void run() {
			LOG.debug("Collecting metrics to InfluxDB...");
			final long startTime = System.currentTimeMillis();
			try {
				final BatchPoints.Builder batchPoints = BatchPoints.database(database);
				if (!Strings.isNullOrEmpty(retention))
					batchPoints.retentionPolicy(retention);
				tags.forEach((name, value) -> {
					if (Strings.isNullOrEmpty(value)) {
						LOG.debug("Omitted a tag {}, as its value is empty.", name);
						return;
					}
					batchPoints.tag(name, value);
				});
				int batchPointSize = 0;

				for (final ObjectInstance instance : MBEANS.queryMBeans(null, null)) {
					final ObjectName oname = instance.getObjectName();

					final Point.Builder point = Point.measurement(oname.getDomain())
							.tag(oname.getKeyPropertyList());

					int fields = 0;
					for (final MBeanAttributeInfo attr : MBEANS.getMBeanInfo(oname).getAttributes()) {
						final SupportedType type = SupportedType.find(attr.getType());
						if (type == null) {
							LOG.debug("Unsupported attribute type {} in {}", attr.getType(), oname);
							continue;
						}

						final Object value;
						try {
							value = MBEANS.getAttribute(oname, attr.getName());
							if (value == null)
								continue;
						} catch (Exception e) {
							LOG.debug("Failed to get value of the attribute " + attr.getName() + " in " + oname, e);
							continue;
						}
						fields += type.action.add(point, attr.getName(), value);
					}

					// Skip points with no field. InfluxDB cannot ingest such data.
					if (fields == 0)
						continue;

					batchPoints.point(point.build());
					++batchPointSize;
				}

				for (final InfluxDB conn : conns) {
					try {
						conn.write(batchPoints.build());
					} catch (Exception e) {
						LOG.warn("Sending metrics to {} failed.", conn, e);
					}
				}

				LOG.debug("Collected and sent {} batch points to InfluxDB in {} ms.", batchPointSize, System.currentTimeMillis() - startTime);
			} catch (Throwable th) {
				LOG.error("Unknown error", th);
			}
		}
	}
}
