import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

// Executes the generated handler against JDBC proxies, including overlapping streams.
public class JdbcLifecycle {
    static final List<String> events = new ArrayList<>();
    static int connections;
    static String failure = "";

    static Object defaultValue(Class<?> type) {
        if (type == boolean.class) return false;
        if (type == int.class) return 0;
        return null;
    }

    static Connection connect() {
        final int id = ++connections;
        events.add(id + ":connect");
        if (failure.equals("connect")) throw new RuntimeException("connect failure");
        final boolean[] autoCommit = {true};
        return (Connection) Proxy.newProxyInstance(JdbcLifecycle.class.getClassLoader(),
            new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                String name = method.getName();
                if (name.equals("createStatement")) {
                    if (failure.equals("statement")) throw new SQLException("statement failure");
                    return statement(id, autoCommit);
                }
                if (name.equals("setAutoCommit")) {
                    events.add(id + ":autocommit:" + args[0]);
                    if (failure.equals("autocommit")) throw new SQLException("autocommit failure");
                    autoCommit[0] = (Boolean) args[0];
                }
                if (name.equals("rollback") || name.equals("close")) {
                    events.add(id + ":" + name);
                    if (failure.equals(name)) throw new SQLException(name + " failure");
                }
                return defaultValue(method.getReturnType());
            });
    }

    static Statement statement(int id, boolean[] autoCommit) {
        return (Statement) Proxy.newProxyInstance(JdbcLifecycle.class.getClassLoader(),
            new Class<?>[]{Statement.class}, (proxy, method, args) -> {
                String name = method.getName();
                if (name.equals("execute")) {
                    check(autoCommit[0], "initialization must retain autocommit");
                    events.add(id + ":init");
                    if (failure.equals("init")) throw new SQLException("init failure");
                }
                if (name.equals("executeQuery")) {
                    events.add(id + ":query");
                    if (failure.equals("query")) throw new SQLException("query failure");
                    return resultSet(id);
                }
                if (name.equals("close")) events.add(id + ":statement.close");
                return defaultValue(method.getReturnType());
            });
    }

    static ResultSet resultSet(int id) {
        final int[] count = {0};
        return (ResultSet) Proxy.newProxyInstance(JdbcLifecycle.class.getClassLoader(),
            new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
                String name = method.getName();
                if (name.equals("next")) {
                    if (failure.equals("next")) throw new SQLException("next failure");
                    return count[0]++ < 3;
                }
                if (name.equals("getString")) {
                    if (failure.equals("read")) throw new IllegalArgumentException("read failure");
                    return "value";
                }
                if (name.equals("close")) events.add(id + ":result.close");
                return defaultValue(method.getReturnType());
            });
    }

    static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message + ": " + events);
    }

    static long occurrences(String event) {
        return events.stream().filter(event::equals).count();
    }

    public static void main(String[] args) {
        boolean streaming = Integer.parseInt(args[0]) > 0;
        DataLoader loader = new DataLoader();
        Stream<OutputRow> first = loader.process("first");
        check(connections == 0, "process must remain lazy");
        Iterator<OutputRow> iterator = first.iterator();
        check(iterator.hasNext(), "first stream has rows");
        iterator.next();
        Stream<OutputRow> second = loader.process("second");
        check(second.count() == 3, "second stream complete");
        check(occurrences("1:close") == 0, "second completion must not close first connection");
        first.close();
        loader.endPartition();
        loader.endPartition();
        check(connections == (streaming ? 2 : 1), "connection ownership");
        check(occurrences("1:close") == 1, "idempotent close");
        if (streaming) {
            check(occurrences("1:rollback") == 1 && occurrences("2:rollback") == 1, "transactions ended");
            check(events.indexOf("1:result.close") < events.indexOf("1:rollback"), "result closes before rollback");
        }
        for (String scenario : Arrays.asList("", "connect", "statement", "init", "autocommit", "query", "next", "read", "rollback", "close")) {
            if (!streaming && (scenario.equals("autocommit") || scenario.equals("rollback"))) continue;
            events.clear();
            connections = 0;
            failure = scenario;
            loader = new DataLoader();
            Stream<OutputRow> stream = loader.process("query");
            boolean failed = false;
            try {
                stream.limit(1).count();
            } catch (RuntimeException expected) {
                failed = true;
            } finally {
                // endPartition must clean up even if the consumer does not close its stream.
                loader.endPartition();
                stream.close();
            }
            boolean expectedFailure = !Arrays.asList("", "rollback", "close").contains(scenario);
            check(failed == expectedFailure, "failure propagation: " + scenario);
            if (!scenario.equals("connect")) check(occurrences("1:close") == 1, "connection cleanup: " + scenario);
            if (streaming && Arrays.asList("", "query", "next", "read", "rollback", "close").contains(scenario)) {
                check(occurrences("1:rollback") == 1, "rollback cleanup: " + scenario);
            }
        }
    }
}
