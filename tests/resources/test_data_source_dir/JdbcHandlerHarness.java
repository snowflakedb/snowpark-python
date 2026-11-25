/*
 * Copyright (c) 2012-2026 Snowflake Computing Inc. All rights reserved.
 */
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

public class JdbcHandlerHarness {
    static final List<Resource> resources = new ArrayList<>();
    static String failure;

    static class Resource implements InvocationHandler {
        final String kind;
        final Resource parent;
        boolean closed;
        int rows;
        Resource(String kind, Resource parent) {
            this.kind = kind;
            this.parent = parent;
            resources.add(this);
        }
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if (name.equals("equals")) return proxy == args[0];
            if (name.equals("hashCode")) return System.identityHashCode(proxy);
            if (name.equals("toString")) return kind;
            if (name.equals("isClosed")) return closed;
            if (name.equals("close")) {
                closed = true;
                // JDBC closes dependent resources when a parent is closed.
                for (Resource child : resources) {
                    if (child.parent == this) {
                        child.closed = true;
                        for (Resource grandchild : resources) {
                            if (grandchild.parent == child) grandchild.closed = true;
                        }
                    }
                }
                if ("close".equals(failure)) throw new SQLException("close failure");
                return null;
            }
            if (closed || (parent != null && parent.closed)) throw new SQLException("closed resource");
            if (name.equals(failure)) throw new SQLException("SYNTHETIC_CREDENTIAL expected failure: " + name);
            if (name.equals("createStatement")) return resource(Statement.class, "statement", this);
            if (name.equals("executeQuery")) return resource(ResultSet.class, "result", this);
            if (name.equals("next")) return rows++ < 2;
            if (name.equals("getString")) return "value";
            if (method.getReturnType() == boolean.class) return false;
            if (method.getReturnType() == int.class) return 0;
            return null;
        }
    }

    static <T> T resource(Class<T> type, String kind, Resource parent) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, new Resource(kind, parent)));
    }

    public static Connection connect() throws SQLException {
        if ("connect".equals(failure)) throw new SQLException("expected failure: connect");
        return resource(Connection.class, "connection", null);
    }

    static void assertClosed() {
        for (Resource resource : resources) {
            if (!resource.closed) {
                throw new AssertionError("leaked " + resource.kind);
            }
        }
    }

    static void assertSafeError(RuntimeException error) {
        if (!"Ingestion error: JDBC read failed".equals(error.getMessage())
                || error.getCause() != null || error.getSuppressed().length != 0) {
            throw new AssertionError("unsafe source error", error);
        }
    }

    public static void main(String[] args) {
        String scenario = args[0];
        failure = args.length > 1 ? args[1] : "";
        DataLoader loader = new DataLoader();
        if (scenario.equals("error")) {
            try {
                loader.process("query").count();
                throw new AssertionError("expected source failure");
            } catch (RuntimeException expected) {
                assertSafeError(expected);
                // endPartition is not guaranteed after a handler error.
                assertClosed();
            }
        } else if (scenario.equals("partial")) {
            try (Stream<OutputRow> stream = loader.process("query")) {
                if (!stream.findFirst().isPresent()) throw new AssertionError("missing row");
            }
            assertClosed();
        } else if (scenario.equals("end")) {
            Stream<OutputRow> stream = loader.process("query");
            stream.findFirst();
            loader.endPartition().count();
            assertClosed();
            stream.close();
        } else if (scenario.equals("interleaved") || scenario.equals("interleaved-error")) {
            Stream<OutputRow> first = loader.process("query1");
            Stream<OutputRow> second = loader.process("query2");
            Iterator<OutputRow> firstRows = first.iterator();
            Iterator<OutputRow> secondRows = second.iterator();
            firstRows.next();
            secondRows.next();
            if (scenario.equals("interleaved-error")) {
                failure = "next";
                try {
                    firstRows.next();
                    throw new AssertionError("expected first-stream failure");
                } catch (RuntimeException expected) {
                    assertSafeError(expected);
                    failure = "";
                }
            }
            first.close();
            int remaining = 0;
            while (secondRows.hasNext()) { secondRows.next(); remaining++; }
            if (remaining != 1) throw new AssertionError("interleaved stream lost rows");
            second.close();
            loader.endPartition().count();
            assertClosed();
        } else if (scenario.equals("exhaust")) {
            try (Stream<OutputRow> stream = loader.process("query")) {
                if (stream.count() != 2) throw new AssertionError("wrong row count");
            }
            loader.endPartition().count();
            assertClosed();
        } else if (scenario.equals("repeat")) {
            for (int call = 0; call < 3; call++) {
                if (loader.process("query").count() != 2) throw new AssertionError("wrong row count");
            }
            loader.endPartition().count();
            assertClosed();
        } else {
            throw new AssertionError("unknown scenario " + scenario);
        }
    }
}
