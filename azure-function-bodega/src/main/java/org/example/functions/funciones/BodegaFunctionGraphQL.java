package org.example.functions.funciones;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLContext;
import graphql.Scalars;
import graphql.schema.*;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class BodegaFunctionGraphQL {
    private static GraphQL graphQL;

    // ---------- Helpers ----------
    private static Logger safeLogger(Object o) {
        if (o instanceof Logger l) return l;
        return Logger.getLogger("GraphQL");
    }
    private static long toId(Object raw) {
        if (raw instanceof Number n) return n.longValue();
        if (raw instanceof char[] ca) return Long.parseLong(new String(ca));
        return Long.parseLong(String.valueOf(raw));
    }
    private static String trimOrNull(Object o) {
        if (o == null) return null;
        String s = String.valueOf(o).trim();
        return s.isEmpty() ? null : s;
    }

    static {
        // ===== Tipo y Input =====
        GraphQLObjectType bodegaType = GraphQLObjectType.newObject()
                .name("Bodega")
                .field(f -> f.name("id").type(Scalars.GraphQLID))
                .field(f -> f.name("nombre").type(Scalars.GraphQLString))
                .field(f -> f.name("direccion").type(Scalars.GraphQLString))
                .build();

        GraphQLInputObjectType bodegaInput = GraphQLInputObjectType.newInputObject()
                .name("BodegaInput")
                .field(f -> f.name("nombre").type(new GraphQLNonNull(Scalars.GraphQLString)))
                .field(f -> f.name("direccion").type(new GraphQLNonNull(Scalars.GraphQLString)))
                .build();

        // ===== DataFetchers (Query) =====
        DataFetcher<Map<String, Object>> bodegaByIdFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            long id = toId(env.getArgument("id"));
            log.info("bodegaById id=" + id);
            return fetchBodegaById(id, log);
        };

        DataFetcher<List<Map<String, Object>>> bodegasFetcher = env ->
                fetchAllBodegas(safeLogger(env.getGraphQlContext().get("logger")));

        // ===== DataFetchers (Mutations) =====
        DataFetcher<Map<String, Object>> crearBodegaFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            Map<String, Object> input = env.getArgument("input");
            String nombre = trimOrNull(input.get("nombre"));
            String direccion = trimOrNull(input.get("direccion"));
            if (nombre == null || nombre.isBlank() || direccion == null || direccion.isBlank()) {
                throw new IllegalArgumentException("nombre y direccion son obligatorios");
            }
            long newId = insertBodega(nombre, direccion, log);
            return fetchBodegaById(newId, log);
        };

        DataFetcher<Map<String, Object>> actualizarBodegaFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            long id = toId(env.getArgument("id"));
            Map<String, Object> input = env.getArgument("input");
            String nombre = trimOrNull(input.get("nombre"));        // null => no actualizar
            String direccion = trimOrNull(input.get("direccion"));
            boolean ok = updateBodega(id, nombre, direccion, log);
            if (!ok) return null; // si no existe, null
            return fetchBodegaById(id, log);
        };

        DataFetcher<Boolean> eliminarBodegaFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            long id = toId(env.getArgument("id"));
            return deleteBodega(id, log);
        };

        // ===== Query & Mutation =====
        GraphQLObjectType queryType = GraphQLObjectType.newObject()
                .name("Query")
                .field(f -> f
                        .name("bodega")
                        .type(bodegaType)
                        .argument(GraphQLArgument.newArgument().name("id").type(new GraphQLNonNull(Scalars.GraphQLID)))
                        .dataFetcher(bodegaByIdFetcher))
                .field(f -> f
                        .name("bodegas")
                        .type(GraphQLList.list(bodegaType))
                        .dataFetcher(bodegasFetcher))
                .build();

        GraphQLObjectType mutationType = GraphQLObjectType.newObject()
                .name("Mutation")
                .field(f -> f
                        .name("crearBodega")
                        .type(bodegaType)
                        .argument(GraphQLArgument.newArgument().name("input").type(new GraphQLNonNull(bodegaInput)))
                        .dataFetcher(crearBodegaFetcher))
                .field(f -> f
                        .name("actualizarBodega")
                        .type(bodegaType)
                        .argument(GraphQLArgument.newArgument().name("id").type(new GraphQLNonNull(Scalars.GraphQLID)))
                        .argument(GraphQLArgument.newArgument().name("input").type(new GraphQLNonNull(bodegaInput)))
                        .dataFetcher(actualizarBodegaFetcher))
                .field(f -> f
                        .name("eliminarBodega")
                        .type(Scalars.GraphQLBoolean)
                        .argument(GraphQLArgument.newArgument().name("id").type(new GraphQLNonNull(Scalars.GraphQLID)))
                        .dataFetcher(eliminarBodegaFetcher))
                .build();

        GraphQLSchema schema = GraphQLSchema.newSchema()
                .query(queryType)
                .mutation(mutationType)
                .build();

        graphQL = GraphQL.newGraphQL(schema).build();
    }

    @FunctionName("graphqlBodegas")
    public HttpResponseMessage handleGraphQL(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.POST, HttpMethod.GET},
                    route = "graphql/bodegas",
                    authLevel = AuthorizationLevel.ANONYMOUS
            ) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context
    ) {
        Logger log = context.getLogger();

        String query = null;
        Map<String, Object> variables = new HashMap<>();

        try {
            if (request.getHttpMethod() == HttpMethod.POST) {
                String body = request.getBody().orElse("");
                if (body != null && !body.isBlank()) {
                    Map<String, Object> parsed = SimpleJson.parseJsonObject(body);
                    Object q = parsed.get("query");
                    if (q != null) query = String.valueOf(q);
                    Object vars = parsed.get("variables");
                    if (vars instanceof Map) variables = (Map<String, Object>) vars;
                    else if (vars instanceof String && !String.valueOf(vars).isBlank()) {
                        Object obj = SimpleJson.parse(String.valueOf(vars));
                        if (obj instanceof Map) variables = (Map<String, Object>) obj;
                    }
                }
            } else {
                query = request.getQueryParameters().get("query");
                String varsStr = request.getQueryParameters().get("variables");
                if (varsStr != null && !varsStr.isBlank()) {
                    Object obj = SimpleJson.parse(varsStr);
                    if (obj instanceof Map) variables = (Map<String, Object>) obj;
                }
            }
        } catch (Exception e) {
            log.severe("Error parseando request GraphQL: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body(Map.of("errors", List.of(Map.of("message", "Invalid request payload"))))
                    .header("Content-Type", "application/json")
                    .build();
        }

        if (query == null || query.isBlank()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body(Map.of("errors", List.of(Map.of("message", "Missing 'query'"))))
                    .header("Content-Type", "application/json")
                    .build();
        }

        GraphQLContext gqlCtx = GraphQLContext.newContext()
                .of("logger", log)
                .build();

        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                .query(query)
                .variables(variables)
                .context(gqlCtx)
                .build();

        ExecutionResult result = graphQL.execute(executionInput);
        Map<String, Object> spec = result.toSpecification();

        return request.createResponseBuilder(HttpStatus.OK)
                .body(spec)
                .header("Content-Type", "application/json")
                .build();
    }

    // ===================== JDBC Helpers =====================

    private static Map<String, Object> fetchBodegaById(long id, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        try (Connection conn = open(); PreparedStatement ps = conn.prepareStatement(
                "SELECT ID, NOMBRE, DIRECCION FROM BODEGA WHERE ID=?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                Map<String, Object> b = new HashMap<>();

                Object idObj = rs.getObject("ID");
                if (idObj instanceof java.math.BigDecimal bd) b.put("id", bd.longValue());
                else if (idObj instanceof Number n)           b.put("id", n.longValue());
                else                                          b.put("id", Long.parseLong(String.valueOf(idObj)));

                Object nombreObj = rs.getObject("NOMBRE");
                b.put("nombre", nombreObj instanceof java.sql.Clob c ? c.getSubString(1, (int) c.length()) : rs.getString("NOMBRE"));

                Object dirObj = rs.getObject("DIRECCION");
                b.put("direccion", dirObj instanceof java.sql.Clob c ? c.getSubString(1, (int) c.length()) : rs.getString("DIRECCION"));

                return b;
            }
        }
    }

    private static List<Map<String, Object>> fetchAllBodegas(Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        try (Connection conn = open(); PreparedStatement ps = conn.prepareStatement(
                "SELECT ID, NOMBRE, DIRECCION FROM BODEGA ORDER BY ID");
             ResultSet rs = ps.executeQuery()) {
            List<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> b = new HashMap<>();

                Object idObj = rs.getObject("ID");
                if (idObj instanceof java.math.BigDecimal bd) b.put("id", bd.longValue());
                else if (idObj instanceof Number n)           b.put("id", n.longValue());
                else                                          b.put("id", Long.parseLong(String.valueOf(idObj)));

                Object nombreObj = rs.getObject("NOMBRE");
                b.put("nombre", nombreObj instanceof java.sql.Clob c ? c.getSubString(1, (int) c.length()) : rs.getString("NOMBRE"));

                Object dirObj = rs.getObject("DIRECCION");
                b.put("direccion", dirObj instanceof java.sql.Clob c ? c.getSubString(1, (int) c.length()) : rs.getString("DIRECCION"));

                list.add(b);
            }
            return list;
        }
    }

    private static long insertBodega(String nombre, String direccion, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        String sql = "INSERT INTO BODEGA (NOMBRE, DIRECCION) VALUES (?, ?)";
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(sql, new String[] { "ID" })) {
            ps.setString(1, nombre);
            ps.setString(2, direccion);
            ps.executeUpdate();

            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys != null && keys.next()) {
                    java.math.BigDecimal k = keys.getBigDecimal(1);
                    if (k != null) return k.longValue();
                }
            }
        }

        // Fallback si no hay getGeneratedKeys
        try (Connection conn = open(); PreparedStatement ps = conn.prepareStatement(
                "SELECT ID FROM BODEGA WHERE NOMBRE=? AND DIRECCION=? " +
                        "ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY")) {
            ps.setString(1, nombre);
            ps.setString(2, direccion);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
            }
        }
        throw new SQLException("No fue posible obtener el ID generado");
    }

    private static boolean updateBodega(long id, String nombre, String direccion, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        List<String> sets = new ArrayList<>();
        if (nombre != null) sets.add("NOMBRE=?");
        if (direccion != null) sets.add("DIRECCION=?");
        if (sets.isEmpty()) return true;

        String sql = "UPDATE BODEGA SET " + String.join(", ", sets) + " WHERE ID=?";
        try (Connection conn = open(); PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            if (nombre != null) ps.setString(idx++, nombre);
            if (direccion != null) ps.setString(idx++, direccion);
            ps.setLong(idx, id);
            int rows = ps.executeUpdate();
            return rows > 0;
        }
    }

    private static boolean deleteBodega(long id, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        try (Connection conn = open(); PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM BODEGA WHERE ID=?")) {
            ps.setLong(1, id);
            return ps.executeUpdate() > 0;
        }
    }

    private static Connection open() throws SQLException {
        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        if (walletPath == null || walletPath.isBlank()) {
            walletPath = "/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC";
        }
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";
        return DriverManager.getConnection(url, user, pass);
    }

    static class SimpleJson {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @SuppressWarnings("unchecked")
        static Map<String, Object> parseJsonObject(String json) throws Exception {
            if (json == null || json.isBlank()) return new HashMap<>();
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
        }

        static Object parse(String value) throws Exception {
            if (value == null) return null;
            String maybeDecoded = value;
            if (value.contains("%7B") || value.contains("%22") || value.contains("%5B")) {
                maybeDecoded = URLDecoder.decode(value, StandardCharsets.UTF_8);
            }
            try { return MAPPER.readValue(maybeDecoded, new TypeReference<Map<String, Object>>() {}); }
            catch (Exception ignore) { }
            try { return MAPPER.readValue(maybeDecoded, new TypeReference<List<Object>>() {}); }
            catch (Exception ignore) { }
            return maybeDecoded;
        }
    }
}