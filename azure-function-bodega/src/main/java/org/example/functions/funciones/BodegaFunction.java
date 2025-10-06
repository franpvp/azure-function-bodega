package org.example.functions.funciones;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.messaging.eventgrid.EventGridEvent;
import com.azure.messaging.eventgrid.EventGridPublisherClient;
import com.azure.messaging.eventgrid.EventGridPublisherClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import org.example.functions.dto.BodegaDto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

public class BodegaFunction {

    private static final String ORACLE_WALLET_DIR = "ORACLE_WALLET_DIR";

    @FunctionName("obtenerBodegas")
    public HttpResponseMessage obtenerBodegas(
            @HttpTrigger(
                    name = "reqGetAll",
                    methods = {HttpMethod.GET},
                    authLevel = AuthorizationLevel.ANONYMOUS,
                    route = "bodegas"
            ) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        List<BodegaDto> bodegaDtos = new ArrayList<>();

        try {

            String walletPath = "/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC";
            String walletEnv = System.getenv(ORACLE_WALLET_DIR);
            if (walletEnv != null && !walletEnv.isBlank()) {
                walletPath = walletEnv;
            }

            String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
            String user = "usuario_test";
            String pass = "Usuariotest2025";

            try (Connection conn = DriverManager.getConnection(url, user, pass);
                 PreparedStatement stmt = conn.prepareStatement(
                         "SELECT ID, NOMBRE, DIRECCION FROM BODEGA");
                 ResultSet rs = stmt.executeQuery()) {

                while (rs.next()) {
                    bodegaDtos.add(BodegaDto.builder()
                            .id(rs.getLong("ID"))
                            .nombre(rs.getString("NOMBRE"))
                            .direccion(rs.getString("DIRECCION"))
                            .build());
                }
            }
        } catch (Exception e) {
            context.getLogger().severe("Error consultando Oracle: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error al obtener bodegas: " + e.getMessage())
                    .build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(bodegaDtos)
                .build();
    }

    @FunctionName("getBodegaById")
    public HttpResponseMessage getBodegaById(
            @HttpTrigger(
                    name = "reqGetById",
                    methods = {HttpMethod.GET},
                    route = "bodegas/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        String walletPath = System.getenv(ORACLE_WALLET_DIR);
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        final String path = request.getUri() != null ? request.getUri().getPath() : "/api/bodegas/" + id;

        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM BODEGA WHERE ID = ?")) {

            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    BodegaDto bodegaDto = BodegaDto.builder()
                            .id(rs.getLong("ID"))
                            .nombre(rs.getString("NOMBRE"))
                            .direccion(rs.getString("DIRECCION"))
                            .build();

                    return request.createResponseBuilder(HttpStatus.OK)
                            .header("Content-Type", "application/json")
                            .body(bodegaDto)
                            .build();
                } else {
                    return jsonError(request, HttpStatus.NOT_FOUND, "Not Found",
                            "Bodega no encontrada con ID " + id, path);
                }
            }

        } catch (Exception ex) {
            context.getLogger().severe("Error al obtener bodega: " + ex.getClass().getName());
            return jsonError(request, HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                    "Ocurrió un error al procesar la solicitud.", path);
        }
    }


    @FunctionName("crearBodega")
    public HttpResponseMessage crearBodega(
            @HttpTrigger(
                    name = "reqPost",
                    methods = {HttpMethod.POST},
                    route = "bodegas",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        final Logger log = context.getLogger();

        try {
            String body = request.getBody().orElse("");
            if (body.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .body("El body no puede ser vacío")
                        .build();
            }

            ObjectMapper mapper = new ObjectMapper();
            BodegaDto nuevo = mapper.readValue(body, BodegaDto.class);

            String walletPath = "/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC";
            String walletEnv = System.getenv(ORACLE_WALLET_DIR);

            if (walletEnv != null && !walletEnv.isBlank()) {
                walletPath = walletEnv;
            }
            String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
            String user = "usuario_test";
            String pass = "Usuariotest2025";

            Long newId = null;
            final String sql = "INSERT INTO BODEGA (NOMBRE, DIRECCION) VALUES (?, ?)";

            try (Connection conn = DriverManager.getConnection(url, user, pass);
                 PreparedStatement stmt = conn.prepareStatement(sql, new String[]{"ID"})) {

                stmt.setString(1, nuevo.getNombre());
                stmt.setString(2, nuevo.getDireccion());
                stmt.executeUpdate();

                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    if (keys != null && keys.next()) newId = keys.getLong(1);
                }
            }

            if (newId == null) {
                try (Connection conn = DriverManager.getConnection(url, user, pass);
                     PreparedStatement sel = conn.prepareStatement(
                             "SELECT ID FROM BODEGA WHERE NOMBRE=? AND DIRECCION=? " +
                                     "ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY")) {
                    sel.setString(1, nuevo.getNombre());
                    sel.setString(2, nuevo.getDireccion());
                    try (ResultSet rs = sel.executeQuery()) {
                        if (rs.next()) newId = rs.getLong(1);
                    }
                } catch (Exception ignore) { }
            }

            try {
                final String eventGridTopicEndpoint = "";
                final String eventGridTopicKey      = "";

                if (eventGridTopicEndpoint != null && !eventGridTopicEndpoint.isBlank()
                        && eventGridTopicKey != null && !eventGridTopicKey.isBlank()) {

                    EventGridPublisherClient<EventGridEvent> client = new EventGridPublisherClientBuilder()
                            .endpoint(eventGridTopicEndpoint)
                            .credential(new AzureKeyCredential(eventGridTopicKey))
                            .buildEventGridEventPublisherClient();

                    Map<String, Object> eventData = new LinkedHashMap<>();
                    if (newId != null) eventData.put("id", newId);
                    eventData.put("nombre", nuevo.getNombre());
                    eventData.put("direccion", nuevo.getDireccion());
                    eventData.put("createdAt", OffsetDateTime.now().toString());

                    String subject = (newId != null)
                            ? "/api/bodegas/" + newId
                            : "/api/bodegas";

                    EventGridEvent event = new EventGridEvent(
                            subject,
                            "api.bodega.creada.v1",
                            BinaryData.fromObject(eventData),
                            "1.0"
                    );
                    event.setEventTime(OffsetDateTime.now());
                    client.sendEvent(event);
                } else {
                    log.warning("No se publicó evento: faltan EVENTGRID_TOPIC_ENDPOINT/KEY.");
                }
            } catch (Exception egx) {
                log.severe("Bodega creada pero falló publicar evento: " + egx.getMessage());
            }

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("message", "Bodega creada con éxito");
            if (newId != null) resp.put("id", newId);
            resp.put("bodega", Map.of(
                    "nombre", nuevo.getNombre(),
                    "direccion", nuevo.getDireccion()
            ));

            return request.createResponseBuilder(HttpStatus.CREATED)
                    .header("Content-Type", "application/json")
                    .body(resp)
                    .build();

        } catch (Exception e) {
            context.getLogger().severe("Error creando producto: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error al crear bodega: " + e.getMessage())
                    .build();
        }
    }

    @FunctionName("modificarBodega")
    public HttpResponseMessage modificarBodega(
            @HttpTrigger(
                    name = "reqUpdate",
                    methods = {HttpMethod.PUT},
                    route = "bodegas/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<BodegaDto>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        final String path = request.getUri() != null ? request.getUri().getPath() : "/api/bodegas/" + id;

        if (request.getBody().isEmpty()) {
            return jsonError(request, HttpStatus.BAD_REQUEST, "Bad Request",
                    "Body requerido.", path);
        }
        BodegaDto dto = request.getBody().get();
        if (dto.getNombre() == null || dto.getNombre().isBlank()
                || dto.getDireccion() == null || dto.getDireccion().isBlank()) {
            return jsonError(request, HttpStatus.BAD_REQUEST, "Bad Request",
                    "nombre y direccion son obligatorios.", path);
        }

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            try (PreparedStatement chk = conn.prepareStatement("SELECT 1 FROM BODEGA WHERE ID = ?")) {
                chk.setLong(1, id);
                try (ResultSet rs = chk.executeQuery()) {
                    if (!rs.next()) {
                        return jsonError(request, HttpStatus.NOT_FOUND, "Not Found",
                                "No existe bodega con ID " + id, path);
                    }
                }
            }

            String sql = "UPDATE BODEGA SET NOMBRE = ?, DIRECCION = ? WHERE ID = ?";
            int updated;
            try (PreparedStatement upd = conn.prepareStatement(sql)) {
                upd.setString(1, dto.getNombre());
                upd.setString(2, dto.getDireccion());
                upd.setLong(3, id);
                updated = upd.executeUpdate();
            }
            if (updated == 0) {
                return jsonError(request, HttpStatus.NOT_FOUND, "Not Found",
                        "No se actualizó la bodega con ID " + id, path);
            }

            try {
                final String eventGridTopicEndpoint = "";
                final String eventGridTopicKey      = "";

                if (eventGridTopicEndpoint != null && !eventGridTopicEndpoint.isBlank()
                        && eventGridTopicKey != null && !eventGridTopicKey.isBlank()) {

                    EventGridPublisherClient<EventGridEvent> client = new EventGridPublisherClientBuilder()
                            .endpoint(eventGridTopicEndpoint)
                            .credential(new AzureKeyCredential(eventGridTopicKey))
                            .buildEventGridEventPublisherClient();

                    Map<String, Object> eventData = new LinkedHashMap<>();
                    eventData.put("id", id);
                    eventData.put("nombre", dto.getNombre());
                    eventData.put("direccion", dto.getDireccion());
                    eventData.put("updatedAt", OffsetDateTime.now().toString());

                    EventGridEvent event = new EventGridEvent(
                            path,
                            "api.bodega.actualizada.v1",
                            BinaryData.fromObject(eventData),
                            "1.0"
                    );
                    event.setEventTime(OffsetDateTime.now());

                    client.sendEvent(event);
                } else {
                    context.getLogger().warning("No se publicó evento: faltan EVENTGRID_TOPIC_ENDPOINT/KEY.");
                }
            } catch (Exception egx) {
                context.getLogger().severe("Bodega actualizada pero falló publicar evento: " + egx.getMessage());
            }

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("id", id);
            body.put("nombre", dto.getNombre());
            body.put("direccion", dto.getDireccion());
            return request.createResponseBuilder(HttpStatus.OK)
                    .header("Content-Type", "application/json")
                    .body(body)
                    .build();

        } catch (SQLException sqle) {
            final String msg = sqle.getMessage() != null ? sqle.getMessage() : "";
            if (msg.contains("ORA-17041")) {
                return jsonError(request, HttpStatus.BAD_REQUEST, "Bad Request",
                        "Parámetros incompletos o inválidos en la solicitud.", path);
            }
            context.getLogger().severe("SQL error updateBodega: " + msg);
            return jsonError(request, HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                    "Ocurrió un error al procesar la solicitud.", path);
        } catch (Exception ex) {
            context.getLogger().severe("Error updateBodega: " + ex.getClass().getName());
            return jsonError(request, HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                    "Ocurrió un error al procesar la solicitud.", path);
        }
    }

    @FunctionName("eliminarBodega")
    public HttpResponseMessage eliminarBodega(
            @HttpTrigger(
                    name = "reqDelete",
                    methods = {HttpMethod.DELETE},
                    route = "bodegas/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        String walletPath = System.getenv(ORACLE_WALLET_DIR);
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        final String eventGridTopicEndpoint = "";
        final String eventGridTopicKey      = "";

        Map<String, Object> before = null;

        int rows;
        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            try (PreparedStatement sel = conn.prepareStatement(
                    "SELECT ID, NOMBRE, DIRECCION FROM BODEGA WHERE ID=?")) {
                sel.setLong(1, id);
                try (ResultSet rs = sel.executeQuery()) {
                    if (!rs.next()) {
                        return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                                .header("Content-Type", "application/json")
                                .body("{\"message\":\"No existe bodega con ID " + id + "\"}")
                                .build();
                    }
                    before = new LinkedHashMap<>();
                    before.put("id", rs.getLong("ID"));
                    before.put("nombre", rs.getString("NOMBRE"));
                    before.put("direccion", rs.getString("DIRECCION"));
                }
            }

            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM BODEGA WHERE ID=?")) {
                stmt.setLong(1, id);
                rows = stmt.executeUpdate();
            }

        } catch (SQLException sqle) {
            final String msg = sqle.getMessage() != null ? sqle.getMessage() : "";
            if (msg.contains("ORA-02292") || msg.contains("ORA-02266")) {
                return request.createResponseBuilder(HttpStatus.CONFLICT)
                        .header("Content-Type", "application/json")
                        .body("{\"error\":\"No se puede eliminar la bodega: existen registros dependientes (FK).\"}")
                        .build();
            }
            context.getLogger().severe("SQL error eliminarBodega: " + msg);
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("{\"error\":\"" + msg.replace("\"", "\\\"") + "\"}")
                    .build();
        } catch (Exception e) {
            context.getLogger().severe("Error al eliminar bodega: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("{\"error\":\"" + e.getMessage().replace("\"", "\\\"") + "\"}")
                    .build();
        }

        if (rows == 0) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body("{\"message\":\"No existe bodega con ID " + id + "\"}")
                    .build();
        }

        try {
            if (eventGridTopicEndpoint != null && !eventGridTopicEndpoint.isBlank() && eventGridTopicKey != null && !eventGridTopicKey.isBlank()) {
                EventGridPublisherClient<EventGridEvent> client = new EventGridPublisherClientBuilder()
                        .endpoint(eventGridTopicEndpoint)
                        .credential(new AzureKeyCredential(eventGridTopicKey))
                        .buildEventGridEventPublisherClient();

                Map<String, Object> eventData = new LinkedHashMap<>(before);
                eventData.put("deletedAt", OffsetDateTime.now().toString());

                EventGridEvent event = new EventGridEvent(
                        "/api/bodegas/" + id,
                        "api.bodega.eliminada.v1",
                        BinaryData.fromObject(eventData),
                        "1.0"
                );
                event.setEventTime(OffsetDateTime.now());

                client.sendEvent(event);
            } else {
                context.getLogger().warning("No se publicó evento: faltan EVENTGRID_TOPIC_ENDPOINT/KEY.");
            }
        } catch (Exception egx) {
            context.getLogger().severe("Bodega eliminada pero falló publicar evento: " + egx.getMessage());
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("{\"message\":\"Bodega eliminada con éxito\",\"id\":" + id + "}")
                .build();
    }

    private HttpResponseMessage jsonError(HttpRequestMessage<?> request,
                                          HttpStatus status,
                                          String error,
                                          String message,
                                          String path) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", java.time.OffsetDateTime.now().toString());
        body.put("status", status.value());
        body.put("error", error);
        body.put("message", message);
        body.put("path", path);

        return request.createResponseBuilder(status)
                .header("Content-Type", "application/json")
                .body(body)
                .build();
    }

}
