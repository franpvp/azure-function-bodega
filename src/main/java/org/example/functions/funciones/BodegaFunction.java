package org.example.functions.funciones;

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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            // Log interno, pero no se expone el detalle al cliente
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

            try (Connection conn = DriverManager.getConnection(url, user, pass);
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO BODEGA (NOMBRE, DIRECCION) VALUES (?, ?)")) {

                stmt.setString(1, nuevo.getNombre());
                stmt.setString(2, nuevo.getDireccion());
                stmt.executeUpdate();
            }

            return request.createResponseBuilder(HttpStatus.CREATED)
                    .header("Content-Type", "application/json")
                    .body("Bodega creada con éxito")
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
            try (PreparedStatement upd = conn.prepareStatement(sql)) {
                upd.setString(1, dto.getNombre());     // ?1 -> NOMBRE
                upd.setString(2, dto.getDireccion());  // ?2 -> DIRECCION
                upd.setLong(3, id);                    // ?3 -> ID (WHERE)
                int updated = upd.executeUpdate();
                if (updated == 0) {
                    return jsonError(request, HttpStatus.NOT_FOUND, "Not Found",
                            "No se actualizó la bodega con ID " + id, path);
                }
            }

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("id", id);
            body.put("nombre", dto.getNombre());
            body.put("descripcion", dto.getDireccion()); // si tu DTO lo mapea así
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

        int rows;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement("DELETE FROM BODEGA WHERE ID=?")) {
            stmt.setLong(1, id);
            rows = stmt.executeUpdate();
        } catch (Exception e) {
            context.getLogger().severe("Error al eliminar bodega: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage()).build();
        }

        if (rows == 0) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .body("No existe bodega con ID " + id).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .body("Bodega eliminado con éxito").build();
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
