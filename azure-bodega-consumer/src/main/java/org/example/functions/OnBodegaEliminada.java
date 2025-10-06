package org.example.functions;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.EventGridTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

import java.util.logging.Logger;

public class OnBodegaEliminada {
    @FunctionName("onBodegaEliminada")
    public void run(
            @EventGridTrigger(name = "eventBodegaEliminada") String content,
            final ExecutionContext context)
    {
        Logger logger = context.getLogger();
        logger.info("Funcion con Event Grid trigger ejecutada.");

        Gson gson = new Gson();
        JsonObject eventGridEvent = gson.fromJson(content, JsonObject.class);

        logger.info("Evento recibido: " + eventGridEvent.toString());

        String eventType = eventGridEvent.get("eventType").getAsString();
        String data = eventGridEvent.get("data").toString();

        logger.info("Tipo de evento: " + eventType);
        logger.info("Data del evento: " + data);
    }
}
