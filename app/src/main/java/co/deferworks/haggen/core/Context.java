package co.deferworks.haggen.core;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.logging.Logger;

public class Context {
    public static final String NAME = "HaggenContext";

    public static void callExternalService(String serviceURL) {
        Logger.getLogger(NAME).info("Calling external service at: " + serviceURL);
        var request = HttpRequest.newBuilder()
                .uri(URI.create(serviceURL))
                .headers("Accept", "application/json", "User-Agent", "HaggenClient/1.0", "Content-Type",
                        "application/json", "Accept-Language", "en-US")
                .GET().build();
        try {
            var response = java.net.http.HttpClient.newHttpClient().send(request,
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            Logger.getLogger(NAME).info("Response from external service: \n" + response.body());
        } catch (Exception e) {
            Logger.getLogger(NAME).severe("Failed to call external service: " + e.getMessage());
        }
    }

}
