package com.tencent.angel.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RestClient {

    private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

    public static void main(String[] args) throws InterruptedException {
        //pmml model test.
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        for(int i=0; i<10; i++) {
            callables.add(new Callable<String>() {
                public String call() throws Exception {
                    restRun();
                    return null;
                }
            });
        }
        List<Future<String>> futures = executorService.invokeAll(callables);
        executorService.shutdown();
    }

    private static void restRun() {
        LOG.info("Now in thread.");
        String postResource = "http://localhost:8501/angelServing/v1.0/models/lr/versions/6:predict";
        Client client = Client.create();
        WebResource postWebResource = client
                .resource(postResource);
        for(int i = 0; i < 20; i++) {
            String input = "{\"instances\":[{\"values\":{\"x1\":6.2, \"x2\":2.2, \"x3\":1.1, \"x4\":1.5}}]}";
            long start = System.currentTimeMillis();
            ClientResponse postResponse = postWebResource.type("application/json")
                    .post(ClientResponse.class, input);
            LOG.info("Finished prediction with {} ms", (System.currentTimeMillis() - start));
            LOG.info(postResponse.getEntity(String.class));
        }
    }

}
