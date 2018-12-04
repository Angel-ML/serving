package com.tencent.angel.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class RestClient {

    public static void main(String[] args) {
        String getResource = "http://localhost:8501/angelServing/v1.0/models/default/versions/1";
        String postResource = "";
        Client client = Client.create();
        WebResource getWebResource = client
                .resource(getResource);
        WebResource postWebResource = client
                .resource(postResource);
        ClientResponse getResponse = getWebResource.accept("application/json")
                .get(ClientResponse.class);
        System.out.println(getResponse.getEntity(String.class));
        //String input = "";
        //ClientResponse postResponse = postWebResource.type("application/json")
        //        .post(ClientResponse.class, input);
    }

}
