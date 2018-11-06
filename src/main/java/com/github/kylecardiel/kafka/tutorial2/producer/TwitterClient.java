package com.github.kylecardiel.kafka.tutorial2.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TwitterClient {

    private String consumerKey = "uknA9SGRCqKj0quU8xkAppzFq";
    private String consumerSecret = "LaPxZUcLPfXnWXDthVHr6984sUGWnPVxzF8LtgFr6tIFgOTkcX";
    private String token = "200151080-epaNt1iFDD65a3ghPmOimcJ4qgGcUgB9YYORnQl7";
    private String secret = "aKFWt3z6m5hxurB6QTtwgxLAO7yFB9jaZTTFp5iUmdN8m";

    public Client create(BlockingQueue<String> msgQueue, List<String> terms){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

}
