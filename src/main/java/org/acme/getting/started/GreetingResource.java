package org.acme.getting.started;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.jboss.resteasy.annotations.jaxrs.PathParam;

@Path("/hello")
public class GreetingResource {

    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

    @Inject
    GreetingService service;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/greeting/{name}")
    public String greeting(@PathParam String name) {
        return service.greeting(name);
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() throws Exception {
        String topicId = "graal-pubsub-test-" + UUID.randomUUID().toString();
        String subscriptionId = "graal-pubsub-test-sub" + UUID.randomUUID().toString();

        String msg = "FAILED";
        createTopic(topicId);
        createSubscription(subscriptionId, topicId);
        publishMessage(topicId);
        msg = subscribeSync(subscriptionId);

        deleteTopic(topicId);
        deleteSubscription(subscriptionId);

        return subscriptionId + " - - - - " + msg;
    }

    private static void createTopic(String topicId) throws IOException {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            TopicName topicName = TopicName.of(PROJECT_ID, topicId);
            Topic topic = topicAdminClient.createTopic(topicName);
            System.out.println("Created topic: " + topic.getName() + " under project: " + PROJECT_ID);
        }
    }

    private static void createSubscription(String subscriptionId, String topicId) throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            TopicName topicName = TopicName.of(PROJECT_ID, topicId);
            ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(PROJECT_ID, subscriptionId);
            Subscription subscription =
                subscriptionAdminClient.createSubscription(
                    subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
            System.out.println("Created pull subscription: " + subscription.getName());
        }
    }

    private static void publishMessage(String topicId) throws Exception {
        Publisher publisher =
            Publisher
                .newBuilder(TopicName.of(PROJECT_ID, topicId))
                .build();

        String message = "Pub/Sub Graal Test published message at timestamp: " + Instant.now();
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

        publisher.publish(pubsubMessage);

        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        String messageId = messageIdFuture.get();

        System.out.println("Published message with ID: " + messageId);
    }

    private static String subscribeSync(String subscriptionId) throws IOException {
        SubscriberStubSettings subscriberStubSettings =
            SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(
                    SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                        .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                        .build())
                .build();

        String result = "NO PAYLOAD";
        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
            String subscriptionName = ProjectSubscriptionName.format(PROJECT_ID, subscriptionId);
            PullRequest pullRequest =
                PullRequest.newBuilder()
                    .setMaxMessages(1)
                    .setSubscription(subscriptionName)
                    .build();

            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
            List<String> ackIds = new ArrayList<>();
            for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                String payload = message.getMessage().getData().toStringUtf8();
                ackIds.add(message.getAckId());
                System.out.println("Received Payload: " + payload);
                result = payload;
            }

            AcknowledgeRequest acknowledgeRequest =
                AcknowledgeRequest.newBuilder()
                    .setSubscription(subscriptionName)
                    .addAllAckIds(ackIds)
                    .build();

            subscriber.acknowledgeCallable().call(acknowledgeRequest);
        }

        return result;
    }

    private static void deleteTopic(String topicId) throws IOException {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            TopicName topicName = TopicName.of(PROJECT_ID, topicId);
            try {
                topicAdminClient.deleteTopic(topicName);
                System.out.println("Deleted topic " + topicName);
            } catch (NotFoundException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static void deleteSubscription(String subscriptionId)
        throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(PROJECT_ID, subscriptionId);
            try {
                subscriptionAdminClient.deleteSubscription(subscriptionName);
                System.out.println("Deleted subscription " + subscriptionName);
            } catch (NotFoundException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}