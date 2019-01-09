package com.tencent.angel.client;

import com.google.protobuf.Int64Value;
import com.tencent.angel.core.graph.TensorProtos.TensorProto;
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec;
import com.tencent.angel.serving.apis.common.InstanceProtos;
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest;
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse;
import com.tencent.angel.serving.apis.modelmgr.ModelServiceGrpc;
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request;
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response;
import com.tencent.angel.serving.apis.prediction.PredictionServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import com.tencent.angel.utils.ProtoUtils;
import java.util.*;
import java.util.concurrent.*;

import io.grpc.channelz.v1.ChannelzGrpc;
import io.grpc.channelz.v1.GetChannelRequest;
import io.grpc.channelz.v1.GetServersRequest;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcClient {
    private static final Logger LOG = LoggerFactory.getLogger(RpcClient.class);

    private final ManagedChannel channel;
    private final PredictionServiceGrpc.PredictionServiceBlockingStub blockingStub;
    private final PredictionServiceGrpc.PredictionServiceStub asyncStub;
    private final ModelServiceGrpc.ModelServiceBlockingStub modelServiceBlockingStub;
    private final ModelServiceGrpc.ModelServiceStub modelServiceStub;
    private final ChannelzGrpc.ChannelzBlockingStub channelzBlockingStub;

    public RpcClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
//        this(NettyChannelBuilder.forAddress(host, port).usePlaintext(true)
//                .directExecutor().maxMessageSize(Integer.MAX_VALUE));
    }

    /** Construct client for accessing prediction service server using the existing channel. */
    public RpcClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = PredictionServiceGrpc.newBlockingStub(channel);
        asyncStub = PredictionServiceGrpc.newStub(channel);
        modelServiceBlockingStub = ModelServiceGrpc.newBlockingStub(channel);
        modelServiceStub = ModelServiceGrpc.newStub(channel);
        channelzBlockingStub = ChannelzGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void doPredict(String modelName, long modelVersion) throws Exception {
        Map<Integer, Float> values = new HashMap<>();
        int dim = 123;

        //build input tensor
        Random rand = new Random();
        for (int j = 0; j < 13; j++) {
            values.put(rand.nextInt(dim), rand.nextFloat());
        }

        InstanceProtos.Instance instance = ProtoUtils.getInstance(dim, values);
        // Generate gRPC request, signature inputs name should be correct or exceptions
        ModelSpec modelSpec = ProtoUtils.getModelSpec(modelName, modelVersion, "predict");
        Request request = Request.newBuilder().setModelSpec(modelSpec).addInstances(instance).build();
        GetModelStatusRequest statusRequest = GetModelStatusRequest.newBuilder().setModelSpec(modelSpec).build();
        // Request gRPC server
        try {
            /*StreamObserver<PredictResponse> responseStreamObserver = new StreamObserver<PredictResponse>() {
                @Override
                public void onNext(PredictResponse value) {
                    java.util.Map<String, TensorProto> outputs = value.getOutputsMap();
                    LOG.info(outputs.toString());
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {
                    LOG.info("finished.");
                }
            };
            asyncStub.predict(request, responseStreamObserver);
            LOG.info("Finished prediction with {} ms", (System.currentTimeMillis() - start));*/
            long start = System.currentTimeMillis();
            Response response = blockingStub.predict(request);
            LOG.info("Finished prediction with {} ms", (System.currentTimeMillis() - start));
            InstanceProtos.Instance result = response.getPredictions(0);
            LOG.info(result.toString());
            start = System.currentTimeMillis();
            GetModelStatusResponse statusResponse = modelServiceBlockingStub.getModelStatus(statusRequest);
            LOG.info("Finished get model status with {} ms", (System.currentTimeMillis() - start));
            LOG.info(statusResponse.toString());
            //LOG.info(channelzBlockingStub.getServers(GetServersRequest.newBuilder().build()).toString());
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        for(int i=0; i<10; i++) {
            callables.add(new Callable<String>() {
                public String call() throws Exception {
                    LOG.info("Now in thread.");
                    RpcClient client = new RpcClient("localhost", 8500);
                    String modelName = "lr";
                    long modelVersion = 1L;
                    try {
                        for(int i = 0; i < 10; i++) {
                            client.doPredict(modelName, modelVersion);
                        }
                    } finally {
                        client.shutdown();
                    }
                    return null;
                }
            });
        }
        List<Future<String>> futures = executorService.invokeAll(callables);
        executorService.shutdown();
    }
}
