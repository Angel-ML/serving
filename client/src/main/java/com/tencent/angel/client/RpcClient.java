package com.tencent.angel.client;

import com.google.protobuf.Int64Value;
import com.tencent.angel.core.graph.TensorProtos.TensorProto;
import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec;
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest;
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse;
import com.tencent.angel.serving.apis.modelmgr.ModelServiceGrpc;
import com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest;
import com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse;
import com.tencent.angel.serving.apis.prediction.PredictionServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import com.tencent.angel.utils.ProtoUtils;
import java.util.*;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcClient {
    private static final Logger LOG = LoggerFactory.getLogger(RpcClient.class);

    private final ManagedChannel channel;
    private final PredictionServiceGrpc.PredictionServiceBlockingStub blockingStub;
    private final PredictionServiceGrpc.PredictionServiceStub asyncStub;
    private final ModelServiceGrpc.ModelServiceBlockingStub modelServiceBlockingStub;
    private final ModelServiceGrpc.ModelServiceStub modelServiceStub;

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
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void doPredict(String modelName, long modelVersion) {
        List<Integer> keys = new ArrayList<Integer>();
        List<Float> values = new ArrayList<Float>();
        long dim = 123;

        //build input tensor
        Random rand = new Random();
        for (int j = 0; j < 13; j++) {
            keys.add(rand.nextInt((int)dim));
            values.add(rand.nextFloat());
        }

        TensorProto featuresTensorProto = ProtoUtils.toTensorProto(dim, keys, values);
        // Generate gRPC request, signature inputs name should be correct or exceptions
        ModelSpec modelSpec = ProtoUtils.getModelSpec(modelName, modelVersion, "predict");
        PredictRequest request = PredictRequest.newBuilder().setModelSpec(modelSpec)
                .putInputs("inputs", featuresTensorProto).build();
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
            PredictResponse response = blockingStub.predict(request);
            LOG.info("Finished prediction with {} ms", (System.currentTimeMillis() - start));
            java.util.Map<String, TensorProto> outputs = response.getOutputsMap();
            LOG.info(outputs.toString());
            start = System.currentTimeMillis();
            GetModelStatusResponse statusResponse = modelServiceBlockingStub.getModelStatus(statusRequest);
            LOG.info("Finished get model status with {} ms", (System.currentTimeMillis() - start));
            LOG.info(statusResponse.toString());
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        for(int i=0; i<30; i++) {
            callables.add(new Callable<String>() {
                public String call() throws Exception {
                    LOG.info("Now in thread.");
                    RpcClient client = new RpcClient("localhost", 8500);
                    String modelName = "lr";
                    long modelVersion = 1L;
                    try {
                        for(int i = 0; i < 1000; i++) {
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
