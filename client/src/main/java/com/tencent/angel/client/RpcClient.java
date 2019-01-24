package com.tencent.angel.client;

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec;
import com.tencent.angel.serving.apis.common.InstanceProtos;
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest;
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse;
import com.tencent.angel.serving.apis.modelmgr.ModelServiceGrpc;
import com.tencent.angel.serving.apis.prediction.RequestProtos.Request;
import com.tencent.angel.serving.apis.prediction.ResponseProtos.Response;
import com.tencent.angel.serving.apis.prediction.PredictionServiceGrpc;
import com.tencent.angel.utils.InstanceUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import com.tencent.angel.utils.ProtoUtils;
import java.util.*;
import java.util.concurrent.*;

import io.grpc.channelz.v1.ChannelzGrpc;
import org.apache.commons.cli.*;
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

    public void doPredict(String modelName, long modelVersion, String modelPlatform) throws Exception {
        InstanceProtos.Instance instance;
        //for angel test dense data
        ArrayList<Float> denseValues = new ArrayList<>();
        long dim = 123;
        Random rand = new Random();
        for (int j = 0; j < (int)dim; j++) {
            denseValues.add(rand.nextFloat());
        }
        InstanceProtos.Instance denseInstance = ProtoUtils.getInstance(denseValues.iterator());

        //for angel test sparse data
        Map<Integer, Float> integerFloatHashMap = new HashMap<>();
        for (int j = 0; j < 13; j++) {
            integerFloatHashMap.put(rand.nextInt((int)dim), rand.nextFloat());
        }
        System.out.println(integerFloatHashMap.toString());
        InstanceProtos.Instance sparseInstance = ProtoUtils.getInstance((int)dim, integerFloatHashMap);

        //for pmml test data
        Map<String, Double> data = new HashMap<String, Double>();
        data.put("x1", 6.2);
        data.put("x2", 2.2);
        data.put("x3", 1.1);
        data.put("x4", 1.5);
        InstanceProtos.Instance pmmlInstance = ProtoUtils.getInstance(data);

        switch (modelPlatform.trim().toLowerCase()) {
            case "angel":
                instance = denseInstance;
                break;
            case "pmml":
                instance = pmmlInstance;
                break;
            default:
                instance = null;
                return;
        }
        // Generate gRPC request, signature inputs name should be correct or exceptions
        ModelSpec modelSpec = ProtoUtils.getModelSpec(modelName, modelVersion, "predict");
        Request request = Request.newBuilder().setModelSpec(modelSpec).addInstances(instance).build();
        GetModelStatusRequest statusRequest = GetModelStatusRequest.newBuilder().setModelSpec(modelSpec).build();
        // Request gRPC server
        try {
            long start = System.currentTimeMillis();
            Response response = blockingStub.predict(request);
            LOG.info("Finished prediction with {} ms", (System.currentTimeMillis() - start));
            InstanceProtos.Instance result = response.getPredictions(0);
            LOG.info(Objects.requireNonNull(InstanceUtils.getStringKeyMap(result)).toString());
            start = System.currentTimeMillis();
            GetModelStatusResponse statusResponse = modelServiceBlockingStub.getModelStatus(statusRequest);
            LOG.info("Finished get model status with {} ms", (System.currentTimeMillis() - start));
            LOG.info(statusResponse.toString());
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Options options = new Options();
        Option modelName = new Option("n", "modelName", true, "model name.");
        modelName.setRequired(true);
        options.addOption(modelName);
        Option modelVersion = new Option("v", "modelVersion", true, "model version");
        modelVersion.setRequired(true);
        options.addOption(modelVersion);
        Option modelPlatform = new Option("p", "modelPlatform", true, "model platform");
        modelPlatform.setRequired(true);
        options.addOption(modelPlatform);
        Option rpcPort = new Option("e", "rpcPort", true, "RPC port");
        rpcPort.setRequired(true);
        options.addOption(rpcPort);
        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
            return;
        }
        String name = cmd.getOptionValue("modelName");
        String platform = cmd.getOptionValue("modelPlatform");
        long version = Long.valueOf(cmd.getOptionValue("modelVersion"));
        int port = Integer.valueOf(cmd.getOptionValue("rpcPort"));
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Set<Callable<String>> callables = new HashSet<Callable<String>>();
        for(int i=0; i<1; i++) {
            callables.add(new Callable<String>() {
                public String call() throws Exception {
                    LOG.info("Now in thread.");
                    RpcClient client = new RpcClient("localhost", port);
                    try {
                        for(int i = 0; i < 1; i++) {
                            client.doPredict(name, version, platform);
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
