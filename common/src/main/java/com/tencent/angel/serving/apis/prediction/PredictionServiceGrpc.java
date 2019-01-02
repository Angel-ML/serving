package com.tencent.angel.serving.apis.prediction;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 * open source marker; do not remove
 * PredictionService provides access to machine-learned models loaded by
 * model_servers.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: apis/prediction/prediction_service.proto")
public final class PredictionServiceGrpc {

  private PredictionServiceGrpc() {}

  public static final String SERVICE_NAME = "angel.serving.PredictionService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest,
      com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse> getClassifyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Classify",
      requestType = com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest.class,
      responseType = com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest,
      com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse> getClassifyMethod() {
    io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest, com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse> getClassifyMethod;
    if ((getClassifyMethod = PredictionServiceGrpc.getClassifyMethod) == null) {
      synchronized (PredictionServiceGrpc.class) {
        if ((getClassifyMethod = PredictionServiceGrpc.getClassifyMethod) == null) {
          PredictionServiceGrpc.getClassifyMethod = getClassifyMethod = 
              io.grpc.MethodDescriptor.<com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest, com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "angel.serving.PredictionService", "Classify"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PredictionServiceMethodDescriptorSupplier("Classify"))
                  .build();
          }
        }
     }
     return getClassifyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest,
      com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> getRegressMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Regress",
      requestType = com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest.class,
      responseType = com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest,
      com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> getRegressMethod() {
    io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest, com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> getRegressMethod;
    if ((getRegressMethod = PredictionServiceGrpc.getRegressMethod) == null) {
      synchronized (PredictionServiceGrpc.class) {
        if ((getRegressMethod = PredictionServiceGrpc.getRegressMethod) == null) {
          PredictionServiceGrpc.getRegressMethod = getRegressMethod = 
              io.grpc.MethodDescriptor.<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest, com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "angel.serving.PredictionService", "Regress"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PredictionServiceMethodDescriptorSupplier("Regress"))
                  .build();
          }
        }
     }
     return getRegressMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest,
      com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> getPredictMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Predict",
      requestType = com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest.class,
      responseType = com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest,
      com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> getPredictMethod() {
    io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest, com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> getPredictMethod;
    if ((getPredictMethod = PredictionServiceGrpc.getPredictMethod) == null) {
      synchronized (PredictionServiceGrpc.class) {
        if ((getPredictMethod = PredictionServiceGrpc.getPredictMethod) == null) {
          PredictionServiceGrpc.getPredictMethod = getPredictMethod = 
              io.grpc.MethodDescriptor.<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest, com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "angel.serving.PredictionService", "Predict"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PredictionServiceMethodDescriptorSupplier("Predict"))
                  .build();
          }
        }
     }
     return getPredictMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest,
      com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> getMultiInferenceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MultiInference",
      requestType = com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest.class,
      responseType = com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest,
      com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> getMultiInferenceMethod() {
    io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest, com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> getMultiInferenceMethod;
    if ((getMultiInferenceMethod = PredictionServiceGrpc.getMultiInferenceMethod) == null) {
      synchronized (PredictionServiceGrpc.class) {
        if ((getMultiInferenceMethod = PredictionServiceGrpc.getMultiInferenceMethod) == null) {
          PredictionServiceGrpc.getMultiInferenceMethod = getMultiInferenceMethod = 
              io.grpc.MethodDescriptor.<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest, com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "angel.serving.PredictionService", "MultiInference"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PredictionServiceMethodDescriptorSupplier("MultiInference"))
                  .build();
          }
        }
     }
     return getMultiInferenceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest,
      com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> getGetModelMetadataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetModelMetadata",
      requestType = com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest.class,
      responseType = com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest,
      com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> getGetModelMetadataMethod() {
    io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest, com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> getGetModelMetadataMethod;
    if ((getGetModelMetadataMethod = PredictionServiceGrpc.getGetModelMetadataMethod) == null) {
      synchronized (PredictionServiceGrpc.class) {
        if ((getGetModelMetadataMethod = PredictionServiceGrpc.getGetModelMetadataMethod) == null) {
          PredictionServiceGrpc.getGetModelMetadataMethod = getGetModelMetadataMethod = 
              io.grpc.MethodDescriptor.<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest, com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "angel.serving.PredictionService", "GetModelMetadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PredictionServiceMethodDescriptorSupplier("GetModelMetadata"))
                  .build();
          }
        }
     }
     return getGetModelMetadataMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PredictionServiceStub newStub(io.grpc.Channel channel) {
    return new PredictionServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PredictionServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new PredictionServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PredictionServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new PredictionServiceFutureStub(channel);
  }

  /**
   * <pre>
   * open source marker; do not remove
   * PredictionService provides access to machine-learned models loaded by
   * model_servers.
   * </pre>
   */
  public static abstract class PredictionServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Classify.
     * </pre>
     */
    public void classify(com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getClassifyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Regress.
     * </pre>
     */
    public void regress(com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRegressMethod(), responseObserver);
    }

    /**
     * <pre>
     * Predict -- provides access to loaded TensorFlow model.
     * </pre>
     */
    public void predict(com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getPredictMethod(), responseObserver);
    }

    /**
     * <pre>
     * MultiInference API for multi-headed models.
     * </pre>
     */
    public void multiInference(com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMultiInferenceMethod(), responseObserver);
    }

    /**
     * <pre>
     * GetModelMetadata - provides access to metadata for loaded models.
     * </pre>
     */
    public void getModelMetadata(com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetModelMetadataMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getClassifyMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest,
                com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse>(
                  this, METHODID_CLASSIFY)))
          .addMethod(
            getRegressMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest,
                com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse>(
                  this, METHODID_REGRESS)))
          .addMethod(
            getPredictMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest,
                com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse>(
                  this, METHODID_PREDICT)))
          .addMethod(
            getMultiInferenceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest,
                com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse>(
                  this, METHODID_MULTI_INFERENCE)))
          .addMethod(
            getGetModelMetadataMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest,
                com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse>(
                  this, METHODID_GET_MODEL_METADATA)))
          .build();
    }
  }

  /**
   * <pre>
   * open source marker; do not remove
   * PredictionService provides access to machine-learned models loaded by
   * model_servers.
   * </pre>
   */
  public static final class PredictionServiceStub extends io.grpc.stub.AbstractStub<PredictionServiceStub> {
    private PredictionServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PredictionServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PredictionServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PredictionServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Classify.
     * </pre>
     */
    public void classify(com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getClassifyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Regress.
     * </pre>
     */
    public void regress(com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRegressMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Predict -- provides access to loaded TensorFlow model.
     * </pre>
     */
    public void predict(com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getPredictMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * MultiInference API for multi-headed models.
     * </pre>
     */
    public void multiInference(com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMultiInferenceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * GetModelMetadata - provides access to metadata for loaded models.
     * </pre>
     */
    public void getModelMetadata(com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetModelMetadataMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * open source marker; do not remove
   * PredictionService provides access to machine-learned models loaded by
   * model_servers.
   * </pre>
   */
  public static final class PredictionServiceBlockingStub extends io.grpc.stub.AbstractStub<PredictionServiceBlockingStub> {
    private PredictionServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PredictionServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PredictionServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PredictionServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Classify.
     * </pre>
     */
    public com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse classify(com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest request) {
      return blockingUnaryCall(
          getChannel(), getClassifyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Regress.
     * </pre>
     */
    public com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse regress(com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest request) {
      return blockingUnaryCall(
          getChannel(), getRegressMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Predict -- provides access to loaded TensorFlow model.
     * </pre>
     */
    public com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse predict(com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest request) {
      return blockingUnaryCall(
          getChannel(), getPredictMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * MultiInference API for multi-headed models.
     * </pre>
     */
    public com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse multiInference(com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest request) {
      return blockingUnaryCall(
          getChannel(), getMultiInferenceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * GetModelMetadata - provides access to metadata for loaded models.
     * </pre>
     */
    public com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse getModelMetadata(com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetModelMetadataMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * open source marker; do not remove
   * PredictionService provides access to machine-learned models loaded by
   * model_servers.
   * </pre>
   */
  public static final class PredictionServiceFutureStub extends io.grpc.stub.AbstractStub<PredictionServiceFutureStub> {
    private PredictionServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PredictionServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PredictionServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PredictionServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Classify.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse> classify(
        com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getClassifyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Regress.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> regress(
        com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRegressMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Predict -- provides access to loaded TensorFlow model.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> predict(
        com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getPredictMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * MultiInference API for multi-headed models.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> multiInference(
        com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMultiInferenceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * GetModelMetadata - provides access to metadata for loaded models.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> getModelMetadata(
        com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetModelMetadataMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CLASSIFY = 0;
  private static final int METHODID_REGRESS = 1;
  private static final int METHODID_PREDICT = 2;
  private static final int METHODID_MULTI_INFERENCE = 3;
  private static final int METHODID_GET_MODEL_METADATA = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PredictionServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(PredictionServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CLASSIFY:
          serviceImpl.classify((com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse>) responseObserver);
          break;
        case METHODID_REGRESS:
          serviceImpl.regress((com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse>) responseObserver);
          break;
        case METHODID_PREDICT:
          serviceImpl.predict((com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse>) responseObserver);
          break;
        case METHODID_MULTI_INFERENCE:
          serviceImpl.multiInference((com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse>) responseObserver);
          break;
        case METHODID_GET_MODEL_METADATA:
          serviceImpl.getModelMetadata((com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class PredictionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PredictionServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.tencent.angel.serving.apis.prediction.PredictionServiceProtos.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PredictionService");
    }
  }

  private static final class PredictionServiceFileDescriptorSupplier
      extends PredictionServiceBaseDescriptorSupplier {
    PredictionServiceFileDescriptorSupplier() {}
  }

  private static final class PredictionServiceMethodDescriptorSupplier
      extends PredictionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    PredictionServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (PredictionServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PredictionServiceFileDescriptorSupplier())
              .addMethod(getClassifyMethod())
              .addMethod(getRegressMethod())
              .addMethod(getPredictMethod())
              .addMethod(getMultiInferenceMethod())
              .addMethod(getGetModelMetadataMethod())
              .build();
        }
      }
    }
    return result;
  }
}
