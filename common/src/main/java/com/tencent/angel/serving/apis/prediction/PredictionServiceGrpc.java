package com.tencent.angel.serving.apis.prediction;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 * <pre>
 * open source marker; do not remove
 * PredictionService provides access to machine-learned models loaded by
 * model_servers.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: apis/prediction/prediction_service.proto")
public class PredictionServiceGrpc {

  private PredictionServiceGrpc() {}

  public static final String SERVICE_NAME = "angel.serving.PredictionService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest,
      com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse> METHOD_CLASSIFY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "angel.serving.PredictionService", "Classify"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest,
      com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> METHOD_REGRESS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "angel.serving.PredictionService", "Regress"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest,
      com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> METHOD_PREDICT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "angel.serving.PredictionService", "Predict"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest,
      com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> METHOD_MULTI_INFERENCE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "angel.serving.PredictionService", "MultiInference"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest,
      com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> METHOD_GET_MODEL_METADATA =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "angel.serving.PredictionService", "GetModelMetadata"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse.getDefaultInstance()));

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
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
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
      asyncUnimplementedUnaryCall(METHOD_CLASSIFY, responseObserver);
    }

    /**
     * <pre>
     * Regress.
     * </pre>
     */
    public void regress(com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_REGRESS, responseObserver);
    }

    /**
     * <pre>
     * Predict -- provides access to loaded TensorFlow model.
     * </pre>
     */
    public void predict(com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PREDICT, responseObserver);
    }

    /**
     * <pre>
     * MultiInference API for multi-headed models.
     * </pre>
     */
    public void multiInference(com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_MULTI_INFERENCE, responseObserver);
    }

    /**
     * <pre>
     * GetModelMetadata - provides access to metadata for loaded models.
     * </pre>
     */
    public void getModelMetadata(com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_MODEL_METADATA, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_CLASSIFY,
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationRequest,
                com.tencent.angel.serving.apis.prediction.ClassificationProtos.ClassificationResponse>(
                  this, METHODID_CLASSIFY)))
          .addMethod(
            METHOD_REGRESS,
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest,
                com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse>(
                  this, METHODID_REGRESS)))
          .addMethod(
            METHOD_PREDICT,
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest,
                com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse>(
                  this, METHODID_PREDICT)))
          .addMethod(
            METHOD_MULTI_INFERENCE,
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest,
                com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse>(
                  this, METHODID_MULTI_INFERENCE)))
          .addMethod(
            METHOD_GET_MODEL_METADATA,
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
          getChannel().newCall(METHOD_CLASSIFY, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Regress.
     * </pre>
     */
    public void regress(com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REGRESS, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Predict -- provides access to loaded TensorFlow model.
     * </pre>
     */
    public void predict(com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PREDICT, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * MultiInference API for multi-headed models.
     * </pre>
     */
    public void multiInference(com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MULTI_INFERENCE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * GetModelMetadata - provides access to metadata for loaded models.
     * </pre>
     */
    public void getModelMetadata(com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_MODEL_METADATA, getCallOptions()), request, responseObserver);
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
          getChannel(), METHOD_CLASSIFY, getCallOptions(), request);
    }

    /**
     * <pre>
     * Regress.
     * </pre>
     */
    public com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse regress(com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REGRESS, getCallOptions(), request);
    }

    /**
     * <pre>
     * Predict -- provides access to loaded TensorFlow model.
     * </pre>
     */
    public com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse predict(com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PREDICT, getCallOptions(), request);
    }

    /**
     * <pre>
     * MultiInference API for multi-headed models.
     * </pre>
     */
    public com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse multiInference(com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MULTI_INFERENCE, getCallOptions(), request);
    }

    /**
     * <pre>
     * GetModelMetadata - provides access to metadata for loaded models.
     * </pre>
     */
    public com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse getModelMetadata(com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_MODEL_METADATA, getCallOptions(), request);
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
          getChannel().newCall(METHOD_CLASSIFY, getCallOptions()), request);
    }

    /**
     * <pre>
     * Regress.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionResponse> regress(
        com.tencent.angel.serving.apis.prediction.RegressionProtos.RegressionRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REGRESS, getCallOptions()), request);
    }

    /**
     * <pre>
     * Predict -- provides access to loaded TensorFlow model.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.prediction.PredictProtos.PredictResponse> predict(
        com.tencent.angel.serving.apis.prediction.PredictProtos.PredictRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PREDICT, getCallOptions()), request);
    }

    /**
     * <pre>
     * MultiInference API for multi-headed models.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceResponse> multiInference(
        com.tencent.angel.serving.apis.prediction.InferenceProtos.MultiInferenceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_MULTI_INFERENCE, getCallOptions()), request);
    }

    /**
     * <pre>
     * GetModelMetadata - provides access to metadata for loaded models.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataResponse> getModelMetadata(
        com.tencent.angel.serving.apis.prediction.GetModelMetadataProtos.GetModelMetadataRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_MODEL_METADATA, getCallOptions()), request);
    }
  }

  private static final int METHODID_CLASSIFY = 0;
  private static final int METHODID_REGRESS = 1;
  private static final int METHODID_PREDICT = 2;
  private static final int METHODID_MULTI_INFERENCE = 3;
  private static final int METHODID_GET_MODEL_METADATA = 4;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PredictionServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(PredictionServiceImplBase serviceImpl, int methodId) {
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

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_CLASSIFY,
        METHOD_REGRESS,
        METHOD_PREDICT,
        METHOD_MULTI_INFERENCE,
        METHOD_GET_MODEL_METADATA);
  }

}
