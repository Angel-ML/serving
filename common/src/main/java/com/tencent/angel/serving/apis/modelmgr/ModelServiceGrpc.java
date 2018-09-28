package com.tencent.angel.serving.apis.modelmgr;

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
 * ModelService provides methods to query and update the state of the server,
 * e.g. which models/versions are being served.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: apis/modelmgr/model_service.proto")
public class ModelServiceGrpc {

  private ModelServiceGrpc() {}

  public static final String SERVICE_NAME = "angel.serving.ModelService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest,
      com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse> METHOD_GET_MODEL_STATUS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "angel.serving.ModelService", "GetModelStatus"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest,
      com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse> METHOD_HANDLE_RELOAD_CONFIG_REQUEST =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "angel.serving.ModelService", "HandleReloadConfigRequest"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ModelServiceStub newStub(io.grpc.Channel channel) {
    return new ModelServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ModelServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ModelServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static ModelServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ModelServiceFutureStub(channel);
  }

  /**
   * <pre>
   * ModelService provides methods to query and update the state of the server,
   * e.g. which models/versions are being served.
   * </pre>
   */
  public static abstract class ModelServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Gets status of model. If the ModelSpec in the request does not specify
     * version, information about all versions of the model will be returned. If
     * the ModelSpec in the request does specify a version, the status of only
     * that version will be returned.
     * </pre>
     */
    public void getModelStatus(com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_MODEL_STATUS, responseObserver);
    }

    /**
     * <pre>
     * Reloads the set of served models. The new config supersedes the old one,
     * so if a model is omitted from the new config it will be unloaded and no
     * longer served.
     * </pre>
     */
    public void handleReloadConfigRequest(com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_HANDLE_RELOAD_CONFIG_REQUEST, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_MODEL_STATUS,
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest,
                com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse>(
                  this, METHODID_GET_MODEL_STATUS)))
          .addMethod(
            METHOD_HANDLE_RELOAD_CONFIG_REQUEST,
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest,
                com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse>(
                  this, METHODID_HANDLE_RELOAD_CONFIG_REQUEST)))
          .build();
    }
  }

  /**
   * <pre>
   * ModelService provides methods to query and update the state of the server,
   * e.g. which models/versions are being served.
   * </pre>
   */
  public static final class ModelServiceStub extends io.grpc.stub.AbstractStub<ModelServiceStub> {
    private ModelServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ModelServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ModelServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ModelServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Gets status of model. If the ModelSpec in the request does not specify
     * version, information about all versions of the model will be returned. If
     * the ModelSpec in the request does specify a version, the status of only
     * that version will be returned.
     * </pre>
     */
    public void getModelStatus(com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_MODEL_STATUS, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Reloads the set of served models. The new config supersedes the old one,
     * so if a model is omitted from the new config it will be unloaded and no
     * longer served.
     * </pre>
     */
    public void handleReloadConfigRequest(com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_HANDLE_RELOAD_CONFIG_REQUEST, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * ModelService provides methods to query and update the state of the server,
   * e.g. which models/versions are being served.
   * </pre>
   */
  public static final class ModelServiceBlockingStub extends io.grpc.stub.AbstractStub<ModelServiceBlockingStub> {
    private ModelServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ModelServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ModelServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ModelServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Gets status of model. If the ModelSpec in the request does not specify
     * version, information about all versions of the model will be returned. If
     * the ModelSpec in the request does specify a version, the status of only
     * that version will be returned.
     * </pre>
     */
    public com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse getModelStatus(com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_MODEL_STATUS, getCallOptions(), request);
    }

    /**
     * <pre>
     * Reloads the set of served models. The new config supersedes the old one,
     * so if a model is omitted from the new config it will be unloaded and no
     * longer served.
     * </pre>
     */
    public com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse handleReloadConfigRequest(com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_HANDLE_RELOAD_CONFIG_REQUEST, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * ModelService provides methods to query and update the state of the server,
   * e.g. which models/versions are being served.
   * </pre>
   */
  public static final class ModelServiceFutureStub extends io.grpc.stub.AbstractStub<ModelServiceFutureStub> {
    private ModelServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ModelServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ModelServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ModelServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Gets status of model. If the ModelSpec in the request does not specify
     * version, information about all versions of the model will be returned. If
     * the ModelSpec in the request does specify a version, the status of only
     * that version will be returned.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse> getModelStatus(
        com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_MODEL_STATUS, getCallOptions()), request);
    }

    /**
     * <pre>
     * Reloads the set of served models. The new config supersedes the old one,
     * so if a model is omitted from the new config it will be unloaded and no
     * longer served.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse> handleReloadConfigRequest(
        com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_HANDLE_RELOAD_CONFIG_REQUEST, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_MODEL_STATUS = 0;
  private static final int METHODID_HANDLE_RELOAD_CONFIG_REQUEST = 1;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ModelServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(ModelServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_MODEL_STATUS:
          serviceImpl.getModelStatus((com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse>) responseObserver);
          break;
        case METHODID_HANDLE_RELOAD_CONFIG_REQUEST:
          serviceImpl.handleReloadConfigRequest((com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse>) responseObserver);
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
        METHOD_GET_MODEL_STATUS,
        METHOD_HANDLE_RELOAD_CONFIG_REQUEST);
  }

}
