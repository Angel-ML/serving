/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.serving.apis.modelmgr;

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
 * ModelService provides methods to query and update the state of the server,
 * e.g. which models/versions are being served.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: apis/modelmgr/model_service.proto")
public final class ModelServiceGrpc {

  private ModelServiceGrpc() {}

  public static final String SERVICE_NAME = "angel.serving.ModelService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest,
      com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse> getGetModelStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetModelStatus",
      requestType = com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest.class,
      responseType = com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest,
      com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse> getGetModelStatusMethod() {
    io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest, com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse> getGetModelStatusMethod;
    if ((getGetModelStatusMethod = ModelServiceGrpc.getGetModelStatusMethod) == null) {
      synchronized (ModelServiceGrpc.class) {
        if ((getGetModelStatusMethod = ModelServiceGrpc.getGetModelStatusMethod) == null) {
          ModelServiceGrpc.getGetModelStatusMethod = getGetModelStatusMethod = 
              io.grpc.MethodDescriptor.<com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest, com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "angel.serving.ModelService", "GetModelStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new ModelServiceMethodDescriptorSupplier("GetModelStatus"))
                  .build();
          }
        }
     }
     return getGetModelStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest,
      com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse> getHandleReloadConfigRequestMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HandleReloadConfigRequest",
      requestType = com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest.class,
      responseType = com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest,
      com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse> getHandleReloadConfigRequestMethod() {
    io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest, com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse> getHandleReloadConfigRequestMethod;
    if ((getHandleReloadConfigRequestMethod = ModelServiceGrpc.getHandleReloadConfigRequestMethod) == null) {
      synchronized (ModelServiceGrpc.class) {
        if ((getHandleReloadConfigRequestMethod = ModelServiceGrpc.getHandleReloadConfigRequestMethod) == null) {
          ModelServiceGrpc.getHandleReloadConfigRequestMethod = getHandleReloadConfigRequestMethod = 
              io.grpc.MethodDescriptor.<com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest, com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "angel.serving.ModelService", "HandleReloadConfigRequest"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.angel.serving.apis.modelmgr.ModelManagement.ReloadConfigResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new ModelServiceMethodDescriptorSupplier("HandleReloadConfigRequest"))
                  .build();
          }
        }
     }
     return getHandleReloadConfigRequestMethod;
  }

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
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
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
      asyncUnimplementedUnaryCall(getGetModelStatusMethod(), responseObserver);
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
      asyncUnimplementedUnaryCall(getHandleReloadConfigRequestMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetModelStatusMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusRequest,
                com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.GetModelStatusResponse>(
                  this, METHODID_GET_MODEL_STATUS)))
          .addMethod(
            getHandleReloadConfigRequestMethod(),
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
          getChannel().newCall(getGetModelStatusMethod(), getCallOptions()), request, responseObserver);
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
          getChannel().newCall(getHandleReloadConfigRequestMethod(), getCallOptions()), request, responseObserver);
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
          getChannel(), getGetModelStatusMethod(), getCallOptions(), request);
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
          getChannel(), getHandleReloadConfigRequestMethod(), getCallOptions(), request);
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
          getChannel().newCall(getGetModelStatusMethod(), getCallOptions()), request);
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
          getChannel().newCall(getHandleReloadConfigRequestMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_MODEL_STATUS = 0;
  private static final int METHODID_HANDLE_RELOAD_CONFIG_REQUEST = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ModelServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ModelServiceImplBase serviceImpl, int methodId) {
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

  private static abstract class ModelServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ModelServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.tencent.angel.serving.apis.modelmgr.ModelServiceProtos.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ModelService");
    }
  }

  private static final class ModelServiceFileDescriptorSupplier
      extends ModelServiceBaseDescriptorSupplier {
    ModelServiceFileDescriptorSupplier() {}
  }

  private static final class ModelServiceMethodDescriptorSupplier
      extends ModelServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ModelServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ModelServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ModelServiceFileDescriptorSupplier())
              .addMethod(getGetModelStatusMethod())
              .addMethod(getHandleReloadConfigRequestMethod())
              .build();
        }
      }
    }
    return result;
  }
}
