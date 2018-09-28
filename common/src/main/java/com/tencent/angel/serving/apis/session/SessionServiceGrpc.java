package com.tencent.angel.serving.apis.session;

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
 * SessionService defines a service with which a client can interact to execute
 * Tensorflow model inference. The SessionService::SessionRun method is similar
 * to MasterService::RunStep of Tensorflow, except that all sessions are ready
 * to run, and you request a specific model/session with ModelSpec.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: apis/session/session_service.proto")
public class SessionServiceGrpc {

  private SessionServiceGrpc() {}

  public static final String SERVICE_NAME = "angel.serving.SessionService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunRequest,
      com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunResponse> METHOD_SESSION_RUN =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "angel.serving.SessionService", "SessionRun"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SessionServiceStub newStub(io.grpc.Channel channel) {
    return new SessionServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SessionServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new SessionServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static SessionServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new SessionServiceFutureStub(channel);
  }

  /**
   * <pre>
   * SessionService defines a service with which a client can interact to execute
   * Tensorflow model inference. The SessionService::SessionRun method is similar
   * to MasterService::RunStep of Tensorflow, except that all sessions are ready
   * to run, and you request a specific model/session with ModelSpec.
   * </pre>
   */
  public static abstract class SessionServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Runs inference of a given model.
     * </pre>
     */
    public void sessionRun(com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SESSION_RUN, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_SESSION_RUN,
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunRequest,
                com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunResponse>(
                  this, METHODID_SESSION_RUN)))
          .build();
    }
  }

  /**
   * <pre>
   * SessionService defines a service with which a client can interact to execute
   * Tensorflow model inference. The SessionService::SessionRun method is similar
   * to MasterService::RunStep of Tensorflow, except that all sessions are ready
   * to run, and you request a specific model/session with ModelSpec.
   * </pre>
   */
  public static final class SessionServiceStub extends io.grpc.stub.AbstractStub<SessionServiceStub> {
    private SessionServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SessionServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SessionServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SessionServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Runs inference of a given model.
     * </pre>
     */
    public void sessionRun(com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunRequest request,
        io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SESSION_RUN, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * SessionService defines a service with which a client can interact to execute
   * Tensorflow model inference. The SessionService::SessionRun method is similar
   * to MasterService::RunStep of Tensorflow, except that all sessions are ready
   * to run, and you request a specific model/session with ModelSpec.
   * </pre>
   */
  public static final class SessionServiceBlockingStub extends io.grpc.stub.AbstractStub<SessionServiceBlockingStub> {
    private SessionServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SessionServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SessionServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SessionServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Runs inference of a given model.
     * </pre>
     */
    public com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunResponse sessionRun(com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SESSION_RUN, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * SessionService defines a service with which a client can interact to execute
   * Tensorflow model inference. The SessionService::SessionRun method is similar
   * to MasterService::RunStep of Tensorflow, except that all sessions are ready
   * to run, and you request a specific model/session with ModelSpec.
   * </pre>
   */
  public static final class SessionServiceFutureStub extends io.grpc.stub.AbstractStub<SessionServiceFutureStub> {
    private SessionServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SessionServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SessionServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SessionServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Runs inference of a given model.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunResponse> sessionRun(
        com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SESSION_RUN, getCallOptions()), request);
    }
  }

  private static final int METHODID_SESSION_RUN = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final SessionServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(SessionServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SESSION_RUN:
          serviceImpl.sessionRun((com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.angel.serving.apis.session.SessionServiceProtos.SessionRunResponse>) responseObserver);
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
        METHOD_SESSION_RUN);
  }

}
