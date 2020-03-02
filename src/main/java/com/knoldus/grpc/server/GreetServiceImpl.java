package com.knoldus.grpc.server;

import com.knoldus.greet.FindMaximumRequest;
import com.knoldus.greet.FindMaximumResponse;
import com.knoldus.greet.GreetEveryoneRequest;
import com.knoldus.greet.GreetEveryoneResponse;
import com.knoldus.greet.GreetManyTimesRequest;
import com.knoldus.greet.GreetManyTimesResponse;
import com.knoldus.greet.GreetRequest;
import com.knoldus.greet.GreetResponse;
import com.knoldus.greet.GreetServiceGrpc;
import com.knoldus.greet.GreetWithDeadlineRequest;
import com.knoldus.greet.GreetWithDeadlineResponse;
import com.knoldus.greet.Greeting;
import com.knoldus.greet.LongGreetRequest;
import com.knoldus.greet.LongGreetResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

public class GreetServiceImpl extends GreetServiceGrpc.GreetServiceImplBase {

    @Override
    public void greet(GreetRequest request, StreamObserver<GreetResponse> responseObserver) {
        // extract the fields we need
       final Greeting greeting = request.getGreeting();
        final String firstName = greeting.getFirstName();

        // create the response
        final String result = "Hello " + firstName;
        GreetResponse response = GreetResponse.newBuilder()
                .setResult(result)
                .build();

        // send the response
        responseObserver.onNext(response);

        // complete the RPC call
        responseObserver.onCompleted();
    }

    @Override
    public void greetManyTimes(GreetManyTimesRequest request, StreamObserver<GreetManyTimesResponse> responseObserver) {
        final String firstName = request.getGreeting().getFirstName();

        try {
            for (int i = 0; i < 10; i++) {
                String result = "Hello " + firstName + ", response number: " + i;
                GreetManyTimesResponse response = GreetManyTimesResponse.newBuilder()
                        .setResult(result)
                        .build();

                responseObserver.onNext(response);
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<LongGreetRequest> longGreet(StreamObserver<LongGreetResponse> responseObserver) {
        // we create the requestObserver that we'll return in this function
        final StreamObserver<LongGreetRequest> requestObserver = new StreamObserver<LongGreetRequest>() {

            String result = "";

            @Override
            public void onNext(LongGreetRequest value) {
                // client sends a message
                result += "Hello " + value.getGreeting().getFirstName() + "! ";
            }

            @Override
            public void onError(Throwable t) {
                // client sends an error
            }

            @Override
            public void onCompleted() {
                // client is done
                responseObserver.onNext(
                        LongGreetResponse.newBuilder()
                                .setResult(result)
                                .build()
                );
                responseObserver.onCompleted();
            }
        };

        return requestObserver;
    }

    @Override
    public StreamObserver<GreetEveryoneRequest> greetEveryone(StreamObserver<GreetEveryoneResponse> responseObserver) {
       final StreamObserver<GreetEveryoneRequest> requestObserver = new StreamObserver<GreetEveryoneRequest>() {
            @Override
            public void onNext(GreetEveryoneRequest value) {
                String result = "Hello " + value.getGreeting().getFirstName();
               final GreetEveryoneResponse greetEveryoneResponse = GreetEveryoneResponse.newBuilder()
                        .setResult(result)
                        .build();

                responseObserver.onNext(greetEveryoneResponse);
            }

            @Override
            public void onError(Throwable t) {
                // do nothing
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };

        return requestObserver;
    }

    @Override
    public void greetWithDeadline(GreetWithDeadlineRequest request, StreamObserver<GreetWithDeadlineResponse> responseObserver) {

        final Context current = Context.current();

        try {

            for (int i = 0; i < 3; i++) {
                if (!current.isCancelled()) {
                    System.out.println("sleep for 100 ms");
                    Thread.sleep(100);
                } else {
                    return;
                }
            }

            System.out.println("send response");
            responseObserver.onNext(
                    GreetWithDeadlineResponse.newBuilder()
                            .setResult("hello " + request.getGreeting().getFirstName())
                            .build()
            );

            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public StreamObserver<FindMaximumRequest> getLargest(StreamObserver<FindMaximumResponse> responseObserver) {

        final StreamObserver<FindMaximumRequest> maximumRequest = new StreamObserver<FindMaximumRequest>() {
            int currentMax = 0;
            @Override
            public void onNext(FindMaximumRequest findMaximumRequest) {

                if (findMaximumRequest.getNumber() > currentMax) {
                    currentMax = findMaximumRequest.getNumber();
                    responseObserver.onNext(FindMaximumResponse.newBuilder()
                            .setNumber(currentMax)
                            .build());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                responseObserver.onCompleted();

            }

            @Override
            public void onCompleted() {
           responseObserver.onCompleted();
            }

        };
        return maximumRequest;
    }

}
