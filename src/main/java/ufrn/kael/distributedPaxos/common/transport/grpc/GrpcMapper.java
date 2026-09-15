package ufrn.kael.distributedPaxos.common.transport.grpc;

import ufrn.kael.distributedPaxos.common.message.*;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.*;
import ufrn.kael.distributedPaxos.paxos.actors.ProposalId;

public class GrpcMapper {

    // Java -> Protobuf
    public static GrpcMessage toGrpc(Message message) {
        GrpcMessage.Builder builder = GrpcMessage.newBuilder();

        if (message instanceof ApplicationCommand cmd) {
            builder.setCommand(ApplicationCommandProto.newBuilder()
                    .setMessageId(cmd.messageId())
                    .setSenderId(cmd.senderId())
                    .setTargetId(cmd.targetId())
                    .setOriginId(cmd.originId())
                    .setTransaction(TransactionProto.newBuilder()
                            .setOperation(cmd.transaction().operation())
                            .putAllParameters(cmd.transaction().parameters())
                            .build())
                    .build());
        } 
        else if (message instanceof ApplicationResponse res) {
            builder.setResponse(ApplicationResponseProto.newBuilder()
                    .setMessageId(res.messageId())
                    .setSenderId(res.senderId())
                    .setTargetId(res.targetId())
                    .setOriginId(res.originId())
                    .setSuccess(res.success())
                    .setMessage(res.message())
                    .build());
        }
        else if (message instanceof PaxosCommand.Prepare prepare) {
            builder.setPrepare(PrepareProto.newBuilder()
                    .setMessageId(prepare.messageId())
                    .setSenderId(prepare.senderId())
                    .setTargetId(prepare.targetId())
                    .setProposalId(ProposalIdProto.newBuilder()
                            .setNumber(prepare.proposalId().number())
                            .setNodeId(prepare.proposalId().nodeId())
                            .build())
                    .build());
        }
        else if (message instanceof PaxosCommand.Promise promise) {
            PromiseProto.Builder promiseBuilder = PromiseProto.newBuilder()
                    .setMessageId(promise.messageId())
                    .setSenderId(promise.senderId())
                    .setTargetId(promise.targetId())
                    .setProposalId(ProposalIdProto.newBuilder()
                            .setNumber(promise.proposalId().number())
                            .setNodeId(promise.proposalId().nodeId())
                            .build());

            if (promise.highestAcceptedId() != null) {
                promiseBuilder.setHighestAcceptedId(
                        ProposalIdProto.newBuilder()
                                .setNumber(promise.highestAcceptedId().number())
                                .setNodeId(promise.highestAcceptedId().nodeId())
                                .build()
                );
            }

            if (promise.acceptedValue() != null) {
                promiseBuilder.setAcceptedValue(
                        TransactionProto.newBuilder()
                                .setOperation(promise.acceptedValue().operation())
                                .putAllParameters(promise.acceptedValue().parameters())
                                .build()
                );
            }

            builder.setPromise(promiseBuilder.build());
        }
        else if (message instanceof PaxosCommand.Accept accept) {
            builder.setAccept(AcceptProto.newBuilder()
                    .setMessageId(accept.messageId())
                    .setSenderId(accept.senderId())
                    .setTargetId(accept.targetId())
                    .setProposalId(ProposalIdProto.newBuilder()
                            .setNumber(accept.proposalId().number())
                            .setNodeId(accept.proposalId().nodeId())
                            .build())
                    .setValue(TransactionProto.newBuilder()
                            .setOperation(accept.value().operation())
                            .putAllParameters(accept.value().parameters())
                            .build())
                    .build());
        }
        else if (message instanceof PaxosCommand.Accepted accepted) {
            builder.setAccepted(AcceptedProto.newBuilder()
                    .setMessageId(accepted.messageId())
                    .setSenderId(accepted.senderId())
                    .setTargetId(accepted.targetId())
                    .setProposalId(ProposalIdProto.newBuilder()
                            .setNumber(accepted.proposalId().number())
                            .setNodeId(accepted.proposalId().nodeId())
                            .build())
                    .setValue(TransactionProto.newBuilder()
                            .setOperation(accepted.value().operation())
                            .putAllParameters(accepted.value().parameters())
                            .build())
                    .build());
        }        
        
        return builder.build();
    }

    // Protobuf -> Java
    public static Message toJava(GrpcMessage grpcMessage) {

        switch (grpcMessage.getPayloadCase()) {

            case COMMAND:

                ApplicationCommandProto cmd = grpcMessage.getCommand();

                Transaction cmdTransaction = new Transaction(
                        cmd.getTransaction().getOperation(),
                        cmd.getTransaction().getParametersMap()
                );

                return new ApplicationCommand(
                        cmd.getMessageId(),
                        cmd.getSenderId(),
                        cmd.getTargetId(),
                        cmd.getOriginId(),
                        cmdTransaction
                );

            case RESPONSE:

                ApplicationResponseProto res = grpcMessage.getResponse();

                return new ApplicationResponse(
                        res.getMessageId(),
                        res.getSenderId(),
                        res.getTargetId(),
                        res.getOriginId(),
                        res.getSuccess(),
                        res.getMessage()
                );

            case PREPARE:

                PrepareProto prepare = grpcMessage.getPrepare();

                ProposalId prepareProposalId = new ProposalId(
                        prepare.getProposalId().getNumber(),
                        prepare.getProposalId().getNodeId()
                );

                return new PaxosCommand.Prepare(
                        prepare.getMessageId(),
                        prepare.getSenderId(),
                        prepare.getTargetId(),
                        prepareProposalId
                );

            case PROMISE:

                PromiseProto promise = grpcMessage.getPromise();

                ProposalId promiseProposalId = new ProposalId(
                        promise.getProposalId().getNumber(),
                        promise.getProposalId().getNodeId()
                );

                ProposalId highestAcceptedId = null;

                if (promise.hasHighestAcceptedId()) {
                    highestAcceptedId = new ProposalId(
                            promise.getHighestAcceptedId().getNumber(),
                            promise.getHighestAcceptedId().getNodeId()
                    );
                }

                Transaction acceptedValue = null;

                if (promise.hasAcceptedValue()) {
                    acceptedValue = new Transaction(
                            promise.getAcceptedValue().getOperation(),
                            promise.getAcceptedValue().getParametersMap()
                    );
                }

                return new PaxosCommand.Promise(
                        promise.getMessageId(),
                        promise.getSenderId(),
                        promise.getTargetId(),
                        promiseProposalId,
                        highestAcceptedId,
                        acceptedValue
                );

            case ACCEPT:

                AcceptProto accept = grpcMessage.getAccept();

                ProposalId acceptProposalId = new ProposalId(
                        accept.getProposalId().getNumber(),
                        accept.getProposalId().getNodeId()
                );

                Transaction acceptValue = new Transaction(
                        accept.getValue().getOperation(),
                        accept.getValue().getParametersMap()
                );

                return new PaxosCommand.Accept(
                        accept.getMessageId(),
                        accept.getSenderId(),
                        accept.getTargetId(),
                        acceptProposalId,
                        acceptValue
                );

            case ACCEPTED:

                AcceptedProto accepted = grpcMessage.getAccepted();

                ProposalId acceptedProposalId = new ProposalId(
                        accepted.getProposalId().getNumber(),
                        accepted.getProposalId().getNodeId()
                );

                Transaction acceptedTransaction = new Transaction(
                        accepted.getValue().getOperation(),
                        accepted.getValue().getParametersMap()
                );

                return new PaxosCommand.Accepted(
                        accepted.getMessageId(),
                        accepted.getSenderId(),
                        accepted.getTargetId(),
                        acceptedProposalId,
                        acceptedTransaction
                );

            case PAYLOAD_NOT_SET:
            default:
                throw new IllegalArgumentException(
                        "GrpcMessage does not contain a payload."
                );
        }
    }
}