package com.genersoft.iot.vmp.gb28181.transmit.cmd.impl;

import com.genersoft.iot.vmp.gb28181.SipLayer;
import com.genersoft.iot.vmp.gb28181.bean.Device;
import com.genersoft.iot.vmp.gb28181.bean.SipTransactionInfo;
import com.genersoft.iot.vmp.gb28181.bean.SsrcTransaction;
import com.genersoft.iot.vmp.gb28181.event.SipSubscribe;
import com.genersoft.iot.vmp.gb28181.session.SipInviteSessionManager;
import com.genersoft.iot.vmp.gb28181.transmit.SIPSender;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.SIPRequestHeaderProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sip.SipException;
import javax.sip.message.Request;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SIPCommanderTest {

    @Mock
    private SipLayer sipLayer;

    @Mock
    private SIPSender sipSender;

    @Mock
    private SIPRequestHeaderProvider headerProvider;

    @Mock
    private SipInviteSessionManager sessionManager;

    @InjectMocks
    private SIPCommander commander;

    @Test
    void streamByeCmdKeepsTransactionWhenSendFails() throws Exception {
        String callId = "call-1";
        String channelId = "channel-1";
        String localIp = "10.0.0.1";
        String senderIp = "127.0.0.1";

        Device device = new Device();
        device.setDeviceId("device-1");
        device.setLocalIp(localIp);

        SipTransactionInfo transactionInfo = new SipTransactionInfo();
        SsrcTransaction transaction = new SsrcTransaction();
        transaction.setCallId(callId);
        transaction.setApp("rtp");
        transaction.setStream("stream-1");
        transaction.setSipTransactionInfo(transactionInfo);

        Request request = org.mockito.Mockito.mock(Request.class);

        when(sessionManager.getSsrcTransactionByCallId(callId)).thenReturn(transaction);
        when(headerProvider.createByteRequest(device, channelId, transactionInfo)).thenReturn(request);
        when(sipLayer.getLocalIp(localIp)).thenReturn(senderIp);

        doThrow(new SipException("send failed"))
                .when(sipSender)
                .transmitRequestWithResult(
                        eq(senderIp),
                        same(request),
                        ArgumentMatchers.<SipSubscribe.Event>isNull(),
                        ArgumentMatchers.<SipSubscribe.Event>isNull());

        assertThrows(SipException.class, () -> commander.streamByeCmd(
                device, channelId, "rtp", "stream-1", callId, null));

        verify(sipSender).transmitRequestWithResult(
                eq(senderIp),
                same(request),
                ArgumentMatchers.<SipSubscribe.Event>isNull(),
                ArgumentMatchers.<SipSubscribe.Event>isNull());
        verify(sessionManager, never()).removeByCallId(callId);
    }

    @Test
    void streamByeCmdKeepsTransactionWhenProviderIsMissing() throws Exception {
        String callId = "call-2";
        String channelId = "channel-1";
        String localIp = "10.0.0.1";
        String senderIp = "127.0.0.1";

        Device device = new Device();
        device.setDeviceId("device-1");
        device.setLocalIp(localIp);

        SipTransactionInfo transactionInfo = new SipTransactionInfo();
        SsrcTransaction transaction = new SsrcTransaction();
        transaction.setCallId(callId);
        transaction.setApp("rtp");
        transaction.setStream("stream-1");
        transaction.setSipTransactionInfo(transactionInfo);

        Request request = org.mockito.Mockito.mock(Request.class);

        when(sessionManager.getSsrcTransactionByCallId(callId)).thenReturn(transaction);
        when(headerProvider.createByteRequest(device, channelId, transactionInfo)).thenReturn(request);
        when(sipLayer.getLocalIp(localIp)).thenReturn(senderIp);
        when(sipSender.transmitRequestWithResult(senderIp, request, null, null)).thenReturn(false);

        assertThrows(SipException.class, () -> commander.streamByeCmd(
                device, channelId, "rtp", "stream-1", callId, null));

        verify(sessionManager, never()).removeByCallId(callId);
    }

    @Test
    void concurrentStopSubmitsOnlyOneByeForSameCallId() throws Exception {
        String callId = "call-concurrent";
        String channelId = "channel-1";
        String localIp = "10.0.0.1";
        String senderIp = "127.0.0.1";

        Device device = new Device();
        device.setDeviceId("device-1");
        device.setLocalIp(localIp);

        SipTransactionInfo transactionInfo = new SipTransactionInfo();
        SsrcTransaction transaction = new SsrcTransaction();
        transaction.setCallId(callId);
        transaction.setApp("rtp");
        transaction.setStream("stream-1");
        transaction.setSipTransactionInfo(transactionInfo);

        Request request = org.mockito.Mockito.mock(Request.class);
        CountDownLatch sendStarted = new CountDownLatch(1);
        CountDownLatch allowSend = new CountDownLatch(1);

        when(sessionManager.getSsrcTransactionByCallId(callId)).thenReturn(transaction);
        when(headerProvider.createByteRequest(device, channelId, transactionInfo)).thenReturn(request);
        when(sipLayer.getLocalIp(localIp)).thenReturn(senderIp);
        doAnswer(invocation -> {
            sendStarted.countDown();
            if (!allowSend.await(2, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting to release SIP send");
            }
            return true;
        }).when(sipSender).transmitRequestWithResult(senderIp, request, null, null);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> {
                commander.streamByeCmd(device, channelId, "rtp", "stream-1", callId, null);
                return null;
            });
            org.junit.jupiter.api.Assertions.assertTrue(sendStarted.await(2, TimeUnit.SECONDS));
            Future<?> second = executor.submit(() -> {
                commander.streamByeCmd(device, channelId, "rtp", "stream-1", callId, null);
                return null;
            });

            java.util.concurrent.ExecutionException duplicateStop = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> second.get(2, TimeUnit.SECONDS));
            org.junit.jupiter.api.Assertions.assertInstanceOf(SipException.class, duplicateStop.getCause());
            allowSend.countDown();
            first.get(2, TimeUnit.SECONDS);
        } finally {
            allowSend.countDown();
            executor.shutdownNow();
        }

        verify(sipSender).transmitRequestWithResult(senderIp, request, null, null);
        verify(sessionManager).removeByCallId(callId);
    }
}
