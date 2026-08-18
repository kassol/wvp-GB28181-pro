package com.genersoft.iot.vmp.gb28181.transmit.cmd.impl;

import com.genersoft.iot.vmp.gb28181.SipLayer;
import com.genersoft.iot.vmp.conf.UserSetting;
import com.genersoft.iot.vmp.gb28181.bean.Device;
import com.genersoft.iot.vmp.gb28181.bean.DeviceChannel;
import com.genersoft.iot.vmp.gb28181.bean.SipTransactionInfo;
import com.genersoft.iot.vmp.gb28181.bean.SsrcTransaction;
import com.genersoft.iot.vmp.gb28181.event.SipSubscribe;
import com.genersoft.iot.vmp.gb28181.session.SipInviteSessionManager;
import com.genersoft.iot.vmp.gb28181.transmit.SIPSender;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.SIPRequestHeaderProvider;
import com.genersoft.iot.vmp.media.bean.MediaServer;
import com.genersoft.iot.vmp.service.bean.SSRCInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.ArgumentCaptor;

import gov.nist.javax.sip.message.SIPResponse;
import javax.sip.ResponseEvent;
import javax.sip.SipException;
import javax.sip.header.CallIdHeader;
import javax.sip.header.ViaHeader;
import javax.sip.message.Response;
import javax.sip.message.Request;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.ArgumentMatchers.isNull;
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

    @Mock
    private UserSetting userSetting;

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

    @Test
    void downloadResponseWithoutSsrcKeepsAllocatedSsrc() throws Exception {
        assertDownloadResponseSsrc("v=0\r\nm=video 30000 RTP/AVP 96\r\n", "0100000001");
    }

    @Test
    void downloadResponseWithSsrcUsesResponseSsrc() throws Exception {
        assertDownloadResponseSsrc("v=0\r\nm=video 30000 RTP/AVP 96\r\ny=0200000002\r\n", "0200000002");
    }

    private void assertDownloadResponseSsrc(String responseContent, String expectedSsrc) throws Exception {
        String localIp = "10.0.0.1";
        String senderIp = "127.0.0.1";
        SSRCInfo ssrcInfo = new SSRCInfo(30000, "0100000001", "rtp", "stream-1");

        Device device = new Device();
        device.setDeviceId("device-1");
        device.setLocalIp(localIp);
        device.setTransport("UDP");
        device.setStreamMode("UDP");

        DeviceChannel channel = new DeviceChannel();
        channel.setId(1);
        channel.setDeviceId("channel-1");

        MediaServer mediaServer = new MediaServer();
        mediaServer.setId("media-1");
        mediaServer.setSdpIp("10.0.0.2");

        Request request = org.mockito.Mockito.mock(Request.class);
        CallIdHeader requestCallId = org.mockito.Mockito.mock(CallIdHeader.class);
        CallIdHeader responseCallId = org.mockito.Mockito.mock(CallIdHeader.class);
        ViaHeader responseVia = org.mockito.Mockito.mock(ViaHeader.class);
        SIPResponse response = org.mockito.Mockito.mock(SIPResponse.class);

        when(userSetting.getSeniorSdp()).thenReturn(false);
        when(sipLayer.getLocalIp(localIp)).thenReturn(senderIp);
        when(sipSender.getNewCallIdHeader(senderIp, "UDP")).thenReturn(requestCallId);
        when(headerProvider.createPlaybackInviteRequest(same(device), eq("channel-1"), anyString(), anyString(),
                anyString(), isNull(), same(requestCallId), eq("0100000001"))).thenReturn(request);
        when(response.getRawContent()).thenReturn(responseContent.getBytes());
        when(response.getCallIdHeader()).thenReturn(responseCallId);
        when(responseCallId.getCallId()).thenReturn("response-call-id");
        when(response.getTopmostViaHeader()).thenReturn(responseVia);
        when(responseVia.getBranch()).thenReturn("response-branch");
        doAnswer(invocation -> {
            SipSubscribe.Event okEvent = invocation.getArgument(3);
            ResponseEvent responseEvent = new ResponseEvent(this, null, null, (Response) response);
            okEvent.response(new SipSubscribe.EventResult<>(responseEvent));
            return null;
        }).when(sipSender).transmitRequest(eq(senderIp), same(request), any(), any(), eq(1000L));

        commander.downloadStreamCmd(mediaServer, ssrcInfo, device, channel,
                "2026-08-18 00:00:00", "2026-08-18 00:01:00", 1, event -> { }, event -> { }, 1000L);

        ArgumentCaptor<SsrcTransaction> transactionCaptor = ArgumentCaptor.forClass(SsrcTransaction.class);
        verify(sessionManager).put(transactionCaptor.capture());
        assertEquals(expectedSsrc, transactionCaptor.getValue().getSsrc());
    }
}
