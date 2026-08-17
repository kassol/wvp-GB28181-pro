package com.genersoft.iot.vmp.gb28181.service.impl;

import com.genersoft.iot.vmp.common.InviteSessionType;
import com.genersoft.iot.vmp.gb28181.bean.Device;
import com.genersoft.iot.vmp.gb28181.bean.DeviceChannel;
import com.genersoft.iot.vmp.gb28181.bean.SsrcTransaction;
import com.genersoft.iot.vmp.gb28181.service.IDeviceChannelService;
import com.genersoft.iot.vmp.gb28181.service.IDeviceService;
import com.genersoft.iot.vmp.gb28181.session.SipInviteSessionManager;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.ISIPCommander;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrphanRtpCleanupServiceTest {

    @Mock
    private SipInviteSessionManager sessionManager;

    @Mock
    private IDeviceService deviceService;

    @Mock
    private IDeviceChannelService deviceChannelService;

    @Mock
    private ISIPCommander cmder;

    private OrphanRtpCleanupService service;

    @BeforeEach
    void setUp() {
        service = createService(100);
    }

    @AfterEach
    void tearDown() {
        service.shutdown();
    }

    @Test
    void sameSsrcOnDifferentMediaServerOnlyCleansMatchingServer() throws Exception {
        SsrcTransaction otherServer = buildTransaction("zlm-2", "stream-2", "callid-2", InviteSessionType.PLAY);
        SsrcTransaction transaction = buildTransaction("zlm-1", "stream-1", "callid-1", InviteSessionType.PLAY);
        when(sessionManager.getAll()).thenReturn(List.of(otherServer, transaction));
        Device device = prepareDevice(transaction);

        service.onRtpPublishRejected("zlm-1", "00000C14");

        verify(cmder, timeout(2000)).streamByeCmd(eq(device), eq("channel-11"), eq(transaction.getApp()),
                eq(transaction.getStream()), eq(transaction.getCallId()), isNull());
    }

    @Test
    void platformAndTalkCandidatesBeforeDevicePlayAreIgnored() throws Exception {
        SsrcTransaction platform = buildTransaction("zlm-1", "platform-stream", "platform-call", InviteSessionType.PLAY);
        platform.setPlatformId("platform-1");
        SsrcTransaction talk = buildTransaction("zlm-1", "talk-stream", "talk-call", InviteSessionType.TALK);
        SsrcTransaction transaction = buildTransaction("zlm-1", "play-stream", "play-call", InviteSessionType.PLAYBACK);
        when(sessionManager.getAll()).thenReturn(List.of(platform, talk, transaction));
        Device device = prepareDevice(transaction);

        service.onRtpPublishRejected("zlm-1", "00000C14");

        verify(cmder, timeout(2000)).streamByeCmd(eq(device), eq("channel-11"), eq(transaction.getApp()),
                eq(transaction.getStream()), eq(transaction.getCallId()), isNull());
    }

    @Test
    void twoCallIdsForSameStreamAreAmbiguousAndSendNoBye() throws Exception {
        SsrcTransaction first = buildTransaction("zlm-1", "device-stream", "callid-1", InviteSessionType.PLAY);
        SsrcTransaction second = buildTransaction("zlm-1", "device-stream", "callid-2", InviteSessionType.DOWNLOAD);
        when(sessionManager.getAll()).thenReturn(List.of(first, second));

        service.onRtpPublishRejected("zlm-1", "device-stream");
        verify(sessionManager, timeout(2000)).getAll();

        verify(cmder, timeout(500).times(0)).streamByeCmd(any(), any(), any(), any(), any(), any());
    }

    @Test
    void repeatedPendingRejectionRunsOneCleanup() throws Exception {
        SsrcTransaction transaction = buildTransaction("zlm-1", "device-stream", "callid-1", InviteSessionType.PLAY);
        Device device = prepareDevice(transaction);
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        CountDownLatch allowCleanup = new CountDownLatch(1);
        when(sessionManager.getAll()).thenAnswer(invocation -> {
            cleanupStarted.countDown();
            assertTrue(allowCleanup.await(2, TimeUnit.SECONDS));
            return Collections.singletonList(transaction);
        });

        service.onRtpPublishRejected("zlm-1", "device-stream");
        assertTrue(cleanupStarted.await(2, TimeUnit.SECONDS));
        service.onRtpPublishRejected("zlm-1", "device-stream");
        allowCleanup.countDown();

        verify(cmder, timeout(2000)).streamByeCmd(eq(device), eq("channel-11"), any(), any(), any(), isNull());
        verify(sessionManager, times(1)).getAll();
    }

    @Test
    void rejectedQueueEntryCanBeSubmittedByNextHook() throws Exception {
        service.shutdown();
        service = createService(1);
        SsrcTransaction transaction = buildTransaction("zlm-1", "rejected-stream", "callid-1", InviteSessionType.PLAY);
        Device device = prepareDevice(transaction);
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        CountDownLatch allowCleanup = new CountDownLatch(1);
        AtomicInteger cleanupCount = new AtomicInteger();
        when(sessionManager.getAll()).thenAnswer(invocation -> {
            int count = cleanupCount.incrementAndGet();
            if (count == 1) {
                cleanupStarted.countDown();
                assertTrue(allowCleanup.await(2, TimeUnit.SECONDS));
            }
            return count == 3 ? Collections.singletonList(transaction) : Collections.emptyList();
        });

        service.onRtpPublishRejected("zlm-1", "running-stream");
        assertTrue(cleanupStarted.await(2, TimeUnit.SECONDS));
        service.onRtpPublishRejected("zlm-1", "queued-stream");
        service.onRtpPublishRejected("zlm-1", "rejected-stream");
        allowCleanup.countDown();
        verify(sessionManager, timeout(2000).times(2)).getAll();

        service.onRtpPublishRejected("zlm-1", "rejected-stream");

        verify(cmder, timeout(2000)).streamByeCmd(eq(device), eq("channel-11"), any(),
                eq(transaction.getStream()), eq(transaction.getCallId()), isNull());
    }

    @Test
    void eightDigitHexStreamDoesNotFallBackToStreamId() throws Exception {
        SsrcTransaction transaction = buildTransaction("zlm-1", "00000C14", "callid-1", InviteSessionType.PLAY);
        transaction.setSsrc("9999");
        when(sessionManager.getAll()).thenReturn(Collections.singletonList(transaction));

        service.onRtpPublishRejected("zlm-1", "00000C14");
        verify(sessionManager, timeout(2000)).getAll();

        verify(cmder, timeout(500).times(0)).streamByeCmd(any(), any(), any(), any(), any(), any());
    }

    private OrphanRtpCleanupService createService(int queueCapacity) {
        OrphanRtpCleanupService cleanupService = new OrphanRtpCleanupService(queueCapacity);
        ReflectionTestUtils.setField(cleanupService, "sessionManager", sessionManager);
        ReflectionTestUtils.setField(cleanupService, "deviceService", deviceService);
        ReflectionTestUtils.setField(cleanupService, "deviceChannelService", deviceChannelService);
        ReflectionTestUtils.setField(cleanupService, "cmder", cmder);
        return cleanupService;
    }

    private SsrcTransaction buildTransaction(String mediaServerId, String stream, String callId,
                                             InviteSessionType type) {
        SsrcTransaction transaction = new SsrcTransaction();
        transaction.setDeviceId("device-1");
        transaction.setChannelId(11);
        transaction.setCallId(callId);
        transaction.setApp("rtp");
        transaction.setStream(stream);
        transaction.setMediaServerId(mediaServerId);
        transaction.setSsrc("0000003092");
        transaction.setType(type);
        return transaction;
    }

    private Device prepareDevice(SsrcTransaction transaction) {
        Device device = new Device();
        device.setDeviceId(transaction.getDeviceId());
        DeviceChannel channel = new DeviceChannel();
        channel.setDeviceId("channel-11");
        when(deviceService.getDeviceByDeviceId(transaction.getDeviceId())).thenReturn(device);
        when(deviceChannelService.getOneForSourceById(transaction.getChannelId())).thenReturn(channel);
        return device;
    }
}
