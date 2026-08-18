package com.genersoft.iot.vmp.gb28181.service.impl;

import com.genersoft.iot.vmp.common.InviteInfo;
import com.genersoft.iot.vmp.common.InviteSessionStatus;
import com.genersoft.iot.vmp.common.InviteSessionType;
import com.genersoft.iot.vmp.conf.UserSetting;
import com.genersoft.iot.vmp.gb28181.bean.Device;
import com.genersoft.iot.vmp.gb28181.bean.DeviceChannel;
import com.genersoft.iot.vmp.gb28181.bean.SsrcTransaction;
import com.genersoft.iot.vmp.gb28181.service.IDeviceChannelService;
import com.genersoft.iot.vmp.gb28181.service.IDeviceService;
import com.genersoft.iot.vmp.gb28181.service.IInviteStreamService;
import com.genersoft.iot.vmp.gb28181.session.SipInviteSessionManager;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.ISIPCommander;
import com.genersoft.iot.vmp.media.bean.MediaServer;
import com.genersoft.iot.vmp.media.event.media.MediaDepartureEvent;
import com.genersoft.iot.vmp.service.ISendRtpServerService;
import com.genersoft.iot.vmp.service.bean.InviteErrorCode;
import com.genersoft.iot.vmp.service.bean.SSRCInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PlayServiceImplTest {

    @Mock
    private ISendRtpServerService sendRtpServerService;

    @Mock
    private IInviteStreamService inviteStreamService;

    @Mock
    private SipInviteSessionManager sessionManager;

    @Mock
    private IDeviceService deviceService;

    @Mock
    private IDeviceChannelService deviceChannelService;

    @Mock
    private ISIPCommander commander;

    @Mock
    private com.genersoft.iot.vmp.media.service.IMediaServerService mediaServerService;

    @Mock
    private com.genersoft.iot.vmp.service.IReceiveRtpServerService receiveRtpServerService;

    @Mock
    private UserSetting userSetting;

    @InjectMocks
    private PlayServiceImpl playService;

    @Test
    void departureFromOldSsrcDoesNotMatchCurrentInvite() {
        InviteInfo inviteInfo = inviteInfo("media-1", "0200004903");
        MediaDepartureEvent event = departureEvent("media-1", "rtp://__defaultVhost__/rtp/0BEBDA7A");

        assertFalse(PlayServiceImpl.isCurrentMediaSource(event, inviteInfo));
    }

    @Test
    void departureFromCurrentSsrcMatchesCurrentInvite() {
        InviteInfo inviteInfo = inviteInfo("media-1", "0200004903");
        MediaDepartureEvent event = departureEvent("media-1", "rtp://__defaultVhost__/rtp/0BEBD527");

        assertTrue(PlayServiceImpl.isCurrentMediaSource(event, inviteInfo));
    }

    @Test
    void departureWithoutSourceIdentityRemainsBackwardCompatible() {
        InviteInfo inviteInfo = inviteInfo("media-1", "0200004903");
        MediaDepartureEvent event = departureEvent("media-1", null);

        assertTrue(PlayServiceImpl.isCurrentMediaSource(event, inviteInfo));
    }

    @Test
    void departureWithSourceIdentityDoesNotMatchInviteWithoutSsrc() {
        InviteInfo inviteInfo = new InviteInfo();
        inviteInfo.setMediaServerId("media-1");
        MediaDepartureEvent event = departureEvent("media-1", "rtp://__defaultVhost__/rtp/0BEBDA7A");

        assertFalse(PlayServiceImpl.isCurrentMediaSource(event, inviteInfo));
    }

    @Test
    void departureFromOldSsrcStopsOldDialogWithoutTouchingCurrentInvite() throws Exception {
        String stream = "device-1_channel-1";
        InviteInfo currentInvite = inviteInfo("media-1", "0200004903");
        currentInvite.setStream(stream);
        currentInvite.setStatus(InviteSessionStatus.ok);

        SsrcTransaction oldTransaction = new SsrcTransaction();
        oldTransaction.setDeviceId("device-1");
        oldTransaction.setChannelId(1);
        oldTransaction.setCallId("call-old");
        oldTransaction.setApp("rtp");
        oldTransaction.setStream(stream);
        oldTransaction.setMediaServerId("media-1");
        oldTransaction.setSsrc("0200006266");
        oldTransaction.setType(InviteSessionType.PLAY);

        Device device = new Device();
        device.setDeviceId("device-1");
        DeviceChannel channel = org.mockito.Mockito.mock(DeviceChannel.class);

        MediaDepartureEvent event = departureEvent("media-1", "rtp://__defaultVhost__/rtp/0BEBDA7A");
        event.setApp("rtp");
        event.setStream(stream);
        event.setSchema("rtsp");

        when(sendRtpServerService.queryByStream(stream)).thenReturn(List.of());
        when(inviteStreamService.getInviteInfoByStream(null, stream)).thenReturn(currentInvite);
        when(sessionManager.getAll()).thenReturn(List.of(oldTransaction));
        when(deviceService.getDeviceByDeviceId("device-1")).thenReturn(device);
        when(deviceChannelService.getOneForSourceById(1)).thenReturn(channel);
        when(channel.getDeviceId()).thenReturn("channel-1");

        playService.onApplicationEvent(event);

        verify(commander).streamByeCmd(device, "channel-1", "rtp", stream, "call-old", null);
        verify(inviteStreamService, never()).removeInviteInfo(currentInvite);
    }

    @Test
    void currentDepartureUsesMatchingCallIdWhenStreamIndexAlreadyPointsToNewDialog() throws Exception {
        String stream = "device-1_channel-1";
        InviteInfo departingInvite = inviteInfo("media-1", "0200006266");
        departingInvite.setDeviceId("device-1");
        departingInvite.setChannelId(1);
        departingInvite.setStream(stream);
        departingInvite.setStatus(InviteSessionStatus.ok);

        SsrcTransaction oldTransaction = transaction("call-old", stream, "0200006266");
        SsrcTransaction newTransaction = transaction("call-new", stream, "0200004903");

        Device device = new Device();
        device.setDeviceId("device-1");
        DeviceChannel channel = org.mockito.Mockito.mock(DeviceChannel.class);

        MediaDepartureEvent event = departureEvent("media-1", "rtp://__defaultVhost__/rtp/0BEBDA7A");
        event.setApp("rtp");
        event.setStream(stream);
        event.setSchema("rtsp");

        when(sendRtpServerService.queryByStream(stream)).thenReturn(List.of());
        when(inviteStreamService.getInviteInfoByStream(null, stream)).thenReturn(departingInvite);
        when(sessionManager.getSsrcTransactionByStream("rtp", stream)).thenReturn(newTransaction);
        when(sessionManager.getAll()).thenReturn(List.of(oldTransaction, newTransaction));
        when(deviceChannelService.getOneForSourceById(1)).thenReturn(channel);
        when(channel.getDataDeviceId()).thenReturn(10);
        when(channel.getDeviceId()).thenReturn("channel-1");
        when(deviceService.getDevice(10)).thenReturn(device);

        playService.onApplicationEvent(event);

        verify(commander).streamByeCmd(device, "channel-1", "rtp", stream, "call-old", null);
        verify(inviteStreamService).removeInviteInfo(departingInvite);
    }

    @Test
    void stopWithoutInviteInfoSendsByeForEveryMatchingResidualDialog() throws Exception {
        String stream = "device-1_channel-1";
        Device device = new Device();
        device.setDeviceId("device-1");
        device.setServerId("server-1");
        DeviceChannel channel = org.mockito.Mockito.mock(DeviceChannel.class);

        SsrcTransaction first = transaction("call-1", stream, "0200000001");
        SsrcTransaction second = transaction("call-2", stream, "0200000002");
        SsrcTransaction platformTransaction = transaction("platform-call", stream, "0200000003");
        platformTransaction.setDeviceId(null);
        platformTransaction.setPlatformId("platform-1");

        when(userSetting.getServerId()).thenReturn("server-1");
        when(channel.getId()).thenReturn(1);
        when(channel.getDeviceId()).thenReturn("channel-1");
        when(inviteStreamService.getInviteInfo(InviteSessionType.PLAY, 1, stream)).thenReturn(null);
        when(sessionManager.getAll()).thenReturn(List.of(first, second, platformTransaction));

        playService.stop(InviteSessionType.PLAY, device, channel, stream);

        verify(commander).streamByeCmd(device, "channel-1", "rtp", stream, "call-1", null);
        verify(commander).streamByeCmd(device, "channel-1", "rtp", stream, "call-2", null);
        verify(commander, never()).streamByeCmd(device, "channel-1", "rtp", stream, "platform-call", null);
        verify(deviceChannelService).stopPlay(1);
    }

    @Test
    void stopReportsMissingSessionWhenNeitherInviteNorTransactionExists() {
        String stream = "device-1_channel-1";
        Device device = new Device();
        device.setDeviceId("device-1");
        device.setServerId("server-1");
        DeviceChannel channel = org.mockito.Mockito.mock(DeviceChannel.class);

        when(userSetting.getServerId()).thenReturn("server-1");
        when(channel.getId()).thenReturn(1);
        when(channel.getDeviceId()).thenReturn("channel-1");
        when(inviteStreamService.getInviteInfo(InviteSessionType.PLAY, 1, stream)).thenReturn(null);
        when(sessionManager.getAll()).thenReturn(List.of());

        org.junit.jupiter.api.Assertions.assertThrows(
                com.genersoft.iot.vmp.conf.exception.ControllerException.class,
                () -> playService.stop(InviteSessionType.PLAY, device, channel, stream));
    }

    @Test
    void departureKeepsInviteInfoWhenByeSubmissionFails() throws Exception {
        String stream = "device-1_channel-1";
        InviteInfo inviteInfo = inviteInfo("media-1", "0200006266");
        inviteInfo.setDeviceId("device-1");
        inviteInfo.setChannelId(1);
        inviteInfo.setStream(stream);
        inviteInfo.setStatus(InviteSessionStatus.ok);
        SsrcTransaction transaction = transaction("call-1", stream, "0200006266");

        Device device = new Device();
        device.setDeviceId("device-1");
        DeviceChannel channel = org.mockito.Mockito.mock(DeviceChannel.class);
        MediaDepartureEvent event = departureEvent("media-1", "rtp://__defaultVhost__/rtp/0BEBDA7A");
        event.setApp("rtp");
        event.setStream(stream);
        event.setSchema("rtsp");

        when(sendRtpServerService.queryByStream(stream)).thenReturn(List.of());
        when(inviteStreamService.getInviteInfoByStream(null, stream)).thenReturn(inviteInfo);
        when(sessionManager.getSsrcTransactionByStream("rtp", stream)).thenReturn(transaction);
        when(deviceChannelService.getOneForSourceById(1)).thenReturn(channel);
        when(channel.getDataDeviceId()).thenReturn(10);
        when(channel.getDeviceId()).thenReturn("channel-1");
        when(deviceService.getDevice(10)).thenReturn(device);
        doThrow(new javax.sip.SipException("send failed")).when(commander)
                .streamByeCmd(device, "channel-1", "rtp", stream, "call-1", null);

        playService.onApplicationEvent(event);

        verify(inviteStreamService, never()).removeInviteInfo(inviteInfo);
    }

    private InviteInfo inviteInfo(String mediaServerId, String ssrc) {
        InviteInfo inviteInfo = new InviteInfo();
        inviteInfo.setMediaServerId(mediaServerId);
        inviteInfo.setSsrcInfo(new SSRCInfo(10000, ssrc, "rtp", "stream-1"));
        return inviteInfo;
    }

    @Test
    void secondPlayWithDeadCachedStreamSendsByeToOldDialogFirst() throws Exception {
        String stream = "device-1_channel-1";
        Device device = new Device();
        device.setDeviceId("device-1");
        device.setServerId("server-1");
        device.setStreamMode("UDP");
        DeviceChannel channel = org.mockito.Mockito.mock(DeviceChannel.class);
        when(channel.getId()).thenReturn(1);
        when(channel.getDeviceId()).thenReturn("channel-1");

        // 缓存中的旧点播：流已注册但媒体节点上已不可用
        InviteInfo cachedInvite = inviteInfo("media-1", "0200006266");
        cachedInvite.setStream(stream);
        cachedInvite.setType(InviteSessionType.PLAY);
        cachedInvite.setChannelId(1);
        com.genersoft.iot.vmp.common.StreamInfo streamInfo = new com.genersoft.iot.vmp.common.StreamInfo();
        streamInfo.setStream(stream);
        MediaServer mediaServer = new MediaServer();
        mediaServer.setId("media-1");
        streamInfo.setMediaServer(mediaServer);
        cachedInvite.setStreamInfo(streamInfo);

        SsrcTransaction oldTransaction = transaction("call-old", stream, "0200006266");

        when(inviteStreamService.getInviteInfoByDeviceAndChannel(InviteSessionType.PLAY, 1)).thenReturn(cachedInvite);
        when(mediaServerService.isStreamReady(mediaServer, "rtp", stream)).thenReturn(false);
        when(sessionManager.getSsrcTransactionByStream("rtp", stream)).thenReturn(oldTransaction);
        // 打开RTPServer失败使流程提前返回，本测试只关注旧Dialog处理
        when(receiveRtpServerService.openGbRTPServerForPlay(any(), any(), any(), any(), anyBoolean(), any()))
                .thenReturn(null);

        playService.play(mediaServer, device, channel, null, null, (code, msg, data) -> { });

        InOrder inOrder = inOrder(commander, inviteStreamService);
        inOrder.verify(commander).streamByeCmd(device, "channel-1", "rtp", stream, "call-old", null);
        inOrder.verify(inviteStreamService).removeInviteInfoByDeviceAndChannel(InviteSessionType.PLAY, 1);
    }

    @Test
    void secondPlayAbortsWhenOldDialogCannotBeStopped() throws Exception {
        String stream = "device-1_channel-1";
        Device device = new Device();
        device.setDeviceId("device-1");
        device.setServerId("server-1");
        device.setStreamMode("UDP");
        DeviceChannel channel = org.mockito.Mockito.mock(DeviceChannel.class);
        when(channel.getId()).thenReturn(1);
        when(channel.getDeviceId()).thenReturn("channel-1");

        InviteInfo cachedInvite = inviteInfo("media-1", "0200006266");
        cachedInvite.setStream(stream);
        cachedInvite.setType(InviteSessionType.PLAY);
        cachedInvite.setChannelId(1);
        com.genersoft.iot.vmp.common.StreamInfo streamInfo = new com.genersoft.iot.vmp.common.StreamInfo();
        streamInfo.setStream(stream);
        MediaServer mediaServer = new MediaServer();
        mediaServer.setId("media-1");
        streamInfo.setMediaServer(mediaServer);
        cachedInvite.setStreamInfo(streamInfo);

        SsrcTransaction oldTransaction = transaction("call-old", stream, "0200006266");

        when(inviteStreamService.getInviteInfoByDeviceAndChannel(InviteSessionType.PLAY, 1)).thenReturn(cachedInvite);
        when(mediaServerService.isStreamReady(mediaServer, "rtp", stream)).thenReturn(false);
        when(sessionManager.getSsrcTransactionByStream("rtp", stream)).thenReturn(oldTransaction);
        // BYE未能提交：并发停止、Provider缺失等
        doThrow(new javax.sip.SipException("会话正在停止")).when(commander)
                .streamByeCmd(device, "channel-1", "rtp", stream, "call-old", null);

        final int[] failure = new int[1];
        playService.play(mediaServer, device, channel, null, null, (code, msg, data) -> failure[0] = code);

        // 旧Dialog仍在，不得清理旧状态，也不得建立新会话
        assertEquals(InviteErrorCode.ERROR_FOR_SIP_SENDING_FAILED.getCode(), failure[0]);
        verify(inviteStreamService, never()).removeInviteInfoByDeviceAndChannel(InviteSessionType.PLAY, 1);
        verify(deviceChannelService, never()).stopPlay(1);
        verify(receiveRtpServerService, never())
                .openGbRTPServerForPlay(any(), any(), any(), any(), anyBoolean(), any());
    }

    private MediaDepartureEvent departureEvent(String mediaServerId, String originUrl) {
        MediaServer mediaServer = new MediaServer();
        mediaServer.setId(mediaServerId);
        MediaDepartureEvent event = new MediaDepartureEvent(this);
        event.setMediaServer(mediaServer);
        event.setOriginUrl(originUrl);
        return event;
    }

    private SsrcTransaction transaction(String callId, String stream, String ssrc) {
        SsrcTransaction transaction = new SsrcTransaction();
        transaction.setDeviceId("device-1");
        transaction.setChannelId(1);
        transaction.setCallId(callId);
        transaction.setApp("rtp");
        transaction.setStream(stream);
        transaction.setMediaServerId("media-1");
        transaction.setSsrc(ssrc);
        transaction.setType(InviteSessionType.PLAY);
        return transaction;
    }
}
