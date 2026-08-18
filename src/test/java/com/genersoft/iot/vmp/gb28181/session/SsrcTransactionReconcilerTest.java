package com.genersoft.iot.vmp.gb28181.session;

import com.genersoft.iot.vmp.common.InviteInfo;
import com.genersoft.iot.vmp.common.InviteSessionType;
import com.genersoft.iot.vmp.gb28181.bean.Device;
import com.genersoft.iot.vmp.gb28181.bean.DeviceChannel;
import com.genersoft.iot.vmp.gb28181.bean.SsrcTransaction;
import com.genersoft.iot.vmp.gb28181.service.IDeviceChannelService;
import com.genersoft.iot.vmp.gb28181.service.IDeviceService;
import com.genersoft.iot.vmp.gb28181.service.IInviteStreamService;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.ISIPCommander;
import com.genersoft.iot.vmp.service.bean.SSRCInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sip.SipException;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SsrcTransactionReconcilerTest {

    @Mock
    private SipInviteSessionManager sessionManager;

    @Mock
    private IInviteStreamService inviteStreamService;

    @Mock
    private IDeviceService deviceService;

    @Mock
    private IDeviceChannelService deviceChannelService;

    @Mock
    private ISIPCommander cmder;

    @InjectMocks
    private SsrcTransactionReconciler reconciler;

    private SsrcTransaction deviceTransaction(String callId) {
        SsrcTransaction transaction = new SsrcTransaction();
        transaction.setDeviceId("42010000001320000001");
        transaction.setChannelId(7);
        transaction.setCallId(callId);
        transaction.setApp("rtp");
        transaction.setStream("42010000001320000001_42010000001320000001");
        transaction.setSsrc("0000003092");
        transaction.setMediaServerId("media-1");
        transaction.setType(InviteSessionType.PLAY);
        return transaction;
    }

    private InviteInfo inviteInfo(SsrcTransaction transaction, String ssrc, String mediaServerId) {
        InviteInfo inviteInfo = new InviteInfo();
        inviteInfo.setStream(transaction.getStream());
        inviteInfo.setSsrcInfo(new SSRCInfo(30000, ssrc, transaction.getApp(), transaction.getStream()));
        inviteInfo.setMediaServerId(mediaServerId);
        return inviteInfo;
    }

    private Device stubDeviceAndChannel(SsrcTransaction transaction) {
        Device device = new Device();
        device.setDeviceId(transaction.getDeviceId());
        DeviceChannel channel = new DeviceChannel();
        channel.setDeviceId("42010000001320000001");
        when(deviceService.getDeviceByDeviceId(transaction.getDeviceId())).thenReturn(device);
        when(deviceChannelService.getOneForSourceById(transaction.getChannelId())).thenReturn(channel);
        return device;
    }

    @Test
    void orphanTransactionGetsByeOnSecondCycle() throws Exception {
        SsrcTransaction transaction = deviceTransaction("callid-orphan");
        when(sessionManager.getAll()).thenReturn(Collections.singletonList(transaction));
        when(inviteStreamService.getInviteInfo(transaction.getType(), transaction.getChannelId(),
                transaction.getStream())).thenReturn(null);
        Device device = stubDeviceAndChannel(transaction);

        reconciler.reconcile();
        verify(cmder, never()).streamByeCmd(any(), any(), any(), any(), any(), any());

        reconciler.reconcile();
        verify(cmder).streamByeCmd(eq(device), eq(transaction.getDeviceId()), eq(transaction.getApp()),
                eq(transaction.getStream()), eq(transaction.getCallId()), isNull());
    }

    @Test
    void oldAndNewTransactionsForSameStreamOnlyCleanOld() throws Exception {
        SsrcTransaction oldTransaction = deviceTransaction("callid-old");
        SsrcTransaction newTransaction = deviceTransaction("callid-new");
        InviteInfo inviteInfo = inviteInfo(newTransaction, "3092", newTransaction.getMediaServerId());
        when(sessionManager.getAll()).thenReturn(Arrays.asList(oldTransaction, newTransaction));
        when(inviteStreamService.getInviteInfo(any(), any(), any())).thenReturn(inviteInfo);
        when(sessionManager.getSsrcTransactionByStream(newTransaction.getApp(), newTransaction.getStream()))
                .thenReturn(newTransaction);
        stubDeviceAndChannel(oldTransaction);

        reconciler.reconcile();
        reconciler.reconcile();

        verify(cmder).streamByeCmd(any(), any(), any(), any(), eq(oldTransaction.getCallId()), isNull());
        verify(cmder, never()).streamByeCmd(any(), any(), any(), any(), eq(newTransaction.getCallId()), any());
    }

    @Test
    void mismatchedInviteSsrcGetsByeOnSecondCycle() throws Exception {
        SsrcTransaction transaction = deviceTransaction("callid-ssrc-mismatch");
        InviteInfo inviteInfo = inviteInfo(transaction, "3093", transaction.getMediaServerId());
        when(sessionManager.getAll()).thenReturn(Collections.singletonList(transaction));
        when(inviteStreamService.getInviteInfo(any(), any(), any())).thenReturn(inviteInfo);
        when(sessionManager.getSsrcTransactionByStream(transaction.getApp(), transaction.getStream()))
                .thenReturn(transaction);
        stubDeviceAndChannel(transaction);

        reconciler.reconcile();
        reconciler.reconcile();

        verify(cmder).streamByeCmd(any(), any(), any(), any(), eq(transaction.getCallId()), isNull());
    }

    @Test
    void mismatchedInviteMediaServerGetsByeOnSecondCycle() throws Exception {
        SsrcTransaction transaction = deviceTransaction("callid-media-mismatch");
        InviteInfo inviteInfo = inviteInfo(transaction, "3092", "media-2");
        when(sessionManager.getAll()).thenReturn(Collections.singletonList(transaction));
        when(inviteStreamService.getInviteInfo(any(), any(), any())).thenReturn(inviteInfo);
        when(sessionManager.getSsrcTransactionByStream(transaction.getApp(), transaction.getStream()))
                .thenReturn(transaction);
        stubDeviceAndChannel(transaction);

        reconciler.reconcile();
        reconciler.reconcile();

        verify(cmder).streamByeCmd(any(), any(), any(), any(), eq(transaction.getCallId()), isNull());
    }

    @Test
    void completeIdentityMatchIsLeftAlone() throws Exception {
        SsrcTransaction transaction = deviceTransaction("callid-current");
        InviteInfo inviteInfo = inviteInfo(transaction, "3092", transaction.getMediaServerId());
        when(sessionManager.getAll()).thenReturn(Collections.singletonList(transaction));
        when(inviteStreamService.getInviteInfo(any(), any(), any())).thenReturn(inviteInfo);
        when(sessionManager.getSsrcTransactionByStream(transaction.getApp(), transaction.getStream()))
                .thenReturn(transaction);

        reconciler.reconcile();
        reconciler.reconcile();

        verify(cmder, never()).streamByeCmd(any(), any(), any(), any(), any(), any());
    }

    @Test
    void incompleteCurrentDownloadIdentityIsLeftAlone() throws Exception {
        SsrcTransaction transaction = deviceTransaction("callid-download-incomplete");
        transaction.setType(InviteSessionType.DOWNLOAD);
        transaction.setSsrc(null);
        InviteInfo inviteInfo = inviteInfo(transaction, "3092", transaction.getMediaServerId());
        when(sessionManager.getAll()).thenReturn(Collections.singletonList(transaction));
        when(inviteStreamService.getInviteInfo(any(), any(), any())).thenReturn(inviteInfo);
        when(sessionManager.getSsrcTransactionByStream(transaction.getApp(), transaction.getStream()))
                .thenReturn(transaction);

        reconciler.reconcile();
        reconciler.reconcile();

        transaction.setSsrc("0000003092");
        inviteInfo.setMediaServerId(null);
        reconciler.reconcile();
        reconciler.reconcile();

        verify(cmder, never()).streamByeCmd(any(), any(), any(), any(), any(), any());
    }

    @Test
    void ambiguousCurrentTransactionDoesNotCleanAnyDialog() throws Exception {
        SsrcTransaction transaction = deviceTransaction("callid-ambiguous");
        InviteInfo inviteInfo = inviteInfo(transaction, "3093", "media-2");
        when(sessionManager.getAll()).thenReturn(Collections.singletonList(transaction));
        when(inviteStreamService.getInviteInfo(any(), any(), any())).thenReturn(inviteInfo);
        when(sessionManager.getSsrcTransactionByStream(transaction.getApp(), transaction.getStream()))
                .thenReturn(null);

        reconciler.reconcile();
        reconciler.reconcile();

        verify(cmder, never()).streamByeCmd(any(), any(), any(), any(), any(), any());
    }

    @Test
    void failedByeIsRetriedDirectlyOnNextCycle() throws Exception {
        SsrcTransaction transaction = deviceTransaction("callid-retry");
        when(sessionManager.getAll()).thenReturn(Collections.singletonList(transaction));
        when(inviteStreamService.getInviteInfo(any(), any(), any())).thenReturn(null);
        stubDeviceAndChannel(transaction);
        doThrow(new SipException("send failed")).doNothing().when(cmder)
                .streamByeCmd(any(), any(), any(), any(), eq(transaction.getCallId()), isNull());

        reconciler.reconcile();
        reconciler.reconcile();
        reconciler.reconcile();

        verify(cmder, times(2)).streamByeCmd(any(), any(), any(), any(), eq(transaction.getCallId()), isNull());
    }

    @Test
    void missingDeviceRemovesTransactionWithoutBye() throws Exception {
        SsrcTransaction transaction = deviceTransaction("callid-missing-device");
        when(sessionManager.getAll()).thenReturn(Collections.singletonList(transaction));
        when(inviteStreamService.getInviteInfo(any(), any(), any())).thenReturn(null);
        when(deviceService.getDeviceByDeviceId(transaction.getDeviceId())).thenReturn(null);

        reconciler.reconcile();
        reconciler.reconcile();

        verify(cmder, never()).streamByeCmd(any(), any(), any(), any(), any(), any());
        verify(sessionManager).removeByCallId(transaction.getCallId());
    }

    @Test
    void platformAndTalkTransactionsAreIgnored() throws Exception {
        SsrcTransaction platformTransaction = deviceTransaction("callid-platform");
        platformTransaction.setDeviceId(null);
        platformTransaction.setPlatformId("44010200492000000001");
        SsrcTransaction talkTransaction = deviceTransaction("callid-talk");
        talkTransaction.setType(InviteSessionType.TALK);
        when(sessionManager.getAll()).thenReturn(Arrays.asList(platformTransaction, talkTransaction));

        reconciler.reconcile();
        reconciler.reconcile();

        verify(cmder, never()).streamByeCmd(any(), any(), any(), any(), any(), any());
        verify(inviteStreamService, never()).getInviteInfo(any(), any(), any());
    }
}