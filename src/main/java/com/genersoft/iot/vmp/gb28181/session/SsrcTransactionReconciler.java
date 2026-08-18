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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 点播事务对账。
 *
 * SIP事务信息（SsrcTransaction）与点播状态（InviteInfo）分别保存在Redis中，
 * 服务重启、媒体节点重启、异常分支等场景可能出现点播状态已被清除、
 * 但SIP事务仍残留的情况。残留事务对应的设备并不知道点播已结束，会持续推流。
 *
 * 定期扫描事务列表，对已经没有对应点播状态的事务在原Dialog上补发BYE，
 * 通知设备停止推流并清除事务。
 *
 * 事务在成功建立点播（收到200 OK）时才会写入，此时点播状态必然已存在，
 * 因此“事务存在而点播状态不存在”即可判定为残留，不存在误判正在建立中的点播。
 * 为避免与正常停止流程（BYE已发出但清理未完成）竞争，首次发现的残留事务
 * 仅做标记，第二个扫描周期仍存在时才补发BYE。
 */
@Slf4j
@Component
public class SsrcTransactionReconciler {

    @Autowired
    private SipInviteSessionManager sessionManager;

    @Autowired
    private IInviteStreamService inviteStreamService;

    @Autowired
    private IDeviceService deviceService;

    @Autowired
    private IDeviceChannelService deviceChannelService;

    @Autowired
    private ISIPCommander cmder;

    private final Map<String, Long> pendingCallIds = new ConcurrentHashMap<>();

    @Scheduled(fixedDelay = 5, timeUnit = TimeUnit.MINUTES)
    public void reconcile() {
        Set<String> existingCallIds = new HashSet<>();
        for (SsrcTransaction transaction : sessionManager.getAll()) {
            if (!isReconcilable(transaction)) {
                continue;
            }
            existingCallIds.add(transaction.getCallId());
            InviteInfo inviteInfo = inviteStreamService.getInviteInfo(
                    transaction.getType(), transaction.getChannelId(), transaction.getStream());
            if (!shouldCleanup(transaction, inviteInfo)) {
                pendingCallIds.remove(transaction.getCallId());
                continue;
            }
            Long firstSeen = pendingCallIds.putIfAbsent(transaction.getCallId(), System.currentTimeMillis());
            if (firstSeen == null) {
                // 首次发现，下个周期仍存在才处理，避免与正在进行的停止流程竞争
                continue;
            }
            if (cleanup(transaction)) {
                pendingCallIds.remove(transaction.getCallId());
            }
        }
        pendingCallIds.keySet().removeIf(callId -> !existingCallIds.contains(callId));
    }

    private boolean shouldCleanup(SsrcTransaction transaction, InviteInfo inviteInfo) {
        if (inviteInfo == null) {
            return true;
        }
        SsrcTransaction currentTransaction = sessionManager.getSsrcTransactionByStream(
                transaction.getApp(), transaction.getStream());
        if (currentTransaction == null || ObjectUtils.isEmpty(currentTransaction.getCallId())) {
            log.warn("[事务对账] 无法确定当前事务, 跳过清理, app: {}, stream: {}, callId: {}",
                    transaction.getApp(), transaction.getStream(), transaction.getCallId());
            return false;
        }
        if (!transaction.getCallId().equals(currentTransaction.getCallId())) {
            return true;
        }
        String inviteSsrc = inviteInfo.getSsrcInfo() == null ? null : inviteInfo.getSsrcInfo().getSsrc();
        if (ObjectUtils.isEmpty(inviteSsrc) || ObjectUtils.isEmpty(transaction.getSsrc())
                || ObjectUtils.isEmpty(inviteInfo.getMediaServerId())
                || ObjectUtils.isEmpty(transaction.getMediaServerId())) {
            log.warn("[事务对账] 当前事务身份信息不足, 跳过清理, app: {}, stream: {}, callId: {}",
                    transaction.getApp(), transaction.getStream(), transaction.getCallId());
            return false;
        }
        return !sameDecimalSsrc(inviteSsrc, transaction.getSsrc())
                || !Objects.equals(inviteInfo.getMediaServerId(), transaction.getMediaServerId());
    }

    private static boolean sameDecimalSsrc(String first, String second) {
        if (ObjectUtils.isEmpty(first) || ObjectUtils.isEmpty(second)) {
            return false;
        }
        try {
            return Long.parseLong(first) == Long.parseLong(second);
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    /**
     * 对残留事务在原Dialog上补发BYE并清除事务。
     */
    public boolean cleanup(SsrcTransaction transaction) {
        Device device = deviceService.getDeviceByDeviceId(transaction.getDeviceId());
        DeviceChannel channel = transaction.getChannelId() == null
                ? null : deviceChannelService.getOneForSourceById(transaction.getChannelId());
        if (device == null || channel == null) {
            log.warn("[事务对账] 未找到残留事务对应的设备或通道, 直接清除事务, callId: {}", transaction.getCallId());
            sessionManager.removeByCallId(transaction.getCallId());
            return true;
        }
        try {
            log.info("[事务对账] 停止残留事务, device: {}, channel: {}, callId: {}, ssrc: {}",
                    device.getDeviceId(), channel.getDeviceId(), transaction.getCallId(), transaction.getSsrc());
            cmder.streamByeCmd(device, channel.getDeviceId(), transaction.getApp(), transaction.getStream(),
                    transaction.getCallId(), null);
            return true;
        } catch (com.genersoft.iot.vmp.conf.exception.SsrcTransactionNotFoundException e) {
            log.info("[事务对账] 事务已不存在, callId: {}", transaction.getCallId());
            return true;
        } catch (Exception e) {
            log.warn("[事务对账] 发送BYE失败, callId: {}, error: {}", transaction.getCallId(), e.getMessage());
            return false;
        }
    }

    private static boolean isReconcilable(SsrcTransaction transaction) {
        if (transaction == null || transaction.getCallId() == null) {
            return false;
        }
        // 仅处理设备点播/回放/下载，不处理对讲、广播和上级平台相关事务
        if (ObjectUtils.isEmpty(transaction.getDeviceId()) || !ObjectUtils.isEmpty(transaction.getPlatformId())) {
            return false;
        }
        return transaction.getType() == InviteSessionType.PLAY
                || transaction.getType() == InviteSessionType.PLAYBACK
                || transaction.getType() == InviteSessionType.DOWNLOAD;
    }
}
