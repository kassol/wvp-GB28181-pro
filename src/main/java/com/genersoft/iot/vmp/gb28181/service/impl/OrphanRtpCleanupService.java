package com.genersoft.iot.vmp.gb28181.service.impl;

import com.genersoft.iot.vmp.common.InviteSessionType;
import com.genersoft.iot.vmp.gb28181.bean.Device;
import com.genersoft.iot.vmp.gb28181.bean.DeviceChannel;
import com.genersoft.iot.vmp.gb28181.bean.SsrcTransaction;
import com.genersoft.iot.vmp.gb28181.service.IDeviceChannelService;
import com.genersoft.iot.vmp.gb28181.service.IDeviceService;
import com.genersoft.iot.vmp.gb28181.session.SipInviteSessionManager;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.ISIPCommander;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 清理孤儿RTP推流。
 *
 * 当 ZLMediaKit 收到RTP推流但 on_publish 鉴权失败时（点播已经不在WVP的点播状态中），
 * 说明设备仍在向流媒体推流，但对应的点播会话已经丢失（例如重复点播覆盖旧会话、
 * 无人观看自动关流未发BYE、服务重启后未恢复等场景遗留的推流）。
 *
 * 此时若 Redis 中仍保存着原始 INVITE 的事务信息（Call-ID、From/To tag），
 * 可以复用原事务信息向设备补发 BYE，通知设备停止推流，回收上行带宽。
 */
@Slf4j
@Service
public class OrphanRtpCleanupService {

    /**
     * 同一SSRC两次补发BYE之间的最小间隔（毫秒）。
     * ZLMediaKit 对被拒绝的推流会以数秒一次的频率反复触发 on_publish，
     * 这里做节流避免对同一会话重复发送BYE。
     */
    private static final long CLEANUP_INTERVAL_MS = 60 * 1000L;
    private static final int CLEANUP_QUEUE_CAPACITY = 100;

    @Autowired
    private SipInviteSessionManager sessionManager;

    @Autowired
    private IDeviceService deviceService;

    @Autowired
    private IDeviceChannelService deviceChannelService;

    @Autowired
    private ISIPCommander cmder;

    private final Map<String, Long> lastCleanupTime = new ConcurrentHashMap<>();

    private final Set<String> pending = ConcurrentHashMap.newKeySet();

    private final ThreadPoolExecutor executor;

    public OrphanRtpCleanupService() {
        this(CLEANUP_QUEUE_CAPACITY);
    }

    OrphanRtpCleanupService(int queueCapacity) {
        executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), runnable -> {
                    Thread thread = new Thread(runnable, "orphan-rtp-cleanup");
                    thread.setDaemon(true);
                    return thread;
                });
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
        pending.clear();
    }

    /**
     * RTP推流鉴权失败时触发，尝试为遗留的推流会话补发BYE。
     *
     * @param mediaServerId ZLM节点ID
     * @param stream ZLM上的流ID，对于未知SSRC的推流为8位十六进制SSRC
     */
    public void onRtpPublishRejected(String mediaServerId, String stream) {
        if (ObjectUtils.isEmpty(mediaServerId) || ObjectUtils.isEmpty(stream)) {
            return;
        }
        String pendingKey = mediaServerId + '\0' + stream;
        if (!pending.add(pendingKey)) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    cleanup(mediaServerId, stream);
                } finally {
                    pending.remove(pendingKey);
                }
            });
        } catch (RejectedExecutionException e) {
            pending.remove(pendingKey);
            log.warn("[孤儿推流清理] 清理队列已满, 等待下次鉴权失败事件, mediaServerId: {}, stream: {}",
                    mediaServerId, stream);
        }
    }

    private void cleanup(String mediaServerId, String stream) {
        List<SsrcTransaction> candidates = findTransactions(mediaServerId, stream);
        if (candidates.size() != 1) {
            log.info("[孤儿推流清理] 匹配到{}条事务, 跳过补发BYE, mediaServerId: {}, stream: {}",
                    candidates.size(), mediaServerId, stream);
            return;
        }
        SsrcTransaction transaction = candidates.get(0);
        if (!throttle(transaction.getCallId())) {
            return;
        }
        Device device = deviceService.getDeviceByDeviceId(transaction.getDeviceId());
        DeviceChannel channel = transaction.getChannelId() == null
                ? null : deviceChannelService.getOneForSourceById(transaction.getChannelId());
        if (device == null || channel == null) {
            log.warn("[孤儿推流清理] 未找到事务对应的设备或通道, stream: {}, callId: {}",
                    stream, transaction.getCallId());
            return;
        }
        try {
            log.info("[孤儿推流清理] 补发BYE, device: {}, channel: {}, ssrc: {}, callId: {}",
                    device.getDeviceId(), channel.getDeviceId(), transaction.getSsrc(), transaction.getCallId());
            cmder.streamByeCmd(device, channel.getDeviceId(), transaction.getApp(), transaction.getStream(),
                    transaction.getCallId(), null);
        } catch (Exception e) {
            log.warn("[孤儿推流清理] 发送BYE失败, callId: {}, error: {}", transaction.getCallId(), e.getMessage());
        }
    }

    /**
     * 8位十六进制流ID只按SSRC匹配，其他流ID只按事务中的流ID匹配。
     */
    private List<SsrcTransaction> findTransactions(String mediaServerId, String stream) {
        Long ssrcValue = parseHexSsrc(stream);
        List<SsrcTransaction> candidates = new ArrayList<>();
        for (SsrcTransaction transaction : sessionManager.getAll()) {
            if (!isDeviceInvite(transaction) || !mediaServerId.equals(transaction.getMediaServerId())) {
                continue;
            }
            boolean matches = ssrcValue == null
                    ? stream.equals(transaction.getStream())
                    : sameSsrc(ssrcValue, transaction.getSsrc());
            if (matches) {
                candidates.add(transaction);
            }
        }
        return candidates;
    }

    private static boolean isDeviceInvite(SsrcTransaction transaction) {
        if (transaction == null || ObjectUtils.isEmpty(transaction.getDeviceId())
                || !ObjectUtils.isEmpty(transaction.getPlatformId()) || ObjectUtils.isEmpty(transaction.getCallId())) {
            return false;
        }
        return transaction.getType() == InviteSessionType.PLAY
                || transaction.getType() == InviteSessionType.PLAYBACK
                || transaction.getType() == InviteSessionType.DOWNLOAD;
    }

    private boolean throttle(String callId) {
        if (callId == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        Long last = lastCleanupTime.get(callId);
        if (last != null && now - last < CLEANUP_INTERVAL_MS) {
            return false;
        }
        lastCleanupTime.put(callId, now);
        if (lastCleanupTime.size() > 1000) {
            lastCleanupTime.entrySet().removeIf(entry -> now - entry.getValue() > CLEANUP_INTERVAL_MS);
        }
        return true;
    }

    private static Long parseHexSsrc(String stream) {
        if (stream == null || !stream.matches("(?i)[0-9a-f]{8}")) {
            return null;
        }
        try {
            return Long.parseUnsignedLong(stream, 16);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static boolean sameSsrc(long ssrcValue, String decimalSsrc) {
        if (ObjectUtils.isEmpty(decimalSsrc)) {
            return false;
        }
        try {
            return ssrcValue == Long.parseLong(decimalSsrc);
        } catch (NumberFormatException ignored) {
            return false;
        }
    }
}
