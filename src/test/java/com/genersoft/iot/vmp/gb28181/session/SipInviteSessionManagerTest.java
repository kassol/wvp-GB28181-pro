package com.genersoft.iot.vmp.gb28181.session;

import com.genersoft.iot.vmp.common.VideoManagerConstants;
import com.genersoft.iot.vmp.conf.UserSetting;
import com.genersoft.iot.vmp.gb28181.bean.SsrcTransaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.Collections;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SipInviteSessionManagerTest {

    @Mock
    private UserSetting userSetting;

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    @InjectMocks
    private SipInviteSessionManager sessionManager;

    @Test
    void removeByCallIdKeepsStreamIndexWhenItPointsToNewerDialog() {
        String serverId = "server-1";
        String app = "rtp";
        String stream = "shared-stream";
        String oldCallId = "call-old";
        String callKey = VideoManagerConstants.SIP_INVITE_SESSION_CALL_ID + serverId;
        String streamKey = VideoManagerConstants.SIP_INVITE_SESSION_STREAM + serverId;
        String streamField = app + stream;

        SsrcTransaction oldTransaction = transaction(oldCallId, app, stream);
        when(userSetting.getServerId()).thenReturn(serverId);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(hashOperations.get(callKey, oldCallId)).thenReturn(oldTransaction);

        sessionManager.removeByCallId(oldCallId);

        verify(hashOperations).delete(callKey, oldCallId);
        verify(hashOperations, never()).delete(streamKey, streamField);
        verify(redisTemplate).execute(
                any(RedisScript.class),
                any(RedisSerializer.class),
                any(RedisSerializer.class),
                eq(Collections.singletonList(streamKey)),
                eq(streamField),
                eq("\"" + oldCallId + "\""));
    }

    @Test
    void putStoresRecoverableCallIdBeforeStreamIndex() {
        String serverId = "server-1";
        String callKey = VideoManagerConstants.SIP_INVITE_SESSION_CALL_ID + serverId;
        String streamKey = VideoManagerConstants.SIP_INVITE_SESSION_STREAM + serverId;
        SsrcTransaction transaction = transaction("call-1", "rtp", "stream-1");

        when(userSetting.getServerId()).thenReturn(serverId);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);

        sessionManager.put(transaction);

        InOrder writes = inOrder(hashOperations);
        writes.verify(hashOperations).put(callKey, "call-1", transaction);
        writes.verify(hashOperations).put(streamKey, "rtpstream-1", transaction);
    }

    @Test
    void removeByStreamIfSsrcMatchesSkipsWhenIndexHoldsNewerDialog() {
        String serverId = "server-1";
        String streamKey = VideoManagerConstants.SIP_INVITE_SESSION_STREAM + serverId;

        SsrcTransaction newTransaction = transaction("call-new", "rtp", "shared-stream");
        newTransaction.setSsrc("0200000002");
        when(userSetting.getServerId()).thenReturn(serverId);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(hashOperations.get(streamKey, "rtpshared-stream")).thenReturn(newTransaction);

        // 旧回调携带旧SSRC，流索引已被新事务覆盖，不应删除
        sessionManager.removeByStreamIfSsrcMatches("rtp", "shared-stream", "0200000001");

        verify(hashOperations, never()).delete(any(), any());
    }

    @Test
    void removeByStreamIfSsrcMatchesRemovesOwnDialogThroughCallIdCas() {
        String serverId = "server-1";
        String callKey = VideoManagerConstants.SIP_INVITE_SESSION_CALL_ID + serverId;
        String streamKey = VideoManagerConstants.SIP_INVITE_SESSION_STREAM + serverId;

        SsrcTransaction transaction = transaction("call-1", "rtp", "shared-stream");
        transaction.setSsrc("0200000001");
        when(userSetting.getServerId()).thenReturn(serverId);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(hashOperations.get(streamKey, "rtpshared-stream")).thenReturn(transaction);
        when(hashOperations.get(callKey, "call-1")).thenReturn(transaction);

        sessionManager.removeByStreamIfSsrcMatches("rtp", "shared-stream", "0200000001");

        // 流索引的删除交给Lua脚本，仅当其仍指向本次Call-ID时才生效
        verify(hashOperations, never()).delete(streamKey, "rtpshared-stream");
        verify(hashOperations).delete(callKey, "call-1");
        verify(redisTemplate).execute(
                any(RedisScript.class),
                any(RedisSerializer.class),
                any(RedisSerializer.class),
                eq(Collections.singletonList(streamKey)),
                eq("rtpshared-stream"),
                eq("\"call-1\""));
    }

    @Test
    void removeByStreamIfSsrcMatchesRefusesWhenSessionIdentityMissing() {
        // 会话身份缺失时不得退化成无条件删除
        sessionManager.removeByStreamIfSsrcMatches("rtp", "shared-stream", null);

        verify(hashOperations, never()).delete(any(), any());
        verify(redisTemplate, never()).execute(any(RedisScript.class), any(RedisSerializer.class),
                any(RedisSerializer.class), any(), any(), any());
    }

    private SsrcTransaction transaction(String callId, String app, String stream) {
        SsrcTransaction transaction = new SsrcTransaction();
        transaction.setCallId(callId);
        transaction.setApp(app);
        transaction.setStream(stream);
        return transaction;
    }
}
