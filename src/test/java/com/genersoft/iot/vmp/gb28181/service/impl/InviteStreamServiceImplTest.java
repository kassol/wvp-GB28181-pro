package com.genersoft.iot.vmp.gb28181.service.impl;

import com.genersoft.iot.vmp.common.InviteInfo;
import com.genersoft.iot.vmp.common.InviteSessionType;
import com.genersoft.iot.vmp.common.VideoManagerConstants;
import com.genersoft.iot.vmp.service.bean.SSRCInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InviteStreamServiceImplTest {

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    @InjectMocks
    private InviteStreamServiceImpl service;


    @Test
    void removeSkipsWhenSsrcBelongsToNewerInvite() {
        // Lua的CAS判定：字段中不含期望SSRC时不删除
        when(redisTemplate.execute(any(RedisScript.class), any(RedisSerializer.class), any(RedisSerializer.class),
                eq(Collections.singletonList(VideoManagerConstants.INVITE_PREFIX)), eq("PLAY:1:stream-1"),
                eq("\"0200000001\""))).thenReturn(0L);

        boolean removed = service.removeInviteInfoIfSsrcMatches(InviteSessionType.PLAY, 1, "stream-1", "0200000001");

        assertFalse(removed);
        verify(hashOperations, never()).delete(anyString(), any());
    }

    @Test
    void removeDeletesWhenSsrcMatches() {
        when(redisTemplate.execute(any(RedisScript.class), any(RedisSerializer.class), any(RedisSerializer.class),
                eq(Collections.singletonList(VideoManagerConstants.INVITE_PREFIX)), eq("PLAY:1:stream-1"),
                eq("\"0200000001\""))).thenReturn(1L);

        boolean removed = service.removeInviteInfoIfSsrcMatches(InviteSessionType.PLAY, 1, "stream-1", "0200000001");

        assertTrue(removed);
    }

    @Test
    void removeWithoutExpectedSsrcRefusesToDelete() {
        // 会话身份缺失时不得退化成无条件删除，否则会删掉新点播的状态
        boolean removed = service.removeInviteInfoIfSsrcMatches(InviteSessionType.PLAY, 1, "stream-1", null);

        assertFalse(removed);
        verify(hashOperations, never()).delete(anyString(), any());
        verify(redisTemplate, never()).execute(any(RedisScript.class), any(RedisSerializer.class),
                any(RedisSerializer.class), any(), any(), any());
    }

    @Test
    void removeWithoutStreamRefusesToDelete() {
        // 回放/下载同通道多条记录，缺少stream无法定位目标，不得通配删除
        boolean removed = service.removeInviteInfoIfSsrcMatches(InviteSessionType.PLAYBACK, 1, null, "0200000001");

        assertFalse(removed);
        verify(redisTemplate, never()).execute(any(RedisScript.class), any(RedisSerializer.class),
                any(RedisSerializer.class), any(), any(), any());
    }
}
