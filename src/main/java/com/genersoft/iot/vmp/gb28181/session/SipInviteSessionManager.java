package com.genersoft.iot.vmp.gb28181.session;

import com.alibaba.fastjson2.JSON;
import com.genersoft.iot.vmp.common.VideoManagerConstants;
import com.genersoft.iot.vmp.conf.UserSetting;
import com.genersoft.iot.vmp.gb28181.bean.SsrcTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 视频流session管理器，管理视频预览、预览回放的通信句柄
 */
@Slf4j
@Component
public class SipInviteSessionManager {

	private static final DefaultRedisScript<Long> REMOVE_STREAM_BY_CALL_ID_SCRIPT = new DefaultRedisScript<>(
			"local value = redis.call('HGET', KEYS[1], ARGV[1]); " +
					"if not value then return 0 end; " +
					"local callId = '\"callId\":' .. ARGV[2]; " +
					"if string.find(value, callId, 1, true) then " +
					"return redis.call('HDEL', KEYS[1], ARGV[1]) end; " +
					"return 0",
			Long.class);

	private static final RedisSerializer<String> STRING_SERIALIZER = RedisSerializer.string();

	private static final RedisSerializer<Long> LONG_SERIALIZER = new GenericToStringSerializer<>(Long.class);

	@Autowired
	private UserSetting userSetting;

	@Autowired
	private RedisTemplate<String, Object> redisTemplate;

	/**
	 * 添加一个点播/回放的事务信息
	 */
	public synchronized void put(SsrcTransaction ssrcTransaction){
		redisTemplate.opsForHash().put(VideoManagerConstants.SIP_INVITE_SESSION_CALL_ID + userSetting.getServerId()
				, ssrcTransaction.getCallId(), ssrcTransaction);

		redisTemplate.opsForHash().put(VideoManagerConstants.SIP_INVITE_SESSION_STREAM + userSetting.getServerId()
				, ssrcTransaction.getApp() + ssrcTransaction.getStream(), ssrcTransaction);
	}

	public SsrcTransaction getSsrcTransactionByStream(String app, String stream){
		String key = VideoManagerConstants.SIP_INVITE_SESSION_STREAM + userSetting.getServerId();
		return (SsrcTransaction)redisTemplate.opsForHash().get(key, app + stream);
	}

	public SsrcTransaction getSsrcTransactionByCallId(String callId){
		String key = VideoManagerConstants.SIP_INVITE_SESSION_CALL_ID + userSetting.getServerId();
		return (SsrcTransaction)redisTemplate.opsForHash().get(key, callId);
	}

	public List<SsrcTransaction> getSsrcTransactionByDeviceId(String deviceId){
		String key = VideoManagerConstants.SIP_INVITE_SESSION_CALL_ID + userSetting.getServerId();
		List<Object> values = redisTemplate.opsForHash().values(key);
		List<SsrcTransaction> result = new ArrayList<>();
		for (Object value : values) {
			SsrcTransaction ssrcTransaction = (SsrcTransaction) value;
			if (ssrcTransaction != null && deviceId.equals(ssrcTransaction.getDeviceId())) {
				result.add(ssrcTransaction);
			}
		}
		return result;
	}
	
	public synchronized void removeByStream(String app, String stream) {
		SsrcTransaction ssrcTransaction = getSsrcTransactionByStream(app, stream);
		if (ssrcTransaction == null ) {
			return;
		}
		redisTemplate.opsForHash().delete(VideoManagerConstants.SIP_INVITE_SESSION_STREAM + userSetting.getServerId(), app + stream);
		if (ssrcTransaction.getCallId() != null) {
			redisTemplate.opsForHash().delete(VideoManagerConstants.SIP_INVITE_SESSION_CALL_ID + userSetting.getServerId(), ssrcTransaction.getCallId());
		}
	}

	/**
	 * 删除流索引对应的事务，仅当其SSRC与期望值一致时才删除。
	 * 用于异步失败/超时回调：新点播复用相同stream时，旧回调不应删除新事务。
	 * 实际删除委托给{@link #removeByCallId(String)}，由其Lua脚本保证流索引仍指向本次会话时才删除。
	 *
	 * @param expectedSsrc 本次会话创建时的SSRC，为null时拒绝删除
	 */
	public synchronized void removeByStreamIfSsrcMatches(String app, String stream, String expectedSsrc) {
		if (expectedSsrc == null) {
			log.info("[移除事务] 缺少会话标识, 跳过删除, app: {}, stream: {}", app, stream);
			return;
		}
		SsrcTransaction ssrcTransaction = getSsrcTransactionByStream(app, stream);
		if (ssrcTransaction == null) {
			return;
		}
		if (!expectedSsrc.equals(ssrcTransaction.getSsrc())) {
			log.info("[移除事务] SSRC不匹配, 跳过删除, app: {}, stream: {}, 期望: {}, 当前: {}",
					app, stream, expectedSsrc, ssrcTransaction.getSsrc());
			return;
		}
		removeByCallId(ssrcTransaction.getCallId());
	}

	public synchronized void removeByCallId(String callId) {
		SsrcTransaction ssrcTransaction = getSsrcTransactionByCallId(callId);
		if (ssrcTransaction == null ) {
			return;
		}
		if (ssrcTransaction.getStream() != null) {
			String streamKey = VideoManagerConstants.SIP_INVITE_SESSION_STREAM + userSetting.getServerId();
			redisTemplate.execute(REMOVE_STREAM_BY_CALL_ID_SCRIPT, STRING_SERIALIZER, LONG_SERIALIZER,
					Collections.singletonList(streamKey), ssrcTransaction.getApp() + ssrcTransaction.getStream(),
					JSON.toJSONString(callId));
		}
		redisTemplate.opsForHash().delete(VideoManagerConstants.SIP_INVITE_SESSION_CALL_ID + userSetting.getServerId(), callId);
	}

	public List<SsrcTransaction> getAll() {
		String key = VideoManagerConstants.SIP_INVITE_SESSION_CALL_ID + userSetting.getServerId();
		List<Object> values = redisTemplate.opsForHash().values(key);
		List<SsrcTransaction> result = new ArrayList<>();
		for (Object value : values) {
			result.add((SsrcTransaction) value);
		}
		return result;
	}
}
