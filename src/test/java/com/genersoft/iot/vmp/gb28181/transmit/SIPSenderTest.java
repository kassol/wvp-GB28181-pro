package com.genersoft.iot.vmp.gb28181.transmit;

import com.genersoft.iot.vmp.conf.SipConfig;
import com.genersoft.iot.vmp.gb28181.SipLayer;
import com.genersoft.iot.vmp.gb28181.event.SipSubscribe;
import com.genersoft.iot.vmp.utils.GitUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sip.header.CSeqHeader;
import javax.sip.header.CallIdHeader;
import javax.sip.header.UserAgentHeader;
import javax.sip.header.ViaHeader;
import javax.sip.message.Request;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SIPSenderTest {

    @Mock
    private SipLayer sipLayer;

    @Mock
    private GitUtil gitUtil;

    @Mock
    private SipSubscribe sipSubscribe;

    @Mock
    private SipConfig sipConfig;

    @InjectMocks
    private SIPSender sipSender;

    @Test
    void transmitRequestReportsMissingProvider() throws Exception {
        String ip = "127.0.0.1";
        Request request = mock(Request.class);
        ViaHeader viaHeader = mock(ViaHeader.class);
        CallIdHeader callIdHeader = mock(CallIdHeader.class);
        CSeqHeader cSeqHeader = mock(CSeqHeader.class);
        UserAgentHeader userAgentHeader = mock(UserAgentHeader.class);

        when(request.getHeader(ViaHeader.NAME)).thenReturn(viaHeader);
        when(viaHeader.getTransport()).thenReturn("UDP");
        when(request.getHeader(UserAgentHeader.NAME)).thenReturn(userAgentHeader);
        when(request.getHeader(CallIdHeader.NAME)).thenReturn(callIdHeader);
        when(callIdHeader.getCallId()).thenReturn("call-1");
        when(request.getHeader(CSeqHeader.NAME)).thenReturn(cSeqHeader);
        when(cSeqHeader.getSeqNumber()).thenReturn(2L);
        when(sipLayer.getUdpSipProvider(ip)).thenReturn(null);

        boolean transmitted = sipSender.transmitRequestWithResult(ip, request, null, null);

        assertFalse(transmitted);
        verify(sipSubscribe).removeSubscribe("call-12");
    }
}
