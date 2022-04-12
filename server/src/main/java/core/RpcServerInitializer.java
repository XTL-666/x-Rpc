package core;

import codec.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import serializer.kryo.KryoSerializer;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RpcServerInitializer extends ChannelInitializer {
    private Map<String,Object> handlerMap;
    private ThreadPoolExecutor threadPoolExecutor;

    public RpcServerInitializer(Map<String,Object> handlerMap,ThreadPoolExecutor threadPoolExecutor) {
        this.handlerMap = handlerMap;
        this.threadPoolExecutor = threadPoolExecutor;
    }

    @Override
    protected void initChannel(Channel chanel) throws Exception {
        KryoSerializer serializer = KryoSerializer.class.newInstance();
        ChannelPipeline cp = chanel.pipeline();
        cp.addLast(new IdleStateHandler(0,0, Beat.BEAT_TIMEOUT, TimeUnit.SECONDS));
        cp.addLast(new LengthFieldBasedFrameDecoder(65536,0,4,0,0));
        cp.addLast(new RpcDecoder(RpcRequest.class,serializer));
        cp.addLast(new RpcEncoder(RpcResponse.class,serializer));
        cp.addLast(new RpcServerHandler(handlerMap,threadPoolExecutor));
    }
}
