package core;

import codec.Beat;
import codec.RpcRequest;
import codec.RpcResponse;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import net.sf.cglib.reflect.FastClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ServiceUtil;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;


public class RpcServerHandler extends SimpleChannelInboundHandler<RpcRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RpcServerHandler.class);

    private final Map<String,Object> handlerMap;
    private final ThreadPoolExecutor serverHandlerPool;

    public RpcServerHandler(Map<String,Object> handlerMap,final ThreadPoolExecutor threadPoolExecutor) {
        this.handlerMap = handlerMap;
        this.serverHandlerPool = threadPoolExecutor;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx,final RpcRequest request) throws Exception {

        if (Beat.BEAT_ID.equalsIgnoreCase(request.getRequestId())) {
            logger.info("Server read heartbeat ping");
            return;
        }

        serverHandlerPool.execute(new Runnable() {
            @Override
            public void run() {
                logger.info("Recive request" + request.getRequestId());
                RpcResponse response = new RpcResponse();
                response.setRequestId(request.getRequestId());
                try {
                    Object result = handle(request);
                    response.setResult(result);
                } catch (Throwable e) {
                    response.setError(e.toString());
                    logger.error("Rpc server hanle request error", e);
                }
                ctx.writeAndFlush(response).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.info("Send response for request" + request.getRequestId());
                    }
                });

            }
        });
    }

    private Object handle(RpcRequest request) throws Throwable{
        String className = request.getClassName();
        String version = request.getVersion();
        String serviceKey = ServiceUtil.makeServiceKey(className,version);
        Object serviceBean = handlerMap.get(serviceKey);

        if (serviceBean == null) {
            logger.error("Can not find service implement with interface name:{} and version:{}",className,version);
            return null;
        }

        Class<?> serviceClass = serviceBean.getClass();
        String methodName = request.getMethodName();
        Class<?>[] parameterTypes = request.getParameterTypes();
        Object[] parameters = request.getParameters();

        logger.debug(serviceClass.getName());
        logger.debug(methodName);

        for(int i = 0; i < parameters.length; i++) {
            logger.debug(parameters[i].toString());
        }

        for(int i = 0; i < parameterTypes.length; i++) {
            logger.debug(parameterTypes[i].getName());
        }

        FastClass serviceFastClass  = FastClass.create(serviceClass);
        int methodIndex = serviceFastClass.getIndex(methodName,parameterTypes);
        return serviceFastClass.invoke(methodIndex,serviceBean,parameters);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.warn("Server caught exception: " + cause.getMessage());
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            ctx.channel().close();
            logger.warn("Channel idle in last {} seconds, close it", Beat.BEAT_TIMEOUT);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }


}
