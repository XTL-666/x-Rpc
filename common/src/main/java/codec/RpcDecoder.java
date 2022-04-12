package codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serializer.kryo.KryoSerializer;

import java.util.List;

public class RpcDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(RpcDecoder.class);
    private Class<T> genericClass;
    private KryoSerializer serializer;

    public RpcDecoder(Class<RpcRequest> genericClass, KryoSerializer serializer) {
        this.genericClass = genericClass;
        this.serializer = serializer;
    }


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 4) {
            return;
        }
        in.markReaderIndex();
        int dataLength = in.readInt();

        if (in.readableBytes() < dataLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] data = new byte[dataLength];

        in.readBytes(data);
        Object obj = null;

        try{
            obj = serializer.deserialize(data,genericClass);
            out.add(obj);
        }catch (Exception e) {
            logger.error("Decode error:" + e.toString());
        }

    }
}
