package codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serializer.Serializer;
import serializer.kryo.KryoSerializer;

public class RpcEncoder extends MessageToByteEncoder {

    private static final Logger logger = LoggerFactory.getLogger(RpcEncoder.class);
    private Class<?> genericClass;
    private KryoSerializer serializer;

    public RpcEncoder(Class<?> genericClass, KryoSerializer serializer) {
        this.genericClass = genericClass;
        this.serializer = serializer;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object in, ByteBuf out) throws Exception {
        if (genericClass.isInstance(in)) {
            try {
                byte[] data = serializer.serialize(in);
                out.writeInt(data.length);
                out.writeBytes(data);
            } catch (Exception e) {
                logger.error("Encoder error" + e.toString());
            }
        }
    }

}
