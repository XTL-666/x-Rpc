package proxy;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

public interface SerializableFunction<T> extends Serializable{
    default String getName() throws Exception {
        Method writer = this.getClass().getDeclaredMethod("writeReplace");
        writer.setAccessible(true);
        SerializedLambda serializableLamda = (SerializedLambda) writer.invoke(this);
        return serializableLamda.getImplMethodName();
    }
}
