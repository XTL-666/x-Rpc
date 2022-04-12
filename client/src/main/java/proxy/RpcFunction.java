package proxy;

public interface RpcFunction<T,P> extends  SerializableFunction{
    Object apply(T t,P p);
}
