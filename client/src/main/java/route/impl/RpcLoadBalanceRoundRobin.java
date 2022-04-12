package route.impl;

import handler.RpcClientHandler;
import protocol.RpcProtocol;
import route.RpcLoadBalance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcLoadBalanceRoundRobin extends RpcLoadBalance {
    private AtomicInteger roundRobin = new AtomicInteger(0);

    public RpcProtocol doRoute(String serviceKey,List<RpcProtocol> addressList) {
        int size = addressList.size();

        int index = (roundRobin.getAndAdd(1) + size)%size;
        return addressList.get(index);
    }



    @Override
    public RpcProtocol route(String serviceKey, Map<RpcProtocol, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<RpcProtocol>> serviceMap = getServiceMap(connectedServerNodes);
        List<RpcProtocol> addressList = serviceMap.get(serviceKey);
        if (addressList != null || addressList.size() > 0) {
            return doRoute(serviceKey,addressList);
        }else{
            throw new Exception("Can not find connection for service: " + serviceKey);
        }
    }
}
