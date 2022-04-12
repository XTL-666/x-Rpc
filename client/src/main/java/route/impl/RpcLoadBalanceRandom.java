package route.impl;

import handler.RpcClientHandler;
import protocol.RpcProtocol;
import route.RpcLoadBalance;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class RpcLoadBalanceRandom extends RpcLoadBalance {

    private Random random;

    public RpcProtocol doRoute(String serviceKey,List<RpcProtocol> addressList) {
        int size = addressList.size();
        return addressList.get(random.nextInt(size));
    }


    @Override
    public RpcProtocol route(String serviceKey, Map<RpcProtocol, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<RpcProtocol>> serviceMap = getServiceMap(connectedServerNodes);
        List<RpcProtocol> addressList = serviceMap.get(serviceKey);

        if (addressList != null && addressList.size() > 0) {
            return doRoute(serviceKey,addressList);
        } else {
            throw new Exception("Can not find connection for service:" + serviceKey);
        }
    }
}
