package route;

import handler.RpcClientHandler;
import protocol.RpcProtocol;
import protocol.RpcServiceInfo;
import util.ServiceUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class RpcLoadBalance {

    protected Map<String, List<RpcProtocol>> getServiceMap(Map<RpcProtocol, RpcClientHandler> connectedServerNodes) {
        Map<String, List<RpcProtocol>> serviceMap = new HashMap<>();
        if (connectedServerNodes != null && connectedServerNodes.size() > 0) {
            for (RpcProtocol rpcProtocol : connectedServerNodes.keySet()) {
                for(RpcServiceInfo rpcServiceInfo : rpcProtocol.getServiceInfoList()){
                    String serviceKey = ServiceUtil.makeServiceKey(rpcServiceInfo.getServiceName(),rpcServiceInfo.getVersion());
                    List<RpcProtocol> rpcProtocolList = serviceMap.get(serviceKey);

                    if (rpcProtocolList == null) {
                        rpcProtocolList = new ArrayList<>();
                    }
                    rpcProtocolList.add(rpcProtocol);
                    serviceMap.putIfAbsent(serviceKey,rpcProtocolList);
                }
            }
        }
        return serviceMap;
    }

    public  abstract RpcProtocol route(String serviceKey,Map<RpcProtocol,RpcClientHandler> connectedServerNodes) throws Exception;

}
