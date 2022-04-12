package registry;

import config.Constant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.RpcProtocol;
import protocol.RpcServiceInfo;
import util.ServiceUtil;
import zookeeper.CuratorClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ServiceRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);

    private CuratorClient curatorClient;
    private List<String> pathList = new ArrayList<>();

    public ServiceRegistry(String registryAddress) {
        this.curatorClient =  new CuratorClient(registryAddress,5000);
    }

    public void registryService(String host, int port, Map<String,Object> serviceMap) {
        List<RpcServiceInfo> serviceInfoList = new ArrayList<>();
        for (String key : serviceMap.keySet()) {
            String[] serviceInfo = key.split(ServiceUtil.SERVICE_CONCAT_TOKEN);
            if (serviceInfo.length > 0) {
                RpcServiceInfo rpcServiceInfo = new RpcServiceInfo();
                rpcServiceInfo.setServiceName(serviceInfo[0]);
                if (serviceInfo.length == 2) {
                    rpcServiceInfo.setVersion(serviceInfo[1]);
                }else{
                    rpcServiceInfo.setVersion("");
                }
                logger.info("Registry new service: {}",key);
                serviceInfoList.add(rpcServiceInfo);
            }else {
                logger.warn("Can not get service name and version:{}",key);
            }
        }
        try{
            RpcProtocol rpcProtocol = new RpcProtocol();
            rpcProtocol.setHost(host);
            rpcProtocol.setPort(port);
            rpcProtocol.setServiceInfoList(serviceInfoList);
            String serviceData = rpcProtocol.toJson();
            byte[] bytes = serviceData.getBytes();
            String path = Constant.ZK_DATA_PATH + "-" + rpcProtocol.hashCode();
            path = this.curatorClient.createPathData(path,bytes);
            pathList.add(path);
            logger.info("Registry {} new service,host:{},port: {}",serviceInfoList.size(),host,port);
        } catch (Exception e) {
            logger.info("Registry service fail,exception:{}",e.getMessage());
            e.printStackTrace();
        }

        curatorClient.addConnectionStateListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                if (connectionState == ConnectionState.RECONNECTED) {
                    logger.info("Connection state:{},registry server after reconnected,",connectionState);
                    registryService(host,port,serviceMap);
                }
            }
        });
    }

    public void unregistryService(){
        logger.info("Unregistry all service");
        for (String path : pathList) {
            try{
                this.curatorClient.deletePath(path);
            } catch (Exception e) {
                logger.info("Delete service path error" + e.getMessage());
            }
        }
        this.curatorClient.close();
    }

}
