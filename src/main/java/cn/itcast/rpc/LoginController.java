package cn.itcast.rpc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

public class LoginController {
	public static void main(String[] args) throws Exception {
		LoginServiceInterface proxy=RPC.getProxy(LoginServiceInterface.class,LoginServiceInterface.versionID,new InetSocketAddress("itcast01",10000),new Configuration());
	
		String logininfo = proxy.login("lsp", "123456");
		
		
		System.out.println(logininfo);
	}
}
