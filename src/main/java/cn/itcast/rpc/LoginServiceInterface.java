package cn.itcast.rpc;

public interface LoginServiceInterface {
    //versionID字段是固定的
    public static final long versionID=1L;

    public String login(String username,String password);

}
