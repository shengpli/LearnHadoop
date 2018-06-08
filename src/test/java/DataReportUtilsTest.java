import cn.touna.report.DataReportUtils;
import com.alibaba.fastjson.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * DataReportUtils Tester.
 *
 * @author lishengping
 * @version 1.0
 * @since <pre>05/28/2018</pre>
 */
public class DataReportUtilsTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: syncSendMsg(String msg)
     */
    @Test
    public void testSyncSendMsg() throws Exception {
        //MDC.put("trace_id", "123");
        JSONObject testJson = new JSONObject(true);
        testJson.put("userid", "10001");
        testJson.put("mobile", "13827404611");
        testJson.put("time", "2018522151020");
        testJson.put("app_version", "v5.0");
        testJson.put("device_id", "478FA2BE-DA92-447A-95CB-F887A18ADBFE");
        testJson.put("os", "iOS");
        testJson.put("os_version", "iPhone7,1");
        testJson.put("result", "FALSE");
        testJson.put("resultid", "0001");
        testJson.put("description", "123");
        testJson.put("taskid", "123");

        JSONObject changeAbleJson = new JSONObject(true);
        changeAbleJson.put("taskid", "可变参数");
        changeAbleJson.put("taskid2", "可变参数");

/*        while (true) {
            Thread.sleep(100);
            testJson.put("description", "同步");
            boolean result = DataReportUtils.syncSendMsg("evnt_tnloan_app_register", testJson.toJSONString(),changeAbleJson.toJSONString());
            System.out.println(result);
        }*/

        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            testJson.put("description", "同步");
            boolean result = DataReportUtils.syncSendMsg("evnt_tnloan_app_register", testJson.toJSONString(),changeAbleJson.toJSONString());
            System.out.println(result);
        }
        Thread.sleep(8000);
    }

    @Test
    public void testParams() throws Exception {
        JSONObject notChangeAbleJson = new JSONObject(true);

        JSONObject changeAbleJson = new JSONObject(true);

        DataReportUtils.syncSendMsg("evnt_tnloan_app_register",notChangeAbleJson.toJSONString(),null);

        //DataReportUtils.syncSendMsg("evnt_tnloan_app_register",null,null);

        //DataReportUtils.syncSendMsg(null,null,null);

        Thread.sleep(3000);

    }



    /**
     * Method: asyncSendMsg(String msg)
     */
    @Test
    public void testAsyncSendMsg() throws Exception {
        //MDC.put("trace_id", "123");
        JSONObject testJson = new JSONObject(true);
        testJson.put("userid", "10001");
        testJson.put("time", "2018522151020");
        testJson.put("app_version", "v5.0");
        testJson.put("device_id", "478FA2BE-DA92-447A-95CB-F887A18ADBFE");
        testJson.put("os", "iOS");
        testJson.put("os_version", "iPhone7,1");
        testJson.put("gps", "116.236681,39.612279");
        testJson.put("result", "FALSE");
        testJson.put("resultid", "0001");

        JSONObject changeAbleJson = new JSONObject(true);
        changeAbleJson.put("taskid", "可变参数");
        changeAbleJson.put("taskid2", "可变参数");

        for (int i = 0; i < 100; i++) {
            testJson.put("description", "异步");
            boolean result = DataReportUtils.asyncSendMsg("evnt_tnloan_app_register", testJson.toJSONString(),changeAbleJson.toJSONString());
            System.out.println(result);
        }
        Thread.sleep(5000);
    }


    /**
     * Method: structureFinallyMessage(String msg)
     */
    @Test
    public void testStructureFinallyMessage() throws Exception {
/*
try { 
   Method method = DataReportUtils.getClass().getMethod("structureFinallyMessage", String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

} 
