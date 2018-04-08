
import org.quartz.JobExecutionContext;

import java.io.File;
import java.util.logging.Logger;

/**
 * 向yarn 上提交Spark 任务
 */
public class TaskSubmit4LivyMgrImpl implements BatchMgr {

//   private static Logger log = Logger.getLogger(TaskSubmit4LivyMgrImpl.class);
    @Override
    //通过Livy 提交spark 任务到yarn 集群
    public void invoke(JobExecutionContext context) {
//        log.info("~~ invoke task");
        String url = "http://192.168.109.130:8998/batches";
        String postData = "{\"kind\": \"spark\"}";
        String pData = "{\"file\":\"D://Pi.jar\",\"className\":\"neu.edu.cn.PiJob\" }";
        String reqResult = ReqEngine.sendPostReq(url, pData);
        System.out.println(reqResult);
    }
    public static void main(String[] args){

/*        String url = "http://192.168.109.130:8998/batches";

        String postData = "{\"kind\": \"spark\"}";
        String pData = "{\"file\":\"hdfs://192.168.109.130:9000/jar/spark-examples-1.6.3-hadoop2.6.0.jar\",\"className\":\"org.apache.spark.examples.SparkPi\"}";
        String reqResult = ReqEngine.sendPostReq(url, pData);
//        String reqResult = ReqEngine.sendPostReq(url,postData);
        System.out.println(reqResult);*/
    	
        String url = "http://192.168.109.130:8998/batches";
        String postData = "{\"kind\": \"spark\"}";
        String pData = "{\"file\":\"/user/PI.jar\",\"className\":\"PI.PiJob\" }";
        String reqResult = ReqEngine.sendPostReq(url, pData);
        System.out.println(reqResult);
    }
}
