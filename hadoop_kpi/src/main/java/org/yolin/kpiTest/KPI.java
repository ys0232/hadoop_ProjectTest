package org.yolin.kpiTest;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class KPI {
    private String remote_addr;
    private String remote_user;
    private String time_local;
    private String request;
    private String status;
    private String body_bytes_sent;
    private String http_referer;
    private String http_user_agent;
    private boolean valid=true;

    public String getRemote_addr() {
        return remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public String getRequest() {
        return request;
    }

    public String getStatus() {
        return status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }
public Date getTime_local_Date()throws ParseException{
        SimpleDateFormat df=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
        return df.parse(this.time_local);
}

public String getTime_local_Date_hour()throws ParseException{
        SimpleDateFormat df=new SimpleDateFormat("yyyyMMddHH");
        return df.format(this.getTime_local_Date());
}
public String getHttp_referer_domain(){
        if (http_referer.length()<8){
            return http_referer;
        }
        String str=this.http_referer.replace("\"","").replace("http://","").replace("https://","");
        return str.indexOf("/")>0?str.substring(0,str.indexOf("/")):str;
}

    @Override
    public String toString() {
        return "org.yolin.kpiTest.KPI{" +
                "remote_addr='" + remote_addr + '\'' +
                ", remote_user='" + remote_user + '\'' +
                ", time_local='" + time_local + '\'' +
                ", request='" + request + '\'' +
                ", status='" + status + '\'' +
                ", body_bytes_sent='" + body_bytes_sent + '\'' +
                ", http_referer='" + http_referer + '\'' +
                ", http_user_agent='" + http_user_agent + '\'' +
                ", valid=" + valid +
                '}';
    }


    private static KPI paser(String line){
    System.out.println(line);
    KPI kpi=new KPI();
    String[] arr=line.split(" ");
    if (arr.length>11){
        kpi.setRemote_addr(arr[0]);
        kpi.setRemote_user(arr[1]);
        kpi.setTime_local(arr[3].substring(1));
        kpi.setRequest(arr[6]);
        kpi.setStatus(arr[8]);
        kpi.setBody_bytes_sent(arr[9]);
        kpi.setHttp_referer(arr[10]);
        if (arr.length>12){
        kpi.setHttp_user_agent(arr[11]+" "+arr[12]);
        }else {
            kpi.setHttp_user_agent(arr[11]);
        }
        if (Integer.parseInt(kpi.getStatus())>=400){
            kpi.setValid(false);
        }
    }else {
        kpi.setValid(false);
    }
    return kpi;
    }
    public boolean isValid(){
        return valid;
    }
    public static KPI filterPVs(String line){
        //对不在page中的请求进行过滤
        KPI kpi=paser(line);
        Set page=new HashSet();
        page.add("/about");
        page.add("/black-ip-list");
        page.add("/finance-rhive-repurchase/");
        page.add("/hadoop-family-roadmap/");
        page.add("/hadoop-hive-intro/");
        page.add("/hadoop-zookeeper-intro/");
        page.add("/hadoop-mahout-roadmap/");
        if (!page.contains(kpi.getRequest())){
            kpi.setValid(false);
        }
        return kpi;
    }

    public static void main(String[] args){
        String line="222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"";
        System.out.println(line);
        KPI kpi=new KPI();
        String[] arr=line.split(" ");

        kpi.setRemote_addr(arr[0]);
        kpi.setRemote_user(arr[1]);
        kpi.setTime_local(arr[3].substring(1));
        kpi.setRequest(arr[6]);
        kpi.setStatus(arr[8]);
        kpi.setBody_bytes_sent(arr[9]);
        kpi.setHttp_referer(arr[10]);
        kpi.setHttp_user_agent(arr[11]+" "+arr[12]);
        System.out.println(kpi.toString());

        try{
            SimpleDateFormat df=new SimpleDateFormat("yyyy.MM.dd:HH:mm:ss", Locale.US);
            System.out.println(df.format(kpi.getTime_local_Date()));
            System.out.println(kpi.getTime_local_Date_hour());
            System.out.println(kpi.getHttp_referer_domain());
        }catch (ParseException e){
            e.printStackTrace();
        }
    }
}
