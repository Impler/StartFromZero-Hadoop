package com.allin.hadoop.inaction.kpi.dmo;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 请求日志对象
 */
public class RequestLog {

    private static Pattern pattern =
            Pattern.compile(
                    "(\\d+.\\d+.\\d+.\\d+) - - \\[(.*?)\\] \"(.*?) (.*?) (.*?)\" (\\d+) (\\d+) " +
                            "\"(.*?)\" \"(.*?)\"");
    // 请求ip
    private String ip;
    // 请求url
    private String url;
    // 请求时间
    private String time;
    // 请求方式
    private String method;
    // 访问协议
    private String protocal;
    // 响应码
    private int status;
    // 页面大小
    private int pageSize;
    // 来源
    private String refer;
    // 浏览器类型
    private String browser;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getProtocal() {
        return protocal;
    }

    public void setProtocal(String protocal) {
        this.protocal = protocal;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public String getRefer() {
        return refer;
    }

    public void setRefer(String refer) {
        this.refer = refer;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public static RequestLog parseLine(String line) {
        if (StringUtils.isBlank(line)) {
            return null;
        }
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
            // 模式匹配每个字段
            String ip = matcher.group(1);
            String time = matcher.group(2);
            String method = matcher.group(3);
            String url = matcher.group(4);
            String protocal = matcher.group(5);
            String status = matcher.group(6);
            String pageSize = matcher.group(7);
            String refer = matcher.group(8);
            String browser = matcher.group(9);

            // 封装对象
            RequestLog request = new RequestLog();
            request.setIp(ip);
            request.setTime(time);
            request.setMethod(method);
            request.setUrl(url);
            request.setProtocal(protocal);
            request.setStatus(Integer.parseInt(status));
            request.setPageSize(Integer.parseInt(pageSize));
            request.setRefer(refer);
            request.setBrowser(browser);
            return request;
        } else {
            return null;
        }
    }

}
